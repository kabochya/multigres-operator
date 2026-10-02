package shard

import (
	"fmt"
	"slices"
	"strconv"

	v1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	nameutil "github.com/multigres/multigres-operator/pkg/util/name"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	unmanagedComponent    = "unmanaged-pooler"
	unmanagedFinalizer    = "multigres.com/source-process-shutdown"
	migrationKeyPath      = "/etc/multigres/migration/key"
	sourceConnectionLabel = "multigres.com/source-connection"
)

func migrationKeyVolume(ref *corev1.SecretKeySelector) corev1.Volume {
	return corev1.Volume{
		Name: "migration-key",
		VolumeSource: corev1.VolumeSource{
			Secret: &corev1.SecretVolumeSource{
				SecretName:  ref.Name,
				DefaultMode: ptr.To(int32(0o444)),
				Items:       []corev1.KeyToPath{{Key: ref.Key, Path: "key"}},
			},
		},
	}
}

func migrationKeyMount() corev1.VolumeMount {
	return corev1.VolumeMount{
		Name:      "migration-key",
		MountPath: "/etc/multigres/migration",
		ReadOnly:  true,
	}
}

func migrationClientTLSArgs(s *v1.Shard) []string {
	return []string{
		"--multipooler-grpc-cert",
		ShardTLSCertFile,
		"--multipooler-grpc-key",
		ShardTLSKeyFile,
		"--multipooler-grpc-ca",
		ShardTLSCAFile,
		"--multipooler-grpc-server-name",
		v1.ComponentCertCommonName(
			v1.ComponentMultiPoolerTLS,
			s.Labels[metadata.LabelMultigresCluster],
			s.Namespace,
		),
		"--multipooler-grpc-require-tls",
	}
}

// sourceCapacity divides the application budget across all source processes.
// Admin capacity is fixed at two per process and is deliberately explicit.
func sourceCapacity(spec v1.UnmanagedPoolerSpec) (int32, error) {
	if len(spec.Cells) > 10 {
		return 0, fmt.Errorf("source supports at most ten cells")
	}
	replicas := spec.ReplicasPerCell
	if replicas == 0 {
		replicas = 1
	}
	if len(spec.Cells) == 0 || replicas < 1 || replicas > 8 {
		return 0, fmt.Errorf("source requires cells and 1..8 replicas per cell")
	}
	var count int32
	for range spec.Cells {
		count += replicas
	}
	capacity := spec.ConnectionBudget / count
	if capacity < 4 {
		return 0, fmt.Errorf(
			"source budget must allow at least four application connections per process",
		)
	}
	if len(spec.Cells) != len(slices.Compact(slices.Sorted(slices.Values(spec.Cells)))) {
		return 0, fmt.Errorf("source cells must be unique")
	}
	return capacity, nil
}

func sourceLabels(s *v1.Shard) map[string]string {
	labels := metadata.BuildStandardLabels(
		s.Labels[metadata.LabelMultigresCluster],
		unmanagedComponent,
	)
	metadata.AddDatabaseLabel(labels, s.Spec.DatabaseName)
	metadata.AddTableGroupLabel(labels, s.Spec.TableGroupName)
	metadata.AddShardLabel(labels, s.Spec.ShardName)
	return labels
}

func isUnmanagedPod(p corev1.Pod) bool {
	return p.Labels[metadata.LabelAppComponent] == unmanagedComponent
}

// BuildUnmanagedPoolerPod deliberately does not use managed pool builders: no
// pgctld, Postgres PVC, backup, consensus readiness gate, or superuser Secret.
func BuildUnmanagedPoolerPod(
	s *v1.Shard,
	name, cell string,
	spec v1.UnmanagedPoolerSpec,
	index int,
	scheme *runtime.Scheme,
) (*corev1.Pod, error) {
	if !shardTLSConfigured(s) {
		return nil, fmt.Errorf("migration poolers require internal mTLS")
	}
	if s.Spec.MigrationKeySecretRef == nil || s.Spec.MigrationKeySecretRef.Name == "" ||
		s.Spec.MigrationKeySecretRef.Key == "" {
		return nil, fmt.Errorf("migration key Secret reference required")
	}
	if spec.ConnectionName == "" || !slices.Contains(spec.Cells, v1.CellName(cell)) {
		return nil, fmt.Errorf("connection and configured cell required")
	}
	capacity, err := sourceCapacity(spec)
	if err != nil {
		return nil, err
	}
	podName := nameutil.JoinWithConstraints(
		nameutil.PodConstraints,
		s.Name,
		"source",
		name,
		cell,
		strconv.Itoa(index),
	)
	labels := sourceLabels(s)
	metadata.AddCellLabel(labels, v1.CellName(cell))
	labels[sourceConnectionLabel] = spec.ConnectionName
	image := string(s.Spec.Images.Multipooler)
	if image == "" {
		image = v1.DefaultMultipoolerImage
	}
	args := []string{
		"multipooler",
		"--management-mode=unmanaged",
		"--source-connection=" + spec.ConnectionName,
		"--migration-key-file=" + migrationKeyPath,
		"--database=" + string(s.Spec.DatabaseName),
		"--table-group=" + string(s.Spec.TableGroupName),
		"--shard=" + string(s.Spec.ShardName),
		"--cell=" + cell,
		"--service-id=k8s-$(POD_UID)",
		"--hostname=$(POD_IP)",
		"--grpc-port=15270",
		"--http-port=15200",
		"--topo-global-server-addresses=" + s.Spec.GlobalTopoServer.Address,
		"--topo-global-root=" + s.Spec.GlobalTopoServer.RootPath,
		"--connpool-global-capacity=" + strconv.Itoa(int(capacity)),
		"--connpool-admin-capacity=2",
		"--onterm-timeout=80s",
		"--grpc-cert",
		ShardTLSCertFile,
		"--grpc-key",
		ShardTLSKeyFile,
		"--grpc-ca",
		ShardTLSCAFile,
		"--grpc-server-ca",
		ShardTLSCAFile,
	}
	args = append(args, migrationClientTLSArgs(s)...)
	volumes := []corev1.Volume{
		migrationKeyVolume(s.Spec.MigrationKeySecretRef),
		buildShardTLSVolume(s, v1.ComponentMultiPoolerTLS),
	}
	mounts := []corev1.VolumeMount{migrationKeyMount(), shardTLSVolumeMount()}
	if v1.TopoClientTLSConfigured(s.Spec.GlobalTopoServer) {
		args = append(args, v1.TopoClientTLSArgs()...)
		volumes = append(volumes, v1.BuildTopoClientTLSVolume(s.Spec.GlobalTopoServer))
		mounts = append(mounts, v1.TopoClientTLSVolumeMount())
	}
	container := corev1.Container{
		Name:      "multipooler",
		Image:     image,
		Args:      args,
		Resources: spec.Multipooler.Resources,
		SecurityContext: buildMultipoolerSecurityContext(
			v1.PoolSpec{Multipooler: spec.Multipooler},
		),
		VolumeMounts: mounts,
		Ports:        buildMultipoolerContainerPorts(),
		Env: []corev1.EnvVar{
			{
				Name: "POD_UID",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "metadata.uid"},
				},
			},
			{
				Name: "POD_IP",
				ValueFrom: &corev1.EnvVarSource{
					FieldRef: &corev1.ObjectFieldSelector{FieldPath: "status.podIP"},
				},
			},
		},
	}
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:       podName,
			Namespace:  s.Namespace,
			Labels:     labels,
			Finalizers: []string{unmanagedFinalizer},
		},
		Spec: corev1.PodSpec{
			RestartPolicy:                 corev1.RestartPolicyNever,
			TerminationGracePeriodSeconds: ptr.To(int64(90)),
			Containers:                    []corev1.Container{container},
			Volumes:                       volumes,
			NodeSelector:                  s.Spec.CellTopologyLabels[v1.CellName(cell)],
			ImagePullSecrets:              s.Spec.Images.ImagePullSecrets,
		},
	}
	pod.Annotations = map[string]string{metadata.AnnotationSpecHash: ComputeSpecHash(pod)}
	if err = ctrl.SetControllerReference(s, pod, scheme); err != nil {
		return nil, err
	}
	return pod, nil
}
