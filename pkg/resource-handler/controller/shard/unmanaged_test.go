package shard

import (
	"context"
	"errors"
	"testing"

	v1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func migrationShard() *v1.Shard {
	s := newTestShard()
	s.Spec.InternalTLS = &v1.InternalTLSConfig{Enabled: ptr.To(true)}
	s.Spec.MigrationKeySecretRef = &corev1.SecretKeySelector{
		LocalObjectReference: corev1.LocalObjectReference{Name: "migration-key"},
		Key:                  "key",
	}
	s.Spec.UnmanagedPoolers = map[string]v1.UnmanagedPoolerSpec{
		"source": {
			ConnectionName:   "external",
			Cells:            []v1.CellName{"cell1", "cell2"},
			ConnectionBudget: 24,
		},
	}
	return s
}

func TestSourceBudgetAndIsolation(t *testing.T) {
	s := migrationShard()
	spec := s.Spec.UnmanagedPoolers["source"]
	scheme := testScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	p, err := BuildUnmanagedPoolerPod(s, "source", "cell1", spec, 0, scheme)
	require.NoError(t, err)
	require.Empty(t, p.Spec.InitContainers)
	require.Len(t, p.Spec.Containers, 1)
	require.Empty(t, p.Spec.ReadinessGates)
	require.Equal(t, corev1.RestartPolicyNever, p.Spec.RestartPolicy)
	require.Contains(t, p.Spec.Containers[0].Args, "--service-id=k8s-$(POD_UID)")
	require.Contains(t, p.Spec.Containers[0].Args, "--connpool-global-capacity=12")
	require.Contains(t, p.Spec.Containers[0].Args, "--connpool-admin-capacity=2")
	require.Equal(t, unmanagedComponent, p.Labels[metadata.LabelAppComponent])
	for _, v := range p.Spec.Volumes {
		require.Nil(t, v.PersistentVolumeClaim)
		if v.Secret != nil {
			require.NotEqual(t, s.Spec.PostgresPasswordSecretRef.Name, v.Secret.SecretName)
		}
	}
	managed, err := BuildPoolPod(s, "managed", "cell1", newTestPoolSpec(), 0, scheme)
	require.NoError(t, err)
	require.Contains(t, managed.Spec.Containers[0].Args, "--migration-key-file="+migrationKeyPath)
	for _, c := range managed.Spec.InitContainers {
		for _, m := range c.VolumeMounts {
			require.NotEqual(t, "migration-key", m.Name)
		}
	}
	spec.ReplicasPerCell = 8
	_, err = sourceCapacity(spec)
	require.Error(t, err, "scaling must not multiply the source budget")
	s.Spec.InternalTLS = nil
	_, err = BuildUnmanagedPoolerPod(
		s,
		"source",
		"cell1",
		s.Spec.UnmanagedPoolers["source"],
		0,
		scheme,
	)
	require.Error(t, err)
}

func TestSourceRemovalRequiresCompletionAndProcessTermination(t *testing.T) {
	ctx := t.Context()
	s := migrationShard()
	scheme := testScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	pod, err := BuildUnmanagedPoolerPod(
		s,
		"source",
		"cell1",
		s.Spec.UnmanagedPoolers["source"],
		0,
		scheme,
	)
	require.NoError(t, err)
	pod.UID = types.UID("unique-process")
	c := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&corev1.Pod{}).
		WithObjects(s, pod).
		Build()
	state := &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_MANAGED}
	var readErr error
	r := &ShardReconciler{
		Client:               c,
		Scheme:               scheme,
		CreateTopoStore:      newMemoryTopoFactory(),
		ReadMigrationRouting: func(context.Context, *v1.Shard) (*pb.MigrationRouting, error) { return state, readErr },
	}
	pending, err := r.reconcileUnmanagedPoolers(ctx, s, true)
	require.NoError(t, err)
	require.True(t, pending)
	observed := &corev1.Pod{}
	key := client.ObjectKeyFromObject(pod)
	require.NoError(t, c.Get(ctx, key, observed))
	require.True(t, observed.DeletionTimestamp.IsZero())
	readErr = errors.New("authority unavailable")
	_, err = r.reconcileUnmanagedPoolers(ctx, s, true)
	require.Error(t, err)
	require.NoError(t, c.Get(ctx, key, observed))
	require.True(t, observed.DeletionTimestamp.IsZero())
	readErr = nil
	state.MigrationCompleted = true
	_, err = r.reconcileUnmanagedPoolers(ctx, s, true)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, observed))
	require.False(t, observed.DeletionTimestamp.IsZero())
	require.Contains(t, observed.Finalizers, unmanagedFinalizer)
	// A terminating but still-running process keeps its fencing identity.
	_, err = r.reconcileUnmanagedPoolers(ctx, s, true)
	require.NoError(t, err)
	require.NoError(t, c.Get(ctx, key, observed))
	require.Contains(t, observed.Finalizers, unmanagedFinalizer)
	observed.Status.ContainerStatuses = []corev1.ContainerStatus{
		{
			Name:  "multipooler",
			State: corev1.ContainerState{Terminated: &corev1.ContainerStateTerminated{ExitCode: 0}},
		},
	}
	require.NoError(t, c.Status().Update(ctx, observed))
	_, err = r.reconcileUnmanagedPoolers(ctx, s, true)
	require.NoError(t, err)
	require.Error(t, c.Get(ctx, key, observed))
}

func TestSourceDriftWaitsForFence(t *testing.T) {
	s := migrationShard()
	spec := s.Spec.UnmanagedPoolers["source"]
	spec.Cells = []v1.CellName{"cell1"}
	s.Spec.UnmanagedPoolers["source"] = spec
	scheme := testScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	p, err := BuildUnmanagedPoolerPod(
		s,
		"source",
		"cell1",
		s.Spec.UnmanagedPoolers["source"],
		0,
		scheme,
	)
	require.NoError(t, err)
	p.Annotations[metadata.AnnotationSpecHash] = "old-spec"
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s, p).Build()
	state := &pb.MigrationRouting{Mode: pb.MigrationMode_MIGRATION_MODE_UNMANAGED}
	r := &ShardReconciler{
		Client:               c,
		Scheme:               scheme,
		ReadMigrationRouting: func(context.Context, *v1.Shard) (*pb.MigrationRouting, error) { return state, nil },
	}
	_, err = r.reconcileUnmanagedPoolers(t.Context(), s, false)
	require.NoError(t, err)
	observed := &corev1.Pod{}
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(p), observed))
	require.True(t, observed.DeletionTimestamp.IsZero())
	state.Mode = pb.MigrationMode_MIGRATION_MODE_FENCED
	_, err = r.reconcileUnmanagedPoolers(t.Context(), s, false)
	require.NoError(t, err)
	require.NoError(t, c.Get(t.Context(), client.ObjectKeyFromObject(p), observed))
	require.False(t, observed.DeletionTimestamp.IsZero())
}

func TestMissingSourceProcessBlocksReplacement(t *testing.T) {
	s := migrationShard()
	spec := s.Spec.UnmanagedPoolers["source"]
	spec.Cells = []v1.CellName{"cell1"}
	s.Spec.UnmanagedPoolers["source"] = spec
	scheme := testScheme()
	require.NoError(t, corev1.AddToScheme(scheme))
	_, factory := memorytopo.NewServerAndFactory(t.Context(), "cell1")
	newStore := func(*v1.Shard) (topoclient.Store, error) {
		return topoclient.NewWithFactory(
			factory,
			"",
			[]string{""},
			topoclient.NewDefaultTopoConfig(),
		), nil
	}
	store, err := newStore(s)
	require.NoError(t, err)
	source := topoclient.NewMultipooler("k8s-unproven-process", "cell1", "10.0.0.1")
	source.ManagementMode = pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	source.SourceConnection = "external"
	source.ShardKey = &pb.ShardKey{Database: "postgres", TableGroup: "default", Shard: "0-inf"}
	require.NoError(t, store.RegisterMultipooler(t.Context(), source, false))
	require.NoError(t, store.Close())
	c := fake.NewClientBuilder().WithScheme(scheme).WithObjects(s).Build()
	r := &ShardReconciler{Client: c, Scheme: scheme, CreateTopoStore: newStore}
	_, err = r.reconcileUnmanagedPoolers(t.Context(), s, false)
	require.ErrorContains(t, err, "no shutdown proof")
	pods := &corev1.PodList{}
	require.NoError(t, c.List(t.Context(), pods))
	require.Empty(t, pods.Items)
}
