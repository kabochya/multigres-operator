package shard

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	v1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/data-handler/poolerclient"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"github.com/multigres/multigres/go/common/migrationcontrol"
	"github.com/multigres/multigres/go/common/topoclient"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	rpc "github.com/multigres/multigres/go/pb/multipoolerservice"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func (r *ShardReconciler) migrationRouting(
	ctx context.Context,
	s *v1.Shard,
) (*pb.MigrationRouting, error) {
	if r.ReadMigrationRouting != nil {
		return r.ReadMigrationRouting(ctx, s)
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	transport, err := poolerclient.MigrationTransport(ctx, r.APIReader, s)
	if err != nil {
		return nil, err
	}
	store, err := r.topoStore(ctx, s)
	if err != nil {
		return nil, err
	}
	defer func() { _ = store.Close() }()
	var state *pb.MigrationRouting
	err = migrationcontrol.WithAuthority(
		ctx,
		store,
		string(s.Spec.DatabaseName),
		transport,
		func(c rpc.MultipoolerServiceClient) error {
			response, e := c.GetMigrationMode(
				ctx,
				&rpc.GetMigrationModeRequest{Database: string(s.Spec.DatabaseName)},
			)
			if e != nil {
				return e
			}
			state = response.Routing
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	if state == nil {
		return nil, fmt.Errorf("migration authority uninitialized")
	}
	return state, nil
}

func sourceProcessStopped(p *corev1.Pod) bool {
	// Missing pods, node loss, a deletion timestamp, and readiness do not prove
	// the old process stopped. A never-restarting pod's runtime termination does.
	return len(p.Status.ContainerStatuses) == 1 &&
		p.Status.ContainerStatuses[0].Name == "multipooler" &&
		p.Status.ContainerStatuses[0].State.Terminated != nil
}

func (r *ShardReconciler) finishSourceShutdown(
	ctx context.Context,
	s *v1.Shard,
	p *corev1.Pod,
) error {
	store, err := r.topoStore(ctx, s)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	if p.UID == "" {
		return fmt.Errorf("source pod missing process UID")
	}
	id := &pb.ID{
		Component: pb.ID_MULTIPOOLER,
		Cell:      p.Labels[metadata.LabelMultigresCell],
		Name:      "k8s-" + string(p.UID),
	}
	if err = store.UnregisterMultipooler(
		ctx,
		id,
	); err != nil &&
		!errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		return err
	}
	patch := client.MergeFrom(p.DeepCopy())
	p.Finalizers = slices.DeleteFunc(
		p.Finalizers,
		func(s string) bool { return s == unmanagedFinalizer },
	)
	return r.Patch(ctx, p, patch)
}

// reconcileUnmanagedPoolers never enters the managed pod drain/replacement
// path. Removing desired sources requires completion; changing their spec
// requires FENCED or completion. A runtime-confirmed dead process can recover.
func (r *ShardReconciler) reconcileUnmanagedPoolers(
	ctx context.Context,
	s *v1.Shard,
	removeAll bool,
) (bool, error) {
	existing := &corev1.PodList{}
	if err := r.List(
		ctx,
		existing,
		client.InNamespace(s.Namespace),
		client.MatchingLabels(metadata.GetSelectorLabels(sourceLabels(s))),
	); err != nil {
		return false, err
	}
	desired := map[string]*corev1.Pod{}
	if !removeAll {
		for name, spec := range s.Spec.UnmanagedPoolers {
			replicas := spec.ReplicasPerCell
			if replicas == 0 {
				replicas = 1
			}
			for _, cell := range spec.Cells {
				for i := 0; i < int(replicas); i++ {
					pod, err := BuildUnmanagedPoolerPod(s, name, string(cell), spec, i, r.Scheme)
					if err != nil {
						return false, err
					}
					desired[pod.Name] = pod
				}
			}
		}
	}
	pending := false
	for i := range existing.Items {
		pod := &existing.Items[i]
		want := desired[pod.Name]
		delete(desired, pod.Name)
		if !pod.DeletionTimestamp.IsZero() {
			pending = true
			if sourceProcessStopped(pod) {
				if err := r.finishSourceShutdown(ctx, s, pod); err != nil {
					return true, err
				}
			}
			continue
		}
		stopped := sourceProcessStopped(pod)
		drift := want != nil &&
			pod.Annotations[metadata.AnnotationSpecHash] != want.Annotations[metadata.AnnotationSpecHash]
		if want != nil && !drift && !stopped {
			continue
		}
		if !stopped {
			state, err := r.migrationRouting(ctx, s)
			if err != nil {
				return true, err
			}
			if want == nil && !state.MigrationCompleted {
				pending = true
				continue
			}
			if drift && !state.MigrationCompleted &&
				state.Mode != pb.MigrationMode_MIGRATION_MODE_FENCED {
				pending = true
				continue
			}
		}
		// Deletion requests SIGTERM. Retain the Pod finalizer and its topology
		// identity until Kubernetes confirms the container has terminated.
		if err := r.Delete(ctx, pod); err != nil {
			return true, err
		}
		pending = true
	}
	// Do not add capacity while old specs or removed replicas still await drain.
	if pending && len(desired) > 0 {
		return true, nil
	}
	if len(desired) > 0 {
		// A Pod disappearing is not a process-death proof. Keep unknown identities
		// in the fence set and block new capacity until an operator proves shutdown.
		store, err := r.topoStore(ctx, s)
		if err != nil {
			return pending, err
		}
		defer func() { _ = store.Close() }()
		poolers, err := migrationcontrol.Poolers(ctx, store, string(s.Spec.DatabaseName))
		if err != nil {
			return pending, err
		}
		for _, p := range desired {
			if _, err := store.GetCell(ctx, p.Labels[metadata.LabelMultigresCell]); err != nil {
				return pending, fmt.Errorf("source cell must already exist: %w", err)
			}
		}
		known := map[string]bool{}
		for _, p := range existing.Items {
			known["k8s-"+string(p.UID)] = true
		}
		for _, p := range poolers {
			if p.ManagementMode == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED &&
				strings.HasPrefix(p.GetId().GetName(), "k8s-") &&
				!known[p.GetId().GetName()] {
				return pending, fmt.Errorf(
					"source process %s has no shutdown proof; refusing replacement",
					p.GetId().GetName(),
				)
			}
		}
	}
	for _, pod := range desired {
		if err := r.Create(ctx, pod); err != nil {
			return pending, err
		}
	}
	return pending, nil
}
