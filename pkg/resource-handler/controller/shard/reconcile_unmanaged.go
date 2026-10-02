package shard

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

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

func (r *ShardReconciler) sourceLifecycle(
	ctx context.Context,
	s *v1.Shard,
	connection string,
) (*pb.SourceLifecycleAuthorization, error) {
	if r.ReadSourceLifecycle != nil {
		return r.ReadSourceLifecycle(ctx, s, connection)
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
	key, err := poolerclient.MigrationKey(ctx, r.APIReader, s)
	if err != nil {
		return nil, err
	}
	var state *pb.SourceLifecycleAuthorization
	err = migrationcontrol.WithAuthority(
		ctx,
		store,
		string(s.Spec.DatabaseName),
		transport,
		func(c rpc.MultipoolerServiceClient) error {
			response, e := c.ReadSourceLifecycle(
				migrationcontrol.AuthorizedContext(ctx, key),
				&rpc.ReadSourceLifecycleRequest{
					Database:         string(s.Spec.DatabaseName),
					SourceConnection: connection,
				},
			)
			if e != nil {
				return e
			}
			if response.GetAuthorityShardKey().GetDatabase() != string(s.Spec.DatabaseName) ||
				response.GetAuthorityShardKey().GetTableGroup() != string(s.Spec.TableGroupName) ||
				response.GetAuthorityShardKey().GetShard() != string(s.Spec.ShardName) {
				return fmt.Errorf("source lifecycle authority scope mismatch")
			}
			state = response.Authorization
			return nil
		},
	)
	if err != nil {
		return nil, err
	}
	if state != nil &&
		(state.Owner == "" || state.ClosedIntentId == "" || state.SourceConnection != connection || state.SourceConfigurationBinding == "") {
		return nil, fmt.Errorf("source lifecycle authorization incomplete")
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
	// Preserve shutdown evidence for in-flight fence recovery. A missing record
	// can be recreated only because Kubernetes proved this exact UID terminated.
	_, err = store.UpdateMultipoolerFields(ctx, id, func(pooler *pb.Multipooler) error {
		pooler.ServingStatus = pb.PoolerServingStatus_DISABLED
		pooler.RoutingState = nil
		pooler.LifecycleStatus = &pb.PoolerLifecycle{
			Status:  pb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
			Reason:  "source container termination confirmed",
			Updated: timestamppb.Now(),
		}
		return nil
	})
	if errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
		err = store.RegisterMultipooler(
			ctx,
			&pb.Multipooler{
				Id: id,
				ShardKey: &pb.ShardKey{
					Database:   string(s.Spec.DatabaseName),
					TableGroup: string(s.Spec.TableGroupName),
					Shard:      string(s.Spec.ShardName),
				},
				ManagementMode:   pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED,
				SourceConnection: p.Labels[sourceConnectionLabel],
				ServingStatus:    pb.PoolerServingStatus_DISABLED,
				LifecycleStatus: &pb.PoolerLifecycle{
					Status:  pb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN,
					Reason:  "source container termination confirmed",
					Updated: timestamppb.Now(),
				},
			},
			true,
		)
	}
	if err != nil {
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
// path. Removing or replacing live sources requires explicit controller retirement
// authorization. Routing/readiness never authorize deletion. A runtime-confirmed dead process can recover.
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
			state, err := r.sourceLifecycle(ctx, s, pod.Labels[sourceConnectionLabel])
			if err != nil {
				return true, err
			}
			if state == nil || !state.RetireSource {
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
				(p.GetLifecycleStatus().GetStatus() != pb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN || p.GetLifecycleStatus().GetReason() != "source container termination confirmed") &&
				!known[p.GetId().GetName()] {
				return pending, fmt.Errorf(
					"source process %s has no shutdown proof; refusing replacement",
					p.GetId().GetName(),
				)
			}
		}
	}
	if !pending && len(desired) == 0 && len(existing.Items) == 0 &&
		s.Spec.MigrationKeySecretRef != nil &&
		(removeAll || len(s.Spec.UnmanagedPoolers) == 0) {
		if err := r.cleanupSourceShutdownProof(ctx, s); err != nil {
			return true, err
		}
	}
	for _, pod := range desired {
		if err := r.Create(ctx, pod); err != nil {
			return pending, err
		}
	}
	return pending, nil
}

// Proof cleanup requires separate exact-process controller authorization.
func (r *ShardReconciler) cleanupSourceShutdownProof(ctx context.Context, s *v1.Shard) error {
	store, err := r.topoStore(ctx, s)
	if err != nil {
		return err
	}
	defer func() { _ = store.Close() }()
	poolers, err := migrationcontrol.Poolers(ctx, store, string(s.Spec.DatabaseName))
	if err != nil {
		return err
	}
	var retired []*pb.ID
	for _, pooler := range poolers {
		if pooler.ManagementMode == pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED &&
			strings.HasPrefix(pooler.GetId().GetName(), "k8s-") &&
			pooler.GetLifecycleStatus().
				GetStatus() ==
				pb.PoolerLifecycleStatus_LIFECYCLE_SHUTDOWN &&
			pooler.GetLifecycleStatus().GetReason() == "source container termination confirmed" &&
			pooler.GetShardKey().GetTableGroup() == string(s.Spec.TableGroupName) &&
			pooler.GetShardKey().GetShard() == string(s.Spec.ShardName) {
			retired = append(retired, pooler.Id)
		}
	}
	if len(retired) == 0 {
		return nil
	}

	for _, id := range retired {
		record, err := store.GetMultipooler(ctx, id)
		if err != nil {
			return err
		}
		state, err := r.sourceLifecycle(ctx, s, record.GetSourceConnection())
		if err != nil {
			return err
		}
		if state == nil || !state.RetireSource ||
			!slices.ContainsFunc(
				state.ProofReleaseProcesses,
				func(released *pb.ID) bool { return proto.Equal(released, id) },
			) {
			continue
		}
		if err := store.UnregisterMultipooler(
			ctx,
			id,
		); err != nil &&
			!errors.Is(err, &topoclient.TopoError{Code: topoclient.NoNode}) {
			return err
		}
	}
	return nil
}
