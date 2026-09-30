package topo

import (
	"testing"

	v1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres/go/common/topoclient"
	"github.com/multigres/multigres/go/common/topoclient/memorytopo"
	pb "github.com/multigres/multigres/go/pb/clustermetadata"
	"github.com/stretchr/testify/require"
)

func TestMissingSourcePodDoesNotRemoveFencingIdentity(t *testing.T) {
	ctx := t.Context()
	_, factory := memorytopo.NewServerAndFactory(ctx, "cell1")
	store := topoclient.NewWithFactory(factory, "", []string{""}, topoclient.NewDefaultTopoConfig())
	defer func() { _ = store.Close() }()
	source := topoclient.NewMultipooler("k8s-missing-process", "cell1", "10.0.0.1")
	source.ManagementMode = pb.PoolerManagementMode_POOLER_MANAGEMENT_MODE_UNMANAGED
	source.SourceConnection = "source"
	source.ShardKey = &pb.ShardKey{Database: "db", TableGroup: "tg", Shard: "0"}
	source.RoutingState = &pb.RoutingState{Role: pb.RoutingRole_ROUTING_ROLE_PRIMARY}
	require.NoError(t, store.RegisterMultipooler(ctx, source, false))
	s := &v1.Shard{
		Spec: v1.ShardSpec{
			DatabaseName:   "db",
			TableGroupName: "tg",
			ShardName:      "0",
			Pools:          map[v1.PoolName]v1.PoolSpec{"target": {Cells: []v1.CellName{"cell1"}}},
		},
	}
	marked, err := MarkDeadPoolers(ctx, store, s, map[string]bool{})
	require.NoError(t, err)
	require.Zero(t, marked)
	retained, err := store.GetMultipooler(ctx, source.Id)
	require.NoError(t, err)
	require.Equal(
		t,
		pb.PoolerLifecycleStatus_LIFECYCLE_STARTING,
		retained.GetLifecycleStatus().GetStatus(),
	)
	require.False(t, IsPrimaryPooler(source), "source does not count as a consensus primary")
}
