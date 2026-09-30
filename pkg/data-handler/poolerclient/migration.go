package poolerclient

import (
	"context"
	"fmt"

	v1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/multigres/multigres-operator/pkg/util/metadata"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// MigrationTransport reads the current operator identity without using the
// label-filtered cache. Source lifecycle reads require internal mTLS.
func MigrationTransport(
	ctx context.Context,
	reader client.Reader,
	s *v1.Shard,
) (grpc.DialOption, error) {
	if reader == nil || !s.Spec.InternalTLS.IsEnabled() {
		return nil, fmt.Errorf(
			"migration lifecycle requires uncached Secret reader and internal mTLS",
		)
	}
	cluster := s.Labels[metadata.LabelMultigresCluster]
	secret := &corev1.Secret{}
	if err := reader.Get(
		ctx,
		client.ObjectKey{
			Namespace: s.Namespace,
			Name:      v1.ComponentCertSecretName(v1.ComponentOperatorTLS, cluster, s.Namespace),
		},
		secret,
	); err != nil {
		return nil, err
	}
	conf, err := buildTLSConfig(
		secret,
		v1.ComponentCertCommonName(v1.ComponentMultiPoolerTLS, cluster, s.Namespace),
	)
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(credentials.NewTLS(conf)), nil
}
