package cell

import (
	"testing"

	v1 "github.com/multigres/multigres-operator/api/v1alpha1"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"
)

func TestMigrationGatewayUsesDerivedTokenOnly(t *testing.T) {
	scheme := runtime.NewScheme()
	require.NoError(t, v1.AddToScheme(scheme))
	c := &v1.Cell{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cell",
			Namespace: "default",
			UID:       "cell-uid",
			Labels:    map[string]string{"multigres.com/cluster": "cluster"},
		},
		Spec: v1.CellSpec{
			Name:        "cell1",
			InternalTLS: &v1.InternalTLSConfig{Enabled: ptr.To(true)},
			ServingControlTokenSecretRef: &corev1.SecretKeySelector{
				LocalObjectReference: corev1.LocalObjectReference{Name: "derived-token"},
				Key:                  "token",
			},
		},
	}
	deployment, err := BuildMultigatewayDeployment(c, scheme)
	require.NoError(t, err)
	pod := deployment.Spec.Template.Spec
	require.Contains(t, pod.Containers[0].Args, "--pg-admin-port=5434")
	require.Contains(
		t,
		pod.Containers[0].Args,
		"--serving-control-token-file=/etc/multigres/serving/token",
	)
	for _, volume := range pod.Volumes {
		require.NotEqual(t, "migration-key", volume.Name)
		if volume.Name == "serving-token" {
			require.Equal(t, "derived-token", volume.Secret.SecretName)
		}
	}
	service, err := BuildMultigatewayService(c, scheme)
	require.NoError(t, err)
	for _, port := range service.Spec.Ports {
		require.NotEqual(
			t,
			int32(5434),
			port.Port,
			"admin listener requires explicit port-forward/private Service",
		)
	}
	c.Spec.InternalTLS = nil
	_, err = BuildMultigatewayDeployment(c, scheme)
	require.Error(t, err)
}
