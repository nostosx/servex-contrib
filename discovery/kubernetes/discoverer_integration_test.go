//go:build integration

package kubernetes

import (
	"context"
	"os"
	"testing"
	"uuid"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
)

// itClient returns a client for the cluster of the kubeconfig file named by
// SERVEX_IT_KUBECONFIG.
func itClient(t *testing.T) kubernetes.Interface {
	t.Helper()
	path := os.Getenv("SERVEX_IT_KUBECONFIG")
	if path == "" {
		t.Skip("SERVEX_IT_KUBECONFIG is not set")
	}
	cfg, err := clientcmd.BuildConfigFromFlags("", path)
	if err != nil {
		t.Fatal(err)
	}
	client, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		t.Fatal(err)
	}
	return client
}

// itNamespace creates a namespace private to the test.
func itNamespace(t *testing.T, client kubernetes.Interface) string {
	t.Helper()
	ns := &corev1.Namespace{Name: "servex-it-" + uuid.NewV7().String()}
	if _, err := client.CoreV1().Namespaces().Create(t.Context(), ns, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := client.CoreV1().Namespaces().Delete(context.Background(), ns.Name, metav1.DeleteOptions{}); err != nil {
			t.Errorf("cleanup: %v", err)
		}
	})
	return ns.Name
}

// The end-to-end test is not repeated against a real cluster: the API
// server rejects the loopback addresses the local test server listens on.
func TestIntegrationContract(t *testing.T) {
	client := itClient(t)
	runContract(t, func(t *testing.T) contractEnv {
		ns := itNamespace(t, client)
		return contractEnv{reg: sliceRegistrar{client, ns}, dis: New(client, ns)}
	})
}
