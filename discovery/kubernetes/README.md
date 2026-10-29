# discovery/kubernetes

`github.com/nostosx/servex-contrib/discovery/kubernetes` implements the servex
`discovery.Discoverer` contract on Kubernetes EndpointSlices
(`discovery.k8s.io/v1`) through [client-go](https://github.com/kubernetes/client-go).
It does not implement `Registrar`: Kubernetes itself maintains the endpoints of
a Service.

- The service name is the name of a Kubernetes Service in the Discoverer's
  namespace (an empty namespace means `default`).
- Every ready endpoint becomes an instance whose ID is the target object
  name (usually the Pod name), or the first address when no target name is
  present. Each named port adds the endpoint
  `<port name>://<address>:<port>`, so ports named `http` and `grpc` serve the
  transports with those schemes; unnamed ports are ignored.
- Instance metadata holds `zone` and `node` when known. EndpointSlices carry
  no weight, so instances have `Weight` 0, the selector default.
- Each `Watch` runs an EndpointSlice informer until the Watcher is stopped.

## Install

Requires Go 1.27.3. Uses published servex v1.4.0 and kitx v1.0.0.

Run this in your application module (create one with `go mod init` if needed):

```sh
go get github.com/nostosx/servex-contrib/discovery/kubernetes@v1.2.0
```

The example version corresponds to the tag `discovery/kubernetes/v1.2.0`;
see [module versioning](../../README.md#versioning).

## Usage

Save a complete program below as `main.go`, then run `go mod tidy` and
`go run .` from that application directory.

This program runs inside a Kubernetes Pod, not directly on your laptop:
`rest.InClusterConfig` reads the Pod's service-account credentials. In the
`shop` namespace, provide a `user` Service with a port named `http`, ready
backing Pods and a `GET /v1/users/7` endpoint returning `{"name":"alice"}`.
Grant the calling Pod's service account `get`, `list` and `watch` on
`endpointslices` in API group `discovery.k8s.io`, scoped to `shop`.

For local execution, replace `rest.InClusterConfig()` with
`clientcmd.BuildConfigFromFlags("", kubeconfigPath)` from
`k8s.io/client-go/tools/clientcmd`, using a dedicated cluster's kubeconfig and
the same namespace permissions. The program prints the returned user's name
and exits; it does not create a Service, deployment or RBAC policy.

```go
package main

import (
	"context"
	"log"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	k8sdiscovery "github.com/nostosx/servex-contrib/discovery/kubernetes"
	"github.com/nostosx/servex-contrib/transport/httpx"
)

type User struct {
	Name string `json:"name"`
}

func main() {
	cfg, err := rest.InClusterConfig() // needs RBAC to list and watch endpointslices
	if err != nil {
		log.Fatal(err)
	}
	clientset, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		log.Fatal(err)
	}
	dis := k8sdiscovery.New(clientset, "shop")

	// Calls go to the ready endpoints of the Service "user" on its port named "http".
	ctx := context.Background()
	client, err := httpx.NewClient(ctx, "discovery:///user", httpx.WithDiscoverer(dis))
	if err != nil {
		log.Fatal(err)
	}
	defer client.Close()

	reply, err := httpx.Call[User](client, "GET", "/v1/users/{id}")(ctx, map[string]any{"id": 7})
	if err != nil {
		log.Fatal(err)
	}
	log.Println(reply.(*User).Name)
}
```

## Options

| Option | Default | Description |
|---|---|---|
| `WithResyncPeriod(d)` | 0 (disabled) | Resync period of the EndpointSlice informers. |

## Testing

Unit tests use the client-go fake clientset. Integration tests run against a
real cluster (for example kind) and create a private namespace per test.
The kubeconfig needs permission to create/delete namespaces and to manage,
list and watch EndpointSlices within them:

```sh
SERVEX_IT_KUBECONFIG=$HOME/.kube/config GOWORK=off go test -race -count=1 -tags integration ./...
```

## License

Licensed under the [MIT License](../../LICENSE).

## Release

Version v1.2.0, released 2026-10-29. Requires servex v1.4.0. Install from the public Go proxy without source replacements:

```sh
go get github.com/nostosx/servex-contrib/discovery/kubernetes@v1.2.0
```
