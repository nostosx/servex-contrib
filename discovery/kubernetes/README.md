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
- Instance metadata holds `zone` and `node` when known.
- Each `Watch` runs an EndpointSlice informer until the Watcher is stopped.

## Install

Requires Go 1.27.3. This module is unreleased; use the
[local checkout instructions](../../README.md#development), including a
replacement for the core in your application's `go.mod`.

After a release is available:

```sh
go get github.com/nostosx/servex-contrib/discovery/kubernetes@v1.0.0
```

The example version corresponds to the tag `discovery/kubernetes/v1.0.0`;
see [module versioning](../../README.md#versioning).

## Usage

```go
package main

import (
	"context"
	"log"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"

	k8sdiscovery "github.com/nostosx/servex-contrib/discovery/kubernetes"
	"github.com/nostosx/servex/transport/httpx"
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
