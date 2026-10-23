package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"slices"
	"strconv"
	"sync"
	"time"

	discoveryv1 "k8s.io/api/discovery/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

// Metadata keys set on discovered instances.
const (
	// MetadataZone holds the zone of the endpoint, when known.
	MetadataZone = "zone"
	// MetadataNode holds the name of the node hosting the endpoint, when known.
	MetadataNode = "node"
)

// Error reasons used by this package.
const (
	reasonInvalid     = "INVALID_SERVICE_NAME"
	reasonUnavailable = "REGISTRY_UNAVAILABLE"
)

// Compile-time interface check.
var _ discovery.Discoverer = (*Discoverer)(nil)

// Option configures a Discoverer.
type Option func(*options)

// options holds the configuration collected from Options.
type options struct {
	resync time.Duration
}

// WithResyncPeriod sets the resync period of the EndpointSlice informers.
// The default, 0, disables periodic resyncs; watchers are updated on every
// change either way.
func WithResyncPeriod(d time.Duration) Option {
	return func(o *options) {
		if d >= 0 {
			o.resync = d
		}
	}
}

// Discoverer discovers the endpoints of Kubernetes Services in one
// namespace from their EndpointSlices. The client is owned by the caller.
// All methods are safe for concurrent use.
//
// Every ready endpoint (an endpoint whose ready condition is true or
// unknown) becomes an instance whose ID is the name of the target object,
// usually the Pod, or its first address when it has no target. Each named
// port of its EndpointSlice adds the endpoint "<port name>://<address>:<port>"
// to the instance, so ports named "http" and "grpc" serve the transports
// with those schemes; unnamed ports are ignored. Version is always empty;
// Metadata holds MetadataZone and MetadataNode when they are known.
type Discoverer struct {
	client    kubernetes.Interface
	namespace string
	resync    time.Duration
}

// New returns a Discoverer for the Services of namespace. An empty
// namespace means "default".
func New(client kubernetes.Interface, namespace string, opts ...Option) *Discoverer {
	o := options{}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	if namespace == "" {
		namespace = metav1.NamespaceDefault
	}
	return &Discoverer{client: client, namespace: namespace, resync: o.resync}
}

// Watch returns a Watcher for the Service name. It starts an informer for
// the EndpointSlices of the Service and waits, bounded by ctx, for its
// initial list; the informer runs until the Watcher is stopped.
func (d *Discoverer) Watch(ctx context.Context, name string) (discovery.Watcher, error) {
	if name == "" {
		return nil, errorx.New(errorx.InvalidArgument, reasonInvalid, "kubernetes: empty service name")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	selector := labels.Set{discoveryv1.LabelServiceName: name}.String()
	factory := informers.NewSharedInformerFactoryWithOptions(d.client, d.resync,
		informers.WithNamespace(d.namespace),
		informers.WithTweakListOptions(func(o *metav1.ListOptions) { o.LabelSelector = selector }),
	)
	inf := factory.Discovery().V1().EndpointSlices().Informer()

	// lastErr remembers the latest list or watch failure so that a Watch
	// that times out can report why.
	var (
		mu      sync.Mutex
		lastErr error
	)
	if err := inf.SetWatchErrorHandlerWithContext(func(_ context.Context, _ *cache.Reflector, err error) {
		mu.Lock()
		lastErr = err
		mu.Unlock()
	}); err != nil {
		return nil, fmt.Errorf("kubernetes: watch %q: %w", name, err)
	}
	st := newState()
	r := &refresher{name: name, list: inf.GetStore().List, st: st}
	if _, err := inf.AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    func(any) { r.refresh() },
		UpdateFunc: func(any, any) { r.refresh() },
		DeleteFunc: func(any) { r.refresh() },
	}); err != nil {
		return nil, fmt.Errorf("kubernetes: watch %q: %w", name, err)
	}

	bg, cancel := context.WithCancel(context.WithoutCancel(ctx))
	factory.StartWithContext(bg)
	if !cache.WaitForCacheSync(ctx.Done(), inf.HasSynced) {
		cancel()
		factory.Shutdown()
		cause := context.Cause(ctx)
		if cause == nil {
			cause = ctx.Err()
		}
		mu.Lock()
		defer mu.Unlock()
		if lastErr != nil {
			return nil, errorx.Newf(errorx.Unavailable, reasonUnavailable, "kubernetes: list endpoint slices of %q", name).
				WithCause(errors.Join(cause, lastErr))
		}
		return nil, fmt.Errorf("kubernetes: wait for endpoint slices of %q: %w", name, cause)
	}
	// The event handlers run asynchronously, so publish the synced store
	// now; the first Next must not miss the initial list.
	r.refresh()

	w := newWatcher(st, cancel)
	go func() {
		defer close(w.done)
		<-bg.Done()
		factory.Shutdown()
	}()
	return w, nil
}

// refresher publishes the EndpointSlices of one service to a state. It is
// called both from the informer's event handlers and from Watch itself.
type refresher struct {
	// mu makes listing the store and publishing the result one atomic step.
	// Without it, a caller that listed an older store could publish after a
	// caller that listed a newer one, leaving the state stale until the next
	// EndpointSlice event.
	mu   sync.Mutex
	name string
	list func() []any
	st   *state
}

// refresh replaces the instances of r's state with the current contents of
// the store. The informer updates its store before it notifies handlers, so
// the last refresh always observes the latest store.
func (r *refresher) refresh() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.st.set(instances(r.name, r.list()))
}

// instances converts the EndpointSlices of service name in objs into
// instances.
func instances(name string, objs []any) []*discovery.Instance {
	byID := make(map[string]*discovery.Instance)
	for _, obj := range objs {
		slice, ok := obj.(*discoveryv1.EndpointSlice)
		if !ok || slice.Labels[discoveryv1.LabelServiceName] != name {
			continue
		}
		for _, ep := range slice.Endpoints {
			if ep.Conditions.Ready != nil && !*ep.Conditions.Ready || len(ep.Addresses) == 0 {
				continue
			}
			// Addresses are fungible; consumers may use the first one.
			addr := ep.Addresses[0]
			id := addr
			if ep.TargetRef != nil && ep.TargetRef.Name != "" {
				id = ep.TargetRef.Name
			}
			ins, ok := byID[id]
			if !ok {
				ins = &discovery.Instance{ID: id, Name: name}
				byID[id] = ins
			}
			if ep.Zone != nil && *ep.Zone != "" {
				setMetadata(ins, MetadataZone, *ep.Zone)
			}
			if ep.NodeName != nil && *ep.NodeName != "" {
				setMetadata(ins, MetadataNode, *ep.NodeName)
			}
			for _, p := range slice.Ports {
				if p.Name == nil || *p.Name == "" || p.Port == nil {
					continue
				}
				u := *p.Name + "://" + net.JoinHostPort(addr, strconv.Itoa(int(*p.Port)))
				if !slices.Contains(ins.Endpoints, u) {
					ins.Endpoints = append(ins.Endpoints, u)
				}
			}
		}
	}
	out := make([]*discovery.Instance, 0, len(byID))
	for _, ins := range byID {
		slices.Sort(ins.Endpoints)
		out = append(out, ins)
	}
	return out
}

// setMetadata sets one metadata entry of ins.
func setMetadata(ins *discovery.Instance, k, v string) {
	if ins.Metadata == nil {
		ins.Metadata = make(map[string]string)
	}
	ins.Metadata[k] = v
}
