package nacos

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"math"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

// Defaults for Registry options.
const (
	defaultGroup   = "DEFAULT_GROUP"
	defaultCluster = "DEFAULT"
)

// Metadata keys that carry the parts of an instance Nacos has no field for.
// Keys with this prefix are reserved: user metadata using it is overwritten
// on registration and dropped on discovery.
const (
	metaPrefix   = "servex."
	metaID       = metaPrefix + "id"
	metaVersion  = metaPrefix + "version"
	metaEndpoint = metaPrefix + "endpoint"
	metaIndex    = metaPrefix + "index"
)

// Error reasons used by this package.
const (
	reasonInvalid     = "INVALID_INSTANCE"
	reasonUnavailable = "REGISTRY_UNAVAILABLE"
)

// Compile-time interface checks.
var (
	_ discovery.Registrar  = (*Registry)(nil)
	_ discovery.Discoverer = (*Registry)(nil)
)

// Option configures a Registry.
type Option func(*options)

// options holds the configuration collected from Options.
type options struct {
	group   string
	cluster string
}

// WithGroup sets the Nacos group of registered and watched services. The
// default is "DEFAULT_GROUP"; an empty group keeps the default.
func WithGroup(s string) Option {
	return func(o *options) {
		if s != "" {
			o.group = s
		}
	}
}

// WithCluster sets the Nacos cluster of registered instances; watchers
// only see instances of this cluster. The default is "DEFAULT"; an empty
// cluster keeps the default.
func WithCluster(s string) Option {
	return func(o *options) {
		if s != "" {
			o.cluster = s
		}
	}
}

// Registry is a Nacos backed service registry. It implements both
// discovery.Registrar and discovery.Discoverer. The naming client is owned
// by the caller and is not closed by the Registry. All methods are safe for
// concurrent use.
//
// Every endpoint of an instance becomes an ephemeral Nacos instance with
// the endpoint's host and port, kept alive by the naming client's
// connection. The instance ID, version and endpoint URL are stored in
// metadata under the reserved "servex." prefix, so instances need at least
// one endpoint with a port. Each Nacos instance gets the Weight of the
// servex instance, or the Nacos default weight 1 for a non-positive Weight.
// Watchers report the Nacos weight of the first endpoint, rounded to the
// nearest integer and at least 1. A Nacos 2 client publishes one set of instances
// per service, so the Registry always publishes all instances of a service
// registered through it together.
//
// The naming client must be created with
// constant.WithUpdateCacheWhenEmpty(true): by default it ignores empty
// instance lists pushed by the server, so watchers would keep reporting the
// last instances of a service that became empty.
//
// The naming client does not accept a context: methods check ctx before
// calling it and otherwise rely on the client's own timeouts.
type Registry struct {
	client  naming_client.INamingClient
	group   string
	cluster string

	// mu serializes Register and Deregister and guards services.
	mu sync.Mutex
	// services holds, by service name and instance ID, the Nacos instances
	// published through this Registry.
	services map[string]map[string][]vo.RegisterInstanceParam
}

// New returns a Registry that uses client.
func New(client naming_client.INamingClient, opts ...Option) *Registry {
	o := options{group: defaultGroup, cluster: defaultCluster}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Registry{
		client:   client,
		group:    o.group,
		cluster:  o.cluster,
		services: make(map[string]map[string][]vo.RegisterInstanceParam),
	}
}

// Register publishes ins, replacing any instance with the same Name and ID.
func (r *Registry) Register(ctx context.Context, ins *discovery.Instance) error {
	if err := validate(ins); err != nil {
		return err
	}
	params, err := r.params(ins)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	set := maps.Clone(r.services[ins.Name])
	if set == nil {
		set = make(map[string][]vo.RegisterInstanceParam)
	}
	set[ins.ID] = params
	if err := r.publish(ins.Name, set); err != nil {
		return backendErr(fmt.Sprintf("register %q of %q", ins.ID, ins.Name), err)
	}
	r.services[ins.Name] = set
	return nil
}

// Deregister removes the instance with the Name and ID of ins. Ephemeral
// Nacos instances belong to the client connection that published them, so
// only instances registered through this Registry can be removed; removing
// any other instance is a no-op, as is removing an unknown one.
func (r *Registry) Deregister(ctx context.Context, ins *discovery.Instance) error {
	if err := validate(ins); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	set := r.services[ins.Name]
	removed, ok := set[ins.ID]
	if !ok {
		return nil
	}
	rest := maps.Clone(set)
	delete(rest, ins.ID)
	op := fmt.Sprintf("deregister %q of %q", ins.ID, ins.Name)
	if len(rest) > 0 {
		if err := r.publish(ins.Name, rest); err != nil {
			return backendErr(op, err)
		}
		r.services[ins.Name] = rest
		return nil
	}
	// Deregistering removes everything this client published for the
	// service.
	p := removed[0]
	done, err := r.client.DeregisterInstance(vo.DeregisterInstanceParam{
		Ip:          p.Ip,
		Port:        p.Port,
		Cluster:     r.cluster,
		ServiceName: ins.Name,
		GroupName:   r.group,
		Ephemeral:   true,
	})
	if err == nil && !done {
		err = errors.New("deregistration not accepted")
	}
	if err != nil {
		return backendErr(op, err)
	}
	delete(r.services, ins.Name)
	return nil
}

// publish publishes all Nacos instances of set for service name, replacing
// whatever this client published for it before.
func (r *Registry) publish(name string, set map[string][]vo.RegisterInstanceParam) error {
	var all []vo.RegisterInstanceParam
	for _, id := range slices.Sorted(maps.Keys(set)) {
		all = append(all, set[id]...)
	}
	var (
		done bool
		err  error
	)
	if len(all) == 1 {
		done, err = r.client.RegisterInstance(all[0])
	} else {
		done, err = r.client.BatchRegisterInstance(vo.BatchRegisterInstanceParam{
			ServiceName: name,
			GroupName:   r.group,
			Instances:   all,
		})
	}
	if err == nil && !done {
		err = errors.New("registration not accepted")
	}
	return err
}

// params converts the endpoints of ins into Nacos instances.
func (r *Registry) params(ins *discovery.Instance) ([]vo.RegisterInstanceParam, error) {
	if len(ins.Endpoints) == 0 {
		return nil, errorx.Newf(errorx.InvalidArgument, reasonInvalid, "nacos: instance %q of %q has no endpoints", ins.ID, ins.Name)
	}
	out := make([]vo.RegisterInstanceParam, 0, len(ins.Endpoints))
	for i, ep := range ins.Endpoints {
		u, err := url.Parse(ep)
		if err != nil || u.Scheme == "" || u.Hostname() == "" {
			return nil, errorx.Newf(errorx.InvalidArgument, reasonInvalid, "nacos: instance %q has an invalid endpoint %q", ins.ID, ep)
		}
		port, err := strconv.ParseUint(u.Port(), 10, 16)
		if err != nil || port == 0 {
			return nil, errorx.Newf(errorx.InvalidArgument, reasonInvalid, "nacos: endpoint %q of instance %q has no valid port", ep, ins.ID)
		}
		md := make(map[string]string, len(ins.Metadata)+4)
		for k, v := range ins.Metadata {
			if !strings.HasPrefix(k, metaPrefix) {
				md[k] = v
			}
		}
		md[metaID] = ins.ID
		md[metaEndpoint] = ep
		md[metaIndex] = strconv.Itoa(i)
		if ins.Version != "" {
			md[metaVersion] = ins.Version
		}
		out = append(out, vo.RegisterInstanceParam{
			Ip:          u.Hostname(),
			Port:        port,
			Weight:      float64(max(ins.Weight, 1)),
			Enable:      true,
			Healthy:     true,
			Metadata:    md,
			ClusterName: r.cluster,
			ServiceName: ins.Name,
			GroupName:   r.group,
			Ephemeral:   true,
		})
	}
	return out, nil
}

// Watch returns a Watcher for the service name that follows a naming
// subscription until the Watcher is stopped. Only healthy, enabled
// instances with a positive weight in the configured cluster are reported;
// Nacos instances without servex metadata are ignored.
func (r *Registry) Watch(ctx context.Context, name string) (discovery.Watcher, error) {
	if name == "" {
		return nil, errorx.New(errorx.InvalidArgument, reasonInvalid, "nacos: empty service name")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	st := newState()
	// Notifications are authoritative: once one arrived, the initial read
	// below may be older and is discarded.
	var (
		mu       sync.Mutex
		notified bool
	)
	param := &vo.SubscribeParam{
		ServiceName: name,
		GroupName:   r.group,
		Clusters:    []string{r.cluster},
	}
	param.SubscribeCallback = func(list []model.Instance, err error) {
		if err != nil {
			return
		}
		mu.Lock()
		defer mu.Unlock()
		notified = true
		st.set(instances(name, list))
	}
	// The same param must be passed to Unsubscribe: the client identifies
	// the callback by the address of its SubscribeCallback field.
	if err := r.client.Subscribe(param); err != nil {
		return nil, backendErr(fmt.Sprintf("watch %q", name), err)
	}
	list, err := r.client.SelectAllInstances(vo.SelectAllInstancesParam{
		ServiceName: name,
		GroupName:   r.group,
		Clusters:    []string{r.cluster},
	})
	if err != nil {
		_ = r.client.Unsubscribe(param)
		return nil, backendErr(fmt.Sprintf("watch %q", name), err)
	}
	mu.Lock()
	if !notified {
		st.set(instances(name, list))
	}
	mu.Unlock()
	return newWatcher(st, func() error {
		if err := r.client.Unsubscribe(param); err != nil {
			return backendErr(fmt.Sprintf("unsubscribe %q", name), err)
		}
		return nil
	}), nil
}

// instances groups the usable Nacos instances of service name by servex
// instance ID.
func instances(name string, list []model.Instance) []*discovery.Instance {
	type endpoint struct {
		url    string
		index  int
		weight float64
	}
	byID := make(map[string]*discovery.Instance)
	eps := make(map[string][]endpoint)
	for _, n := range list {
		id, ep := n.Metadata[metaID], n.Metadata[metaEndpoint]
		if !n.Healthy || !n.Enable || n.Weight <= 0 || id == "" || ep == "" {
			continue
		}
		if _, ok := byID[id]; !ok {
			ins := &discovery.Instance{ID: id, Name: name, Version: n.Metadata[metaVersion]}
			for k, v := range n.Metadata {
				if !strings.HasPrefix(k, metaPrefix) {
					if ins.Metadata == nil {
						ins.Metadata = make(map[string]string)
					}
					ins.Metadata[k] = v
				}
			}
			byID[id] = ins
		}
		index, _ := strconv.Atoi(n.Metadata[metaIndex])
		eps[id] = append(eps[id], endpoint{ep, index, n.Weight})
	}
	out := make([]*discovery.Instance, 0, len(byID))
	for id, ins := range byID {
		list := eps[id]
		slices.SortFunc(list, func(a, b endpoint) int {
			return cmp.Or(cmp.Compare(a.index, b.index), cmp.Compare(a.url, b.url))
		})
		for _, e := range list {
			ins.Endpoints = append(ins.Endpoints, e.url)
		}
		ins.Weight = weight(list[0].weight)
		out = append(out, ins)
	}
	return out
}

// weight converts the positive Nacos weight w, a float, into an instance
// weight: w rounded to the nearest integer, at least 1 and at most
// math.MaxInt32.
func weight(w float64) int {
	return int(min(max(math.Round(w), 1), math.MaxInt32))
}

// validate checks that ins identifies an instance.
func validate(ins *discovery.Instance) error {
	switch {
	case ins == nil:
		return errorx.New(errorx.InvalidArgument, reasonInvalid, "nacos: nil instance")
	case ins.Name == "":
		return errorx.Newf(errorx.InvalidArgument, reasonInvalid, "nacos: instance %q has an empty name", ins.ID)
	case ins.ID == "":
		return errorx.Newf(errorx.InvalidArgument, reasonInvalid, "nacos: instance of %q has an empty id", ins.Name)
	}
	return nil
}

// backendErr classifies a failed naming client call. Context errors are
// kept as they are; everything else is reported as a retryable
// errorx.Unavailable whose cause is err.
func backendErr(op string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("nacos: %s: %w", op, err)
	}
	return errorx.New(errorx.Unavailable, reasonUnavailable, "nacos: "+op).WithCause(err)
}
