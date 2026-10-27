package consul

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/consul/api"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

// Defaults for Registry options.
const (
	defaultTTL             = 15 * time.Second
	defaultDeregisterAfter = time.Minute
	// blockingWait is the maximum duration of one blocking health query.
	blockingWait = 5 * time.Minute
)

// Tags that carry the parts of an instance Consul has no field for.
const (
	tagID       = "servex.id="
	tagVersion  = "servex.version="
	tagEndpoint = "servex.endpoint="
)

// Error reasons used by this package.
const (
	reasonInvalid     = "INVALID_INSTANCE"
	reasonRejected    = "REGISTRY_REJECTED"
	reasonForbidden   = "REGISTRY_FORBIDDEN"
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
	ttl             time.Duration
	deregisterAfter time.Duration
}

// WithTTL sets the TTL of the health check attached to registered
// instances. The Registry reports the check as passing every TTL/3. The
// default is 15s; non-positive values keep the default.
func WithTTL(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.ttl = d
		}
	}
}

// WithDeregisterCriticalServiceAfter sets how long Consul keeps an instance
// whose check is critical, for example because the process died without
// deregistering, before removing it. The default is one minute, the minimum
// Consul honours; non-positive values keep the default.
func WithDeregisterCriticalServiceAfter(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.deregisterAfter = d
		}
	}
}

// Registry is a Consul backed service registry. It implements both
// discovery.Registrar and discovery.Discoverer. The client is owned by the
// caller. All methods are safe for concurrent use.
//
// An instance is registered with the agent as service Name with the
// service ID "<escaped-name>-<escaped-id>", escaping '%' and '-' in each
// component as '%25' and '%2D'. Its Metadata becomes the service meta, so keys
// must satisfy Consul's rules; the address and port of the first endpoint
// become the service address, and the ID, version and all endpoints are
// stored in tags prefixed with "servex.". A positive Weight becomes the
// passing weight of the service, with Consul's default warning weight of 1;
// other weights keep Consul's default weights. Discovered instances carry
// the passing weight, which Consul reports as 1 by default.
type Registry struct {
	client          *api.Client
	ttl             time.Duration
	deregisterAfter time.Duration

	// mu serializes Register and Deregister and guards beats.
	mu    sync.Mutex
	beats map[string]*heartbeat // by service ID
}

// New returns a Registry that uses client.
func New(client *api.Client, opts ...Option) *Registry {
	o := options{ttl: defaultTTL, deregisterAfter: defaultDeregisterAfter}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return &Registry{
		client:          client,
		ttl:             o.ttl,
		deregisterAfter: o.deregisterAfter,
		beats:           make(map[string]*heartbeat),
	}
}

// Register registers ins with the agent, replacing any instance with the
// same Name and ID, and starts a heartbeat that keeps its TTL check passing
// until Deregister. When a heartbeat is rejected, for example because the
// agent restarted and lost the service, the instance is registered again.
func (r *Registry) Register(ctx context.Context, ins *discovery.Instance) error {
	if err := validate(ins); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	reg, err := r.registration(ins)
	if err != nil {
		return err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	// Stop the heartbeat of a previous registration so that it cannot
	// register the old definition again concurrently.
	old, replacing := r.beats[reg.ID]
	if replacing {
		old.stop()
	}
	if err := r.register(ctx, reg); err != nil {
		if replacing {
			// Keep the previous registration alive.
			r.start(old.reg)
		}
		return backendErr(fmt.Sprintf("register %q", reg.ID), err)
	}
	r.start(reg)
	return nil
}

// start records and runs a heartbeat for reg.
func (r *Registry) start(reg *api.AgentServiceRegistration) {
	ctx, cancel := context.WithCancel(context.Background())
	hb := &heartbeat{cancel: cancel, done: make(chan struct{}), reg: reg}
	r.beats[reg.ID] = hb
	go hb.run(ctx, r)
}

// Deregister stops the heartbeat of the instance with the Name and ID of
// ins and removes it from the agent. Removing an unknown instance is not an
// error.
func (r *Registry) Deregister(ctx context.Context, ins *discovery.Instance) error {
	if err := validate(ins); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	id := serviceID(ins)

	r.mu.Lock()
	defer r.mu.Unlock()
	if hb, ok := r.beats[id]; ok {
		delete(r.beats, id)
		hb.stop()
	}
	err := r.client.Agent().ServiceDeregisterOpts(id, (&api.QueryOptions{}).WithContext(ctx))
	if se, ok := errors.AsType[api.StatusError](err); ok && se.Code == http.StatusNotFound {
		return nil
	}
	if err != nil {
		return backendErr(fmt.Sprintf("deregister %q", id), err)
	}
	return nil
}

// registration converts ins into an agent service registration.
func (r *Registry) registration(ins *discovery.Instance) (*api.AgentServiceRegistration, error) {
	id := serviceID(ins)
	tags := []string{tagID + ins.ID}
	if ins.Version != "" {
		tags = append(tags, tagVersion+ins.Version)
	}
	var (
		address string
		port    int
	)
	for i, ep := range ins.Endpoints {
		u, err := url.Parse(ep)
		if err != nil || u.Scheme == "" || u.Host == "" {
			return nil, errorx.Newf(errorx.InvalidArgument, reasonInvalid, "consul: instance %q has an invalid endpoint %q", ins.ID, ep)
		}
		if i == 0 {
			address = u.Hostname()
			if p := u.Port(); p != "" {
				if port, err = strconv.Atoi(p); err != nil {
					return nil, errorx.Newf(errorx.InvalidArgument, reasonInvalid, "consul: instance %q has an invalid endpoint %q", ins.ID, ep)
				}
			}
		}
		tags = append(tags, tagEndpoint+ep)
	}
	var weights *api.AgentWeights
	if ins.Weight > 0 {
		weights = &api.AgentWeights{Passing: ins.Weight, Warning: 1}
	}
	return &api.AgentServiceRegistration{
		ID:      id,
		Name:    ins.Name,
		Tags:    tags,
		Meta:    maps.Clone(ins.Metadata),
		Address: address,
		Port:    port,
		Weights: weights,
		Check: &api.AgentServiceCheck{
			CheckID:                        checkID(id),
			Name:                           "servex TTL",
			TTL:                            r.ttl.String(),
			Status:                         api.HealthPassing,
			DeregisterCriticalServiceAfter: r.deregisterAfter.String(),
		},
	}, nil
}

// register registers reg with the agent, replacing its existing checks.
func (r *Registry) register(ctx context.Context, reg *api.AgentServiceRegistration) error {
	return r.client.Agent().ServiceRegisterOpts(reg, api.ServiceRegisterOpts{ReplaceExistingChecks: true}.WithContext(ctx))
}

// heartbeat keeps the TTL check of one registered instance passing.
type heartbeat struct {
	cancel context.CancelFunc
	done   chan struct{}
	reg    *api.AgentServiceRegistration
}

// run reports the check of the registration as passing every TTL/3 until
// ctx is done. A failed report registers it again so that an agent that
// lost the service, for example after a restart, learns it back.
func (hb *heartbeat) run(ctx context.Context, r *Registry) {
	defer close(hb.done)
	reg := hb.reg
	ticker := time.NewTicker(max(r.ttl/3, time.Millisecond))
	defer ticker.Stop()
	agent := r.client.Agent()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		err := agent.UpdateTTLOpts(reg.Check.CheckID, "", api.HealthPassing, (&api.QueryOptions{}).WithContext(ctx))
		if err != nil && ctx.Err() == nil {
			// Registering again also resets the check to passing; when the
			// agent is unreachable this fails too and the next tick retries.
			_ = r.register(ctx, reg)
		}
	}
}

// stop stops the heartbeat and waits for it to exit.
func (hb *heartbeat) stop() {
	hb.cancel()
	<-hb.done
}

// Watch returns a Watcher for the service name. It reads the passing
// instances bounded by ctx and then follows changes with blocking health
// queries until the Watcher is stopped.
func (r *Registry) Watch(ctx context.Context, name string) (discovery.Watcher, error) {
	if name == "" {
		return nil, errorx.New(errorx.InvalidArgument, reasonInvalid, "consul: empty service name")
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	entries, meta, err := r.client.Health().Service(name, "", true, (&api.QueryOptions{}).WithContext(ctx))
	if err != nil {
		return nil, backendErr(fmt.Sprintf("watch %q", name), err)
	}
	st := newState()
	st.set(instances(name, entries))

	bg, cancel := context.WithCancel(context.WithoutCancel(ctx))
	w := newWatcher(st, cancel)
	go func() {
		defer close(w.done)
		r.follow(bg, name, meta.LastIndex, st)
	}()
	return w, nil
}

// follow runs blocking health queries starting after index and publishes
// their results to st until ctx is done. Failed queries are retried with
// an exponential backoff.
func (r *Registry) follow(ctx context.Context, name string, index uint64, st *state) {
	delay := minRetry
	for {
		q := (&api.QueryOptions{WaitIndex: index, WaitTime: blockingWait}).WithContext(ctx)
		entries, meta, err := r.client.Health().Service(name, "", true, q)
		if err != nil {
			if !sleep(ctx, delay) {
				return
			}
			delay = min(2*delay, maxRetry)
			continue
		}
		delay = minRetry
		// An index that goes backwards means the agent's state was reset:
		// read again without blocking. Otherwise never block on index 0.
		if meta.LastIndex < index {
			index = 0
		} else {
			index = max(meta.LastIndex, 1)
		}
		st.set(instances(name, entries))
	}
}

// instances converts health entries of service name into instances.
func instances(name string, entries []*api.ServiceEntry) []*discovery.Instance {
	byID := make(map[string]*discovery.Instance, len(entries))
	for _, e := range entries {
		if e == nil || e.Service == nil {
			continue
		}
		// Watch reads passing instances only, so the passing weight applies.
		ins := &discovery.Instance{ID: e.Service.ID, Name: name, Weight: e.Service.Weights.Passing}
		if len(e.Service.Meta) > 0 {
			ins.Metadata = maps.Clone(e.Service.Meta)
		}
		for _, tag := range e.Service.Tags {
			if v, ok := strings.CutPrefix(tag, tagID); ok {
				ins.ID = v
			} else if v, ok := strings.CutPrefix(tag, tagVersion); ok {
				ins.Version = v
			} else if v, ok := strings.CutPrefix(tag, tagEndpoint); ok {
				ins.Endpoints = append(ins.Endpoints, v)
			}
		}
		byID[ins.ID] = ins
	}
	out := make([]*discovery.Instance, 0, len(byID))
	for _, ins := range byID {
		out = append(out, ins)
	}
	return out
}

// serviceIDEscaper keeps the delimiter distinct from component contents.
var serviceIDEscaper = strings.NewReplacer("%", "%25", "-", "%2D")

// serviceID returns the unambiguous agent service ID of ins.
func serviceID(ins *discovery.Instance) string {
	return serviceIDEscaper.Replace(ins.Name) + "-" + serviceIDEscaper.Replace(ins.ID)
}

// checkID returns the ID of the TTL check of service id.
func checkID(id string) string {
	return "service:" + id
}

// validate checks that ins identifies an instance.
func validate(ins *discovery.Instance) error {
	switch {
	case ins == nil:
		return errorx.New(errorx.InvalidArgument, reasonInvalid, "consul: nil instance")
	case ins.Name == "":
		return errorx.Newf(errorx.InvalidArgument, reasonInvalid, "consul: instance %q has an empty name", ins.ID)
	case ins.ID == "":
		return errorx.Newf(errorx.InvalidArgument, reasonInvalid, "consul: instance of %q has an empty id", ins.Name)
	case strings.Contains(ins.Name+ins.ID, "/"):
		return errorx.Newf(errorx.InvalidArgument, reasonInvalid, "consul: instance %q of %q contains a slash", ins.ID, ins.Name)
	}
	return nil
}

// backendErr classifies a failed Consul call. Context errors are kept as
// they are; requests rejected as invalid or forbidden map to
// InvalidArgument and PermissionDenied; everything else, including
// connection failures, is reported as a retryable errorx.Unavailable. The
// original error is always kept as the cause.
func backendErr(op string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("consul: %s: %w", op, err)
	}
	if se, ok := errors.AsType[api.StatusError](err); ok {
		switch se.Code {
		case http.StatusBadRequest:
			return errorx.New(errorx.InvalidArgument, reasonRejected, "consul: "+op).WithCause(err)
		case http.StatusUnauthorized, http.StatusForbidden:
			return errorx.New(errorx.PermissionDenied, reasonForbidden, "consul: "+op).WithCause(err)
		}
	}
	return errorx.New(errorx.Unavailable, reasonUnavailable, "consul: "+op).WithCause(err)
}
