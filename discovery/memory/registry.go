package memory

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"

	"github.com/nostosx/servex/discovery"
)

// errStopped is returned by Watcher.Next after Stop.
var errStopped = errors.New("memory: watcher stopped")

// Compile-time interface checks.
var (
	_ discovery.Registrar  = (*Registry)(nil)
	_ discovery.Discoverer = (*Registry)(nil)
)

// Registry is an in-memory service registry. Instances are keyed by service
// name and ID. The zero value is not usable; call New. All methods are safe
// for concurrent use.
type Registry struct {
	mu       sync.Mutex
	services map[string]*service
}

// service holds the instances of one service name.
type service struct {
	instances map[string]*discovery.Instance
	// version increases on every change.
	version uint64
	// changed is closed and replaced on every change.
	changed chan struct{}
}

// New returns an empty Registry.
func New() *Registry {
	return &Registry{services: make(map[string]*service)}
}

// Register stores a copy of ins under its Name and ID, replacing any
// instance with the same Name and ID, and notifies watchers of the service.
func (r *Registry) Register(ctx context.Context, ins *discovery.Instance) error {
	if err := validate(ins); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s := r.service(ins.Name)
	s.instances[ins.ID] = clone(ins)
	s.notify()
	return nil
}

// Deregister removes the instance with the Name and ID of ins and notifies
// watchers of the service. Removing an unknown instance is not an error.
func (r *Registry) Deregister(ctx context.Context, ins *discovery.Instance) error {
	if err := validate(ins); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	s, ok := r.services[ins.Name]
	if !ok {
		return nil
	}
	if _, ok := s.instances[ins.ID]; !ok {
		return nil
	}
	delete(s.instances, ins.ID)
	s.notify()
	return nil
}

// Watch returns a Watcher for the service name. ctx only bounds the Watch
// call itself; the Watcher lives until its Stop method is called.
func (r *Registry) Watch(ctx context.Context, name string) (discovery.Watcher, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	r.mu.Lock()
	s := r.service(name)
	r.mu.Unlock()
	return &watcher{r: r, s: s, stop: make(chan struct{})}, nil
}

// service returns the entry for name, creating it if needed. Entries are
// never removed so that watchers keep observing the same entry.
// The caller must hold r.mu.
func (r *Registry) service(name string) *service {
	s, ok := r.services[name]
	if !ok {
		s = &service{
			instances: make(map[string]*discovery.Instance),
			changed:   make(chan struct{}),
		}
		r.services[name] = s
	}
	return s
}

// notify records a change and wakes all waiters. The caller must hold r.mu.
func (s *service) notify() {
	s.version++
	close(s.changed)
	s.changed = make(chan struct{})
}

// snapshot returns deep copies of all instances ordered by ID.
// The caller must hold r.mu.
func (s *service) snapshot() []*discovery.Instance {
	out := make([]*discovery.Instance, 0, len(s.instances))
	for _, ins := range s.instances {
		out = append(out, clone(ins))
	}
	slices.SortFunc(out, func(a, b *discovery.Instance) int { return cmp.Compare(a.ID, b.ID) })
	return out
}

// watcher implements discovery.Watcher for a Registry.
type watcher struct {
	r *Registry
	s *service

	mu        sync.Mutex // serializes Next calls and guards seen/delivered
	seen      uint64
	delivered bool

	stop     chan struct{}
	stopOnce sync.Once
}

// Next returns the current snapshot on the first call and blocks until the
// service changes on later calls. It returns an error after Stop.
func (w *watcher) Next() ([]*discovery.Instance, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for {
		select {
		case <-w.stop:
			return nil, errStopped
		default:
		}

		w.r.mu.Lock()
		if !w.delivered || w.s.version != w.seen {
			snap := w.s.snapshot()
			w.seen = w.s.version
			w.delivered = true
			w.r.mu.Unlock()
			return snap, nil
		}
		changed := w.s.changed
		w.r.mu.Unlock()

		select {
		case <-changed:
		case <-w.stop:
			return nil, errStopped
		}
	}
}

// Stop stops the watcher and unblocks a pending Next. It is idempotent.
func (w *watcher) Stop() error {
	w.stopOnce.Do(func() { close(w.stop) })
	return nil
}

// validate checks that ins identifies an instance.
func validate(ins *discovery.Instance) error {
	switch {
	case ins == nil:
		return errors.New("memory: nil instance")
	case ins.Name == "":
		return fmt.Errorf("memory: instance %q has an empty name", ins.ID)
	case ins.ID == "":
		return fmt.Errorf("memory: instance of %q has an empty id", ins.Name)
	}
	return nil
}

// clone returns a deep copy of ins.
func clone(ins *discovery.Instance) *discovery.Instance {
	return &discovery.Instance{
		ID:        ins.ID,
		Name:      ins.Name,
		Version:   ins.Version,
		Weight:    ins.Weight,
		Metadata:  maps.Clone(ins.Metadata),
		Endpoints: slices.Clone(ins.Endpoints),
	}
}
