package kubernetes

import (
	"cmp"
	"context"
	"errors"
	"maps"
	"slices"
	"sync"

	"github.com/nostosx/servex/discovery"
)

// errStopped is returned by Watcher.Next after Stop.
var errStopped = errors.New("kubernetes: watcher stopped")

// state holds the latest instance list of one watched service and wakes
// waiting watchers whenever the list changes.
type state struct {
	mu sync.Mutex
	// instances is sorted by ID. It is replaced, never mutated in place.
	instances []*discovery.Instance
	// version increases on every change.
	version uint64
	// changed is closed and replaced on every change.
	changed chan struct{}
}

// newState returns an empty state.
func newState() *state {
	return &state{changed: make(chan struct{})}
}

// set replaces the instance list and wakes waiters when it differs from the
// current one. set takes ownership of list.
func (s *state) set(list []*discovery.Instance) {
	slices.SortFunc(list, func(a, b *discovery.Instance) int { return cmp.Compare(a.ID, b.ID) })
	s.mu.Lock()
	defer s.mu.Unlock()
	if slices.EqualFunc(s.instances, list, equalInstance) {
		return
	}
	s.instances = list
	s.version++
	close(s.changed)
	s.changed = make(chan struct{})
}

// watcher implements discovery.Watcher on top of a state that a background
// goroutine keeps up to date.
type watcher struct {
	st *state

	mu        sync.Mutex // serializes Next calls and guards seen/delivered
	seen      uint64
	delivered bool

	stop     chan struct{}
	stopOnce sync.Once
	// cancel stops the background goroutine, which closes done on exit.
	cancel context.CancelFunc
	done   chan struct{}
}

// newWatcher returns a watcher for st whose background goroutine is
// stopped by cancel and closes the returned watcher's done channel.
func newWatcher(st *state, cancel context.CancelFunc) *watcher {
	return &watcher{st: st, stop: make(chan struct{}), cancel: cancel, done: make(chan struct{})}
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

		w.st.mu.Lock()
		if !w.delivered || w.st.version != w.seen {
			snap := make([]*discovery.Instance, 0, len(w.st.instances))
			for _, ins := range w.st.instances {
				snap = append(snap, clone(ins))
			}
			w.seen = w.st.version
			w.delivered = true
			w.st.mu.Unlock()
			return snap, nil
		}
		changed := w.st.changed
		w.st.mu.Unlock()

		select {
		case <-changed:
		case <-w.stop:
			return nil, errStopped
		}
	}
}

// Stop stops the watcher, unblocks a pending Next and waits for the
// background goroutine to exit. It is idempotent.
func (w *watcher) Stop() error {
	w.stopOnce.Do(func() {
		close(w.stop)
		w.cancel()
		<-w.done
	})
	return nil
}

// equalInstance reports whether a and b describe the same instance.
func equalInstance(a, b *discovery.Instance) bool {
	return a.ID == b.ID && a.Name == b.Name && a.Version == b.Version &&
		maps.Equal(a.Metadata, b.Metadata) && slices.Equal(a.Endpoints, b.Endpoints)
}

// clone returns a deep copy of ins.
func clone(ins *discovery.Instance) *discovery.Instance {
	return &discovery.Instance{
		ID:        ins.ID,
		Name:      ins.Name,
		Version:   ins.Version,
		Metadata:  maps.Clone(ins.Metadata),
		Endpoints: slices.Clone(ins.Endpoints),
	}
}
