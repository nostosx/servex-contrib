package consul

import (
	"encoding/json/v2"
	"io"
	"net/http"
	"net/http/httptest"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/consul/api"
)

// metaKey is the rule Consul applies to service meta keys.
var metaKey = regexp.MustCompile(`^[A-Za-z0-9_-]{1,128}$`)

// fakeConsul emulates the parts of the Consul HTTP API used by Registry:
// agent service registration, TTL check updates and blocking health
// queries. TTL checks that are not updated in time turn critical.
type fakeConsul struct {
	srv *httptest.Server

	// unavailable makes every request fail with 500.
	unavailable atomic.Bool

	mu       sync.Mutex
	index    uint64
	changed  chan struct{}
	services map[string]*fakeService // by service ID
	updates  map[string]int          // TTL updates by check ID
	regs     map[string]int          // registrations by service ID
}

// fakeService is a registered agent service and its TTL check.
type fakeService struct {
	reg     fakeRegistration
	status  string
	ttl     time.Duration
	updated time.Time
}

// fakeRegistration is the registration body sent by the client.
type fakeRegistration struct {
	ID      string            `json:"ID"`
	Name    string            `json:"Name"`
	Tags    []string          `json:"Tags"`
	Meta    map[string]string `json:"Meta"`
	Address string            `json:"Address"`
	Port    int               `json:"Port"`
	Weights *api.AgentWeights `json:"Weights"`
	Check   *struct {
		CheckID                        string `json:"CheckID"`
		TTL                            string `json:"TTL"`
		Status                         string `json:"Status"`
		DeregisterCriticalServiceAfter string `json:"DeregisterCriticalServiceAfter"`
	} `json:"Check"`
}

// newFakeConsul starts an emulated agent and returns a client for it.
func newFakeConsul(t *testing.T) (*fakeConsul, *api.Client) {
	t.Helper()
	f := &fakeConsul{
		index:    1,
		changed:  make(chan struct{}),
		services: make(map[string]*fakeService),
		updates:  make(map[string]int),
		regs:     make(map[string]int),
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /v1/agent/service/register", f.register)
	mux.HandleFunc("PUT /v1/agent/service/deregister/{id}", f.deregister)
	mux.HandleFunc("PUT /v1/agent/check/update/{id}", f.update)
	mux.HandleFunc("GET /v1/health/service/{name}", f.health)
	f.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if f.unavailable.Load() {
			http.Error(w, "unavailable", http.StatusInternalServerError)
			return
		}
		mux.ServeHTTP(w, r)
	}))
	stop := make(chan struct{})
	reaped := make(chan struct{})
	go f.reap(stop, reaped)
	t.Cleanup(func() {
		close(stop)
		<-reaped
		f.srv.CloseClientConnections()
		f.srv.Close()
	})
	client, err := api.NewClient(&api.Config{Address: f.srv.URL})
	if err != nil {
		t.Fatal(err)
	}
	return f, client
}

// bump records a change. The caller must hold f.mu.
func (f *fakeConsul) bump() {
	f.index++
	close(f.changed)
	f.changed = make(chan struct{})
}

// reap turns expired TTL checks critical until stop is closed.
func (f *fakeConsul) reap(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	ticker := time.NewTicker(20 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
		}
		f.mu.Lock()
		for _, s := range f.services {
			if s.status != api.HealthCritical && s.ttl > 0 && time.Since(s.updated) > s.ttl {
				s.status = api.HealthCritical
				f.bump()
			}
		}
		f.mu.Unlock()
	}
}

// reset forgets all services, as an agent restarted without state would.
func (f *fakeConsul) reset() {
	f.mu.Lock()
	defer f.mu.Unlock()
	clear(f.services)
	f.bump()
}

// counts returns the number of registrations of a service ID and TTL
// updates of its check.
func (f *fakeConsul) counts(id string) (regs, updates int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.regs[id], f.updates[checkID(id)]
}

// service returns a copy of the registration of a service ID.
func (f *fakeConsul) service(id string) (fakeRegistration, bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.services[id]
	if !ok {
		return fakeRegistration{}, false
	}
	return s.reg, true
}

func (f *fakeConsul) register(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var reg fakeRegistration
	if err := json.Unmarshal(body, &reg); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	for k := range reg.Meta {
		if !metaKey.MatchString(k) {
			http.Error(w, "Invalid Service Meta: invalid key "+strconv.Quote(k), http.StatusBadRequest)
			return
		}
	}
	s := &fakeService{reg: reg, status: api.HealthCritical, updated: time.Now()}
	if c := reg.Check; c != nil {
		if c.Status != "" {
			s.status = c.Status
		}
		if c.TTL != "" {
			if s.ttl, err = time.ParseDuration(c.TTL); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.services[reg.ID] = s
	f.regs[reg.ID]++
	f.bump()
}

func (f *fakeConsul) deregister(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := r.PathValue("id")
	if _, ok := f.services[id]; !ok {
		http.Error(w, "Unknown service ID "+strconv.Quote(id), http.StatusNotFound)
		return
	}
	delete(f.services, id)
	f.bump()
}

func (f *fakeConsul) update(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Status string `json:"Status"`
	}
	if err := json.UnmarshalRead(r.Body, &body); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	id := r.PathValue("id")
	for _, s := range f.services {
		if s.reg.Check != nil && s.reg.Check.CheckID == id {
			f.updates[id]++
			s.updated = time.Now()
			if s.status != body.Status {
				s.status = body.Status
				f.bump()
			}
			return
		}
	}
	http.Error(w, "CheckID "+strconv.Quote(id)+" does not have associated TTL", http.StatusNotFound)
}

// health serves /v1/health/service/{name}, blocking while the requested
// index is current.
func (f *fakeConsul) health(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	index, _ := strconv.ParseUint(q.Get("index"), 10, 64)
	wait := 5 * time.Minute
	if v := q.Get("wait"); v != "" {
		if d, err := time.ParseDuration(v); err == nil {
			wait = d
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()

	f.mu.Lock()
block:
	for index != 0 && index >= f.index {
		changed := f.changed
		f.mu.Unlock()
		select {
		case <-changed:
			f.mu.Lock()
		case <-timer.C:
			f.mu.Lock()
			break block
		case <-r.Context().Done():
			return
		}
	}
	passing := q.Has("passing")
	type node struct {
		Node    string `json:"Node"`
		Address string `json:"Address"`
	}
	type service struct {
		ID      string            `json:"ID"`
		Service string            `json:"Service"`
		Tags    []string          `json:"Tags"`
		Meta    map[string]string `json:"Meta"`
		Address string            `json:"Address"`
		Port    int               `json:"Port"`
		Weights api.AgentWeights  `json:"Weights"`
	}
	type entry struct {
		Node    node    `json:"Node"`
		Service service `json:"Service"`
	}
	entries := []entry{}
	name := r.PathValue("name")
	for _, s := range f.services {
		if s.reg.Name != name || passing && s.status != api.HealthPassing {
			continue
		}
		// Consul applies default weights to services registered without.
		weights := api.AgentWeights{Passing: 1, Warning: 1}
		if s.reg.Weights != nil {
			weights = *s.reg.Weights
		}
		entries = append(entries, entry{
			Node: node{Node: "fake", Address: "127.0.0.1"},
			Service: service{
				ID:      s.reg.ID,
				Service: s.reg.Name,
				Tags:    slices.Clone(s.reg.Tags),
				Meta:    s.reg.Meta,
				Address: s.reg.Address,
				Port:    s.reg.Port,
				Weights: weights,
			},
		})
	}
	current := f.index
	f.mu.Unlock()
	w.Header().Set("X-Consul-Index", strconv.FormatUint(current, 10))
	w.Header().Set("Content-Type", "application/json")
	_ = json.MarshalWrite(w, entries) // a failed write surfaces as a client decode error
}
