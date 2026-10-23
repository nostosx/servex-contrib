package kubernetes

import (
	"context"
	"errors"
	"net/url"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	corev1 "k8s.io/api/core/v1"
	discoveryv1 "k8s.io/api/discovery/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	k8stesting "k8s.io/client-go/testing"

	"github.com/nostosx/servex/discovery"
	"github.com/nostosx/servex/errorx"
)

const testNamespace = "apps"

// sliceRegistrar plays the role of the endpoint slice controller in tests:
// it publishes every registered instance as an EndpointSlice of its own,
// with one named port per endpoint.
type sliceRegistrar struct {
	client    kubernetes.Interface
	namespace string
}

var _ discovery.Registrar = sliceRegistrar{}

func sliceName(ins *discovery.Instance) string { return ins.Name + "-" + ins.ID }

func (s sliceRegistrar) Register(ctx context.Context, ins *discovery.Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	slice := &discoveryv1.EndpointSlice{
		Name:      sliceName(ins),
		Namespace: s.namespace,
		Labels: map[string]string{
			discoveryv1.LabelServiceName: ins.Name,
			discoveryv1.LabelManagedBy:   "servex-test",
		},
		AddressType: discoveryv1.AddressTypeIPv4,
	}
	ep := discoveryv1.Endpoint{
		Conditions: discoveryv1.EndpointConditions{Ready: new(true)},
		TargetRef:  &corev1.ObjectReference{Kind: "Pod", Namespace: s.namespace, Name: ins.ID},
	}
	for _, raw := range ins.Endpoints {
		u, err := url.Parse(raw)
		if err != nil {
			return err
		}
		port, err := strconv.Atoi(u.Port())
		if err != nil {
			return err
		}
		ep.Addresses = []string{u.Hostname()}
		slice.Ports = append(slice.Ports, discoveryv1.EndpointPort{Name: new(u.Scheme), Port: new(int32(port))})
	}
	slice.Endpoints = []discoveryv1.Endpoint{ep}
	api := s.client.DiscoveryV1().EndpointSlices(s.namespace)
	_, err := api.Create(ctx, slice, metav1.CreateOptions{})
	if apierrors.IsAlreadyExists(err) {
		current, getErr := api.Get(ctx, slice.Name, metav1.GetOptions{})
		if getErr != nil {
			return getErr
		}
		slice.ResourceVersion = current.ResourceVersion
		_, err = api.Update(ctx, slice, metav1.UpdateOptions{})
	}
	return err
}

func (s sliceRegistrar) Deregister(ctx context.Context, ins *discovery.Instance) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	err := s.client.DiscoveryV1().EndpointSlices(s.namespace).Delete(ctx, sliceName(ins), metav1.DeleteOptions{})
	if apierrors.IsNotFound(err) {
		return nil
	}
	return err
}

func TestContract(t *testing.T) {
	runContract(t, func(t *testing.T) contractEnv {
		client := fake.NewClientset()
		return contractEnv{reg: sliceRegistrar{client, testNamespace}, dis: New(client, testNamespace)}
	})
}

func TestEndToEnd(t *testing.T) {
	client := fake.NewClientset()
	runEndToEnd(t, sliceRegistrar{client, testNamespace}, New(client, testNamespace))
}

func TestValidation(t *testing.T) {
	d := New(fake.NewClientset(), "")
	if d.namespace != metav1.NamespaceDefault {
		t.Errorf("namespace = %q, want default", d.namespace)
	}
	if _, err := d.Watch(t.Context(), ""); errorx.CodeOf(err) != errorx.InvalidArgument {
		t.Errorf("Watch(\"\") error = %v, want InvalidArgument", err)
	}
}

func TestEndpointSliceMapping(t *testing.T) {
	ready, notReady := new(true), new(false)
	slice := func(name, service string, addrType discoveryv1.AddressType, ports []discoveryv1.EndpointPort, eps ...discoveryv1.Endpoint) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{
			Name:        name,
			Namespace:   testNamespace,
			Labels:      map[string]string{discoveryv1.LabelServiceName: service},
			AddressType: addrType,
			Ports:       ports,
			Endpoints:   eps,
		}
	}
	pod := func(name string) *corev1.ObjectReference {
		return &corev1.ObjectReference{Kind: "Pod", Namespace: testNamespace, Name: name}
	}
	ports := []discoveryv1.EndpointPort{
		{Name: new("http"), Port: new(int32(8000))},
		{Name: new("grpc"), Port: new(int32(9000))},
		{Name: new(""), Port: new(int32(1))}, // unnamed ports are ignored
		{Name: new("metrics")},               // so are ports without a number
	}
	client := fake.NewClientset(
		slice("user-v4", "user", discoveryv1.AddressTypeIPv4, ports,
			discoveryv1.Endpoint{Addresses: []string{"10.0.0.1", "10.0.0.9"}, Conditions: discoveryv1.EndpointConditions{Ready: ready},
				TargetRef: pod("user-a"), Zone: new("z1"), NodeName: new("n1")},
			// Unknown readiness counts as ready.
			discoveryv1.Endpoint{Addresses: []string{"10.0.0.2"}, TargetRef: pod("user-b")},
			discoveryv1.Endpoint{Addresses: []string{"10.0.0.3"}, Conditions: discoveryv1.EndpointConditions{Ready: notReady}, TargetRef: pod("user-c")},
			// Endpoints without a target are identified by their address.
			discoveryv1.Endpoint{Addresses: []string{"10.0.0.4"}, Conditions: discoveryv1.EndpointConditions{Ready: ready}},
			discoveryv1.Endpoint{Conditions: discoveryv1.EndpointConditions{Ready: ready}, TargetRef: pod("no-address")},
		),
		// The same Pod in an IPv6 slice is merged into one instance.
		slice("user-v6", "user", discoveryv1.AddressTypeIPv6, ports[:1],
			discoveryv1.Endpoint{Addresses: []string{"fd00::1"}, Conditions: discoveryv1.EndpointConditions{Ready: ready}, TargetRef: pod("user-a")},
		),
		slice("order-v4", "order", discoveryv1.AddressTypeIPv4, ports,
			discoveryv1.Endpoint{Addresses: []string{"10.0.1.1"}, Conditions: discoveryv1.EndpointConditions{Ready: ready}, TargetRef: pod("order-a")},
		),
	)
	// Slices of other namespaces are not watched.
	if _, err := client.DiscoveryV1().EndpointSlices("other").Create(t.Context(),
		&discoveryv1.EndpointSlice{
			Name: "user-other", Namespace: "other", Labels: map[string]string{discoveryv1.LabelServiceName: "user"},
			AddressType: discoveryv1.AddressTypeIPv4,
			Endpoints:   []discoveryv1.Endpoint{{Addresses: []string{"10.9.9.9"}, TargetRef: pod("user-z")}},
		}, metav1.CreateOptions{}); err != nil {
		t.Fatal(err)
	}

	d := New(client, testNamespace)
	w, list := mustWatch(t, d, "user")
	want := []*discovery.Instance{
		{ID: "10.0.0.4", Name: "user", Endpoints: []string{"grpc://10.0.0.4:9000", "http://10.0.0.4:8000"}},
		{ID: "user-a", Name: "user", Metadata: map[string]string{MetadataZone: "z1", MetadataNode: "n1"},
			Endpoints: []string{"grpc://10.0.0.1:9000", "http://10.0.0.1:8000", "http://[fd00::1]:8000"}},
		{ID: "user-b", Name: "user", Endpoints: []string{"grpc://10.0.0.2:9000", "http://10.0.0.2:8000"}},
	}
	if !slices.EqualFunc(list, want, equalInstance) {
		t.Fatalf("snapshot =\n%v\nwant\n%v", dump(list), dump(want))
	}

	// Readiness changes are followed.
	s, err := client.DiscoveryV1().EndpointSlices(testNamespace).Get(t.Context(), "user-v4", metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	s.Endpoints[0].Conditions.Ready = notReady
	s.Endpoints[2].Conditions.Ready = ready
	if _, err := client.DiscoveryV1().EndpointSlices(testNamespace).Update(t.Context(), s, metav1.UpdateOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, w, hasIDs("10.0.0.4", "user-a", "user-b", "user-c"))
	if err := client.DiscoveryV1().EndpointSlices(testNamespace).Delete(t.Context(), "user-v6", metav1.DeleteOptions{}); err != nil {
		t.Fatal(err)
	}
	waitFor(t, w, hasIDs("10.0.0.4", "user-b", "user-c"))
}

func dump(list []*discovery.Instance) []discovery.Instance {
	out := make([]discovery.Instance, 0, len(list))
	for _, ins := range list {
		out = append(out, *ins)
	}
	return out
}

func TestWatchWaitsForInitialList(t *testing.T) {
	client := fake.NewClientset()
	boom := errors.New("apiserver unavailable")
	client.PrependReactor("list", "endpointslices", func(k8stesting.Action) (bool, runtime.Object, error) {
		return true, nil, boom
	})
	d := New(client, testNamespace)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	_, err := d.Watch(ctx, "user")
	if e, ok := errors.AsType[*errorx.Error](err); !ok || e.Code != errorx.Unavailable || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Watch error = %v, want Unavailable caused by the deadline and the list failure", err)
	}

	// Without a recorded failure the context error is returned as it is.
	slow := fake.NewClientset()
	block := make(chan struct{})
	defer close(block)
	slow.PrependReactor("list", "endpointslices", func(k8stesting.Action) (bool, runtime.Object, error) {
		<-block
		return true, nil, boom
	})
	ctx, cancel = context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err := New(slow, testNamespace).Watch(ctx, "user"); !errors.Is(err, context.DeadlineExceeded) || errorx.CodeOf(err) != errorx.DeadlineExceeded {
		t.Fatalf("Watch error = %v, want context.DeadlineExceeded", err)
	}
}

// TestRefreshIsAtomic interleaves two refreshes: the first lists an older
// store and stalls before publishing while the store changes and a second
// refresh runs. The newer snapshot must win.
func TestRefreshIsAtomic(t *testing.T) {
	slice := func(addr string) *discoveryv1.EndpointSlice {
		return &discoveryv1.EndpointSlice{
			Name:      "svc-" + addr,
			Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
			Endpoints: []discoveryv1.Endpoint{{Addresses: []string{addr}}},
			Ports:     []discoveryv1.EndpointPort{{Name: new("http"), Port: new(int32(80))}},
		}
	}
	var (
		mu    sync.Mutex
		store = []any{slice("10.0.0.1")}
		calls int
	)
	listed, release := make(chan struct{}), make(chan struct{})
	secondListed := make(chan struct{})
	list := func() []any {
		mu.Lock()
		objs := slices.Clone(store)
		calls++
		n := calls
		mu.Unlock()
		switch n {
		case 1:
			close(listed)
			<-release
		case 2:
			close(secondListed)
		}
		return objs
	}
	st := newState()
	r := &refresher{name: "svc", list: list, st: st}

	first := make(chan struct{})
	go func() { defer close(first); r.refresh() }()
	<-listed

	mu.Lock()
	store = []any{slice("10.0.0.2")}
	mu.Unlock()
	second := make(chan struct{})
	go func() { defer close(second); r.refresh() }()

	// A non-atomic refresh lets the second call list and publish while the
	// first one is stalled; give it the chance before releasing the first.
	select {
	case <-secondListed:
		<-second
	case <-time.After(100 * time.Millisecond):
	}
	close(release)
	<-first
	<-second

	st.mu.Lock()
	got := st.instances
	st.mu.Unlock()
	if len(got) != 1 || got[0].ID != "10.0.0.2" {
		t.Fatalf("instances = %v, want the newer snapshot 10.0.0.2", dump(got))
	}
}

// TestConcurrentRefreshConverges stresses refreshes from many goroutines
// while the store changes; after the last change the state must match it.
func TestConcurrentRefreshConverges(t *testing.T) {
	var (
		mu    sync.Mutex
		store []any
	)
	list := func() []any {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(store)
	}
	st := newState()
	r := &refresher{name: "svc", list: list, st: st}
	var wg sync.WaitGroup
	for i := range 50 {
		addr := "10.0.1." + strconv.Itoa(i)
		wg.Go(func() {
			mu.Lock()
			store = []any{&discoveryv1.EndpointSlice{
				Labels:    map[string]string{discoveryv1.LabelServiceName: "svc"},
				Endpoints: []discoveryv1.Endpoint{{Addresses: []string{addr}}},
			}}
			mu.Unlock()
			// Like the informer, notify after the store has changed.
			r.refresh()
		})
	}
	wg.Wait()
	want := instances("svc", list())
	st.mu.Lock()
	got := st.instances
	st.mu.Unlock()
	if !slices.EqualFunc(got, want, equalInstance) {
		t.Fatalf("instances = %v, want %v", dump(got), dump(want))
	}
}

func TestSliceRegistrarUsesCurrentResourceVersion(t *testing.T) {
	ins := inst("svc", "one")
	client := fake.NewClientset(&discoveryv1.EndpointSlice{
		Name: sliceName(ins), Namespace: testNamespace, ResourceVersion: "7",
		AddressType: discoveryv1.AddressTypeIPv4,
	})
	updates := 0
	client.PrependReactor("update", "endpointslices", func(action k8stesting.Action) (bool, runtime.Object, error) {
		updates++
		slice := action.(k8stesting.UpdateAction).GetObject().(*discoveryv1.EndpointSlice)
		if slice.ResourceVersion != "7" {
			return true, nil, errors.New("update did not preserve resource version")
		}
		return false, nil, nil
	})
	if err := (sliceRegistrar{client, testNamespace}).Register(t.Context(), ins); err != nil {
		t.Fatal(err)
	}
	got, err := client.DiscoveryV1().EndpointSlices(testNamespace).Get(t.Context(), sliceName(ins), metav1.GetOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if updates != 1 || len(got.Endpoints) != 1 || got.Endpoints[0].TargetRef.Name != ins.ID {
		t.Fatalf("updates = %d, replacement = %+v", updates, got)
	}
}
