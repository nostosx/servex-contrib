package nacos

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/naming_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

var _ naming_client.INamingClient = (*fakeNaming)(nil)

// errFake is returned by fakeNaming while it fails.
var errFake = errors.New("fake nacos: server unavailable")

// fakeNaming emulates a Nacos 2 naming client connected to a server: every
// client publishes at most one set of instances per service, replaced by
// RegisterInstance and BatchRegisterInstance and removed by
// DeregisterInstance, and subscribers are notified of the full host list
// of a service after every change.
type fakeNaming struct {
	// fail makes every call fail with errFake.
	fail atomic.Bool

	mu        sync.Mutex
	published map[string][]model.Instance // by group@@service
	subs      map[string][]*func([]model.Instance, error)
	calls     map[string]int // by method name
	// notifyErr makes notifications carry an error instead of hosts.
	notifyErr bool
}

func newFakeNaming() *fakeNaming {
	return &fakeNaming{
		published: make(map[string][]model.Instance),
		subs:      make(map[string][]*func([]model.Instance, error)),
		calls:     make(map[string]int),
	}
}

func serviceKey(group, name string) string {
	if group == "" {
		group = defaultGroup
	}
	return group + "@@" + name
}

// call records a call and reports the injected failure, if any. The caller
// must hold f.mu.
func (f *fakeNaming) call(method string) error {
	f.calls[method]++
	if f.fail.Load() {
		return errFake
	}
	return nil
}

// count returns how often method was called.
func (f *fakeNaming) count(method string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls[method]
}

// subscribers returns the number of subscriptions of a service.
func (f *fakeNaming) subscribers(group, name string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.subs[serviceKey(group, name)])
}

// hosts returns a copy of the hosts of a service.
func (f *fakeNaming) hosts(key string) []model.Instance {
	out := slices.Clone(f.published[key])
	for i := range out {
		out[i].Metadata = maps.Clone(out[i].Metadata)
	}
	return out
}

// change replaces the published hosts of a service and notifies its
// subscribers outside of the lock, as the client does from its own
// goroutine. The caller must hold f.mu; change releases it.
func (f *fakeNaming) change(key string, hosts []model.Instance) {
	if hosts == nil {
		delete(f.published, key)
	} else {
		f.published[key] = hosts
	}
	subs := slices.Clone(f.subs[key])
	list, notifyErr := f.hosts(key), f.notifyErr
	f.mu.Unlock()
	for _, cb := range subs {
		if notifyErr {
			(*cb)(nil, errFake)
		} else {
			(*cb)(list, nil)
		}
	}
	f.mu.Lock()
}

// update mutates a published host and notifies subscribers.
func (f *fakeNaming) update(group, name string, mutate func(*model.Instance)) {
	f.mu.Lock()
	defer f.mu.Unlock()
	key := serviceKey(group, name)
	hosts := f.hosts(key)
	for i := range hosts {
		mutate(&hosts[i])
	}
	f.change(key, hosts)
}

func toModel(p vo.RegisterInstanceParam, group, name string) model.Instance {
	return model.Instance{
		InstanceId:  fmt.Sprintf("%s#%d#%s#%s", p.Ip, p.Port, p.ClusterName, serviceKey(group, name)),
		Ip:          p.Ip,
		Port:        p.Port,
		Weight:      p.Weight,
		Healthy:     p.Healthy,
		Enable:      p.Enable,
		Ephemeral:   p.Ephemeral,
		ClusterName: p.ClusterName,
		ServiceName: serviceKey(group, name),
		Metadata:    maps.Clone(p.Metadata),
	}
}

func (f *fakeNaming) RegisterInstance(p vo.RegisterInstanceParam) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.call("RegisterInstance"); err != nil {
		return false, err
	}
	if p.ServiceName == "" {
		return false, errors.New("serviceName cannot be empty!")
	}
	f.change(serviceKey(p.GroupName, p.ServiceName), []model.Instance{toModel(p, p.GroupName, p.ServiceName)})
	return true, nil
}

func (f *fakeNaming) BatchRegisterInstance(p vo.BatchRegisterInstanceParam) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.call("BatchRegisterInstance"); err != nil {
		return false, err
	}
	if p.ServiceName == "" || len(p.Instances) == 0 {
		return false, errors.New("serviceName and instances cannot be empty!")
	}
	hosts := make([]model.Instance, 0, len(p.Instances))
	for _, in := range p.Instances {
		if !in.Ephemeral {
			return false, errors.New("Batch registration does not allow persistent instance registration!")
		}
		hosts = append(hosts, toModel(in, p.GroupName, p.ServiceName))
	}
	f.change(serviceKey(p.GroupName, p.ServiceName), hosts)
	return true, nil
}

func (f *fakeNaming) DeregisterInstance(p vo.DeregisterInstanceParam) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.call("DeregisterInstance"); err != nil {
		return false, err
	}
	key := serviceKey(p.GroupName, p.ServiceName)
	if _, ok := f.published[key]; ok {
		f.change(key, nil)
	}
	return true, nil
}

func (f *fakeNaming) UpdateInstance(vo.UpdateInstanceParam) (bool, error) {
	return false, errors.New("fake nacos: UpdateInstance not implemented")
}

func (f *fakeNaming) GetService(vo.GetServiceParam) (model.Service, error) {
	return model.Service{}, errors.New("fake nacos: GetService not implemented")
}

func (f *fakeNaming) SelectAllInstances(p vo.SelectAllInstancesParam) ([]model.Instance, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.call("SelectAllInstances"); err != nil {
		return nil, err
	}
	var out []model.Instance
	for _, h := range f.hosts(serviceKey(p.GroupName, p.ServiceName)) {
		if len(p.Clusters) == 0 || slices.Contains(p.Clusters, h.ClusterName) {
			out = append(out, h)
		}
	}
	return out, nil
}

func (f *fakeNaming) SelectInstances(vo.SelectInstancesParam) ([]model.Instance, error) {
	return nil, errors.New("fake nacos: SelectInstances not implemented")
}

func (f *fakeNaming) SelectOneHealthyInstance(vo.SelectOneHealthInstanceParam) (*model.Instance, error) {
	return nil, errors.New("fake nacos: SelectOneHealthyInstance not implemented")
}

func (f *fakeNaming) Subscribe(p *vo.SubscribeParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.call("Subscribe"); err != nil {
		return err
	}
	key := serviceKey(p.GroupName, p.ServiceName)
	f.subs[key] = append(f.subs[key], &p.SubscribeCallback)
	return nil
}

func (f *fakeNaming) Unsubscribe(p *vo.SubscribeParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.call("Unsubscribe"); err != nil {
		return err
	}
	key := serviceKey(p.GroupName, p.ServiceName)
	// Like the real client, callbacks are identified by the address of the
	// SubscribeCallback field.
	f.subs[key] = slices.DeleteFunc(f.subs[key], func(cb *func([]model.Instance, error)) bool {
		return cb == &p.SubscribeCallback
	})
	return nil
}

func (f *fakeNaming) GetAllServicesInfo(vo.GetAllServiceInfoParam) (model.ServiceList, error) {
	return model.ServiceList{}, errors.New("fake nacos: GetAllServicesInfo not implemented")
}

func (f *fakeNaming) ServerHealthy() bool { return !f.fail.Load() }

func (f *fakeNaming) CloseClient() {}
