package nacos_test

import (
	"errors"
	"sync"

	"github.com/nacos-group/nacos-sdk-go/v2/clients/config_client"
	"github.com/nacos-group/nacos-sdk-go/v2/model"
	"github.com/nacos-group/nacos-sdk-go/v2/vo"
)

// fakeClient is an in-memory config_client.IConfigClient. Like the real
// client it keeps one listener per data ID and group (a second
// ListenConfig for the same item keeps the first listener), cancels it as a
// whole and calls listeners on their own goroutine when an item changes.
type fakeClient struct {
	mu        sync.Mutex
	items     map[string]string
	listeners map[string]func(namespace, group, dataID, data string)
	getErr    error
	listenErr error
	gets      []vo.ConfigParam
	listens   int
	cancels   int
	callbacks sync.WaitGroup
}

var _ config_client.IConfigClient = (*fakeClient)(nil)

func newFakeClient() *fakeClient {
	return &fakeClient{items: map[string]string{}, listeners: map[string]func(string, string, string, string){}}
}

func itemKey(dataID, group string) string { return group + "/" + dataID }

// publish stores content and calls the item's listener, if any.
func (f *fakeClient) publish(dataID, group, content string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items[itemKey(dataID, group)] = content
	if l := f.listeners[itemKey(dataID, group)]; l != nil {
		f.callbacks.Go(func() { l("public", group, dataID, content) })
	}
}

// listening reports whether a listener is registered for the item.
func (f *fakeClient) listening(dataID, group string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listeners[itemKey(dataID, group)] != nil
}

// counts returns the number of ListenConfig and CancelListenConfig calls.
func (f *fakeClient) counts() (listens, cancels int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.listens, f.cancels
}

func (f *fakeClient) GetConfig(p vo.ConfigParam) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.gets = append(f.gets, p)
	if f.getErr != nil {
		return "", f.getErr
	}
	return f.items[itemKey(p.DataId, p.Group)], nil
}

func (f *fakeClient) PublishConfig(p vo.ConfigParam) (bool, error) {
	f.publish(p.DataId, p.Group, p.Content)
	return true, nil
}

func (f *fakeClient) DeleteConfig(p vo.ConfigParam) (bool, error) {
	f.publish(p.DataId, p.Group, "")
	return true, nil
}

func (f *fakeClient) ListenConfig(p vo.ConfigParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.listens++
	if f.listenErr != nil {
		return f.listenErr
	}
	if p.DataId == "" || p.Group == "" || p.OnChange == nil {
		return errors.New("fake: invalid listen parameters")
	}
	if _, ok := f.listeners[itemKey(p.DataId, p.Group)]; !ok {
		f.listeners[itemKey(p.DataId, p.Group)] = p.OnChange
	}
	return nil
}

func (f *fakeClient) CancelListenConfig(p vo.ConfigParam) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cancels++
	delete(f.listeners, itemKey(p.DataId, p.Group))
	return nil
}

func (f *fakeClient) SearchConfig(vo.SearchConfigParam) (*model.ConfigPage, error) {
	return nil, errors.New("fake: search unsupported")
}

func (f *fakeClient) CloseClient() {}
