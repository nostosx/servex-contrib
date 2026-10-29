package memory

import (
	"testing"

	"github.com/nostosx/servex/discovery"
)

func TestWeightIsKept(t *testing.T) {
	r := New()
	ins := &discovery.Instance{ID: "a", Name: "svc", Weight: 3, Endpoints: []string{"http://10.0.0.1:80"}}
	if err := r.Register(t.Context(), ins); err != nil {
		t.Fatal(err)
	}
	ins.Weight = 9 // the registry keeps its own copy
	w, err := r.Watch(t.Context(), "svc")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = w.Stop() }()
	list, err := w.Next()
	if err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 || list[0].Weight != 3 {
		t.Errorf("instances = %+v, want one with Weight 3", list)
	}
}
