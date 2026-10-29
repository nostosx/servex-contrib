package httpx

import (
	"fmt"
	"mime/multipart"
	"reflect"
	"strings"
	"sync"
	"testing"
)

func TestBindingPlanConcurrentUse(t *testing.T) {
	var cache bindingCache
	var wg sync.WaitGroup
	for i := range 64 {
		wg.Go(func() {
			id := fmt.Sprint(i)
			file := &multipart.FileHeader{Filename: id + ".txt"}
			var dst MutualBindTarget
			if err := cache.bindValues(&dst, map[string][]string{"value": {id}, "other": {"child" + id}}); err != nil {
				t.Error(err)
				return
			}
			if err := cache.bindFiles(&dst, map[string][]*multipart.FileHeader{"file": {file}}); err != nil {
				t.Error(err)
				return
			}
			if dst.Value != id || dst.MutualBindChild == nil || dst.Other != "child"+id || dst.File != file || dst.MutualBindTarget != nil {
				t.Errorf("binding result = %+v", dst)
			}
		})
	}
	wg.Wait()
}

func TestBindingPlanWarmSemantics(t *testing.T) {
	var cache bindingCache
	for range 3 {
		var dst bindTarget
		if err := cache.bindValues(&dst, map[string][]string{
			"deep": {"nested"}, "shade": {"outer"}, "via": {"pointer"}, "secret": {"exported"},
			"unsigned": {"255"}, "ints": {"1", "-2"}, "when": {"2026-01-02T03:04:05Z"},
			"when_ptr": {"2026-01-02T03:04:05Z"}, "Skipped": {"ignored"}, "private": {"ignored"},
		}); err != nil {
			t.Fatal(err)
		}
		if dst.Deep != "nested" || dst.Inner.Shade != "" || dst.Shade != "outer" || dst.Ptr == nil || dst.Via != "pointer" || dst.Secret != "exported" || dst.Unsigned != 255 || dst.WhenPtr == nil || !dst.When.Equal(*dst.WhenPtr) || dst.Skipped != "" || dst.private != "" || !reflect.DeepEqual(dst.Ints, []int64{1, -2}) {
			t.Fatalf("bound fields = %+v", dst)
		}
		err := cache.bindValues(&bindTarget{}, map[string][]string{"unsigned": {"256"}, "f32": {"bad"}})
		if err == nil || !strings.HasPrefix(err.Error(), `field "f32":`) {
			t.Fatalf("first binding error = %v", err)
		}
	}
}

func TestBindingPlanCacheBound(t *testing.T) {
	var cache bindingCache
	for i := range maxBindingPlans + 8 {
		typ := reflect.StructOf([]reflect.StructField{{
			Name: "Value", Type: reflect.TypeFor[string](), Tag: reflect.StructTag(fmt.Sprintf(`json:"value" variant:"%d"`, i)),
		}})
		dst := reflect.New(typ)
		if err := cache.bindValues(dst.Interface(), map[string][]string{"value": {"bound"}}); err != nil {
			t.Fatal(err)
		}
		if got := dst.Elem().Field(0).String(); got != "bound" {
			t.Fatalf("type %d value = %q", i, got)
		}
	}
	if len(cache.plans) != maxBindingPlans {
		t.Fatalf("retained %d binding plans, want %d", len(cache.plans), maxBindingPlans)
	}
}
