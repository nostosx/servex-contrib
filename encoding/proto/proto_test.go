package proto_test

import (
	"errors"
	"sync"
	"testing"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/nostosx/servex/errorx"

	protocodec "github.com/nostosx/servex-contrib/encoding/proto"
)

func TestName(t *testing.T) {
	if got := protocodec.Codec.Name(); got != "proto" {
		t.Fatalf("Name() = %q, want %q", got, "proto")
	}
}

func TestRoundTrip(t *testing.T) {
	in, err := structpb.NewStruct(map[string]any{"name": "gopher", "age": 13.0, "tags": []any{"a", "b"}})
	if err != nil {
		t.Fatal(err)
	}
	data, err := protocodec.Codec.Marshal(in)
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	want, err := proto.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	if len(data) != len(want) {
		t.Errorf("Marshal produced %d bytes, want %d", len(data), len(want))
	}
	out := new(structpb.Struct)
	if err := protocodec.Codec.Unmarshal(data, out); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if !proto.Equal(in, out) {
		t.Fatalf("round trip = %v, want %v", out, in)
	}
}

func TestUnmarshalResetsDestination(t *testing.T) {
	data, err := protocodec.Codec.Marshal(wrapperspb.String("new"))
	if err != nil {
		t.Fatal(err)
	}
	dst := wrapperspb.String("old")
	if err := protocodec.Codec.Unmarshal(data, dst); err != nil {
		t.Fatal(err)
	}
	if dst.GetValue() != "new" {
		t.Fatalf("value = %q, want new", dst.GetValue())
	}
}

func TestErrors(t *testing.T) {
	type plain struct{ Name string }
	tests := []struct {
		name string
		run  func() error
	}{
		{"marshal non-message", func() error { _, err := protocodec.Codec.Marshal(plain{}); return err }},
		{"marshal nil", func() error { _, err := protocodec.Codec.Marshal(nil); return err }},
		{"unmarshal non-message", func() error { return protocodec.Codec.Unmarshal(nil, &plain{}) }},
		{"unmarshal nil interface", func() error { return protocodec.Codec.Unmarshal(nil, nil) }},
		{"unmarshal nil message", func() error { return protocodec.Codec.Unmarshal(nil, (*wrapperspb.StringValue)(nil)) }},
		{"unmarshal malformed", func() error { return protocodec.Codec.Unmarshal([]byte{0xff, 0xff}, new(wrapperspb.StringValue)) }},
		{"marshal invalid utf-8", func() error { _, err := protocodec.Codec.Marshal(wrapperspb.String("\xff")); return err }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.run()
			if err == nil {
				t.Fatal("succeeded, want error")
			}
			e, ok := errors.AsType[*errorx.Error](err)
			if !ok || e.Code != errorx.InvalidArgument {
				t.Fatalf("err = %v, want errorx.InvalidArgument", err)
			}
		})
	}
}

func TestConcurrentUse(t *testing.T) {
	var wg sync.WaitGroup
	for i := range 8 {
		wg.Go(func() {
			in := wrapperspb.Int64(int64(i))
			data, err := protocodec.Codec.Marshal(in)
			if err != nil {
				t.Error(err)
				return
			}
			out := new(wrapperspb.Int64Value)
			if err := protocodec.Codec.Unmarshal(data, out); err != nil || out.GetValue() != int64(i) {
				t.Errorf("round trip %d = %v, %v", i, out.GetValue(), err)
			}
		})
	}
	wg.Wait()
}
