package protovalidate_test

import (
	"context"
	"errors"
	"sync"
	"testing"

	validate "buf.build/gen/go/bufbuild/protovalidate/protocolbuffers/go/buf/validate"
	bufvalidate "buf.build/go/protovalidate"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/nostosx/servex-contrib/validation/protovalidate"
	"github.com/nostosx/servex/errorx"
)

// message builds real schema annotations without requiring a protoc executable.
func message(t *testing.T, name string, badRule bool) *dynamicpb.Message {
	t.Helper()
	fieldOptions := &descriptorpb.FieldOptions{}
	proto.SetExtension(fieldOptions, validate.E_Field, &validate.FieldRules{
		Type: &validate.FieldRules_String_{String_: &validate.StringRules{MinLen: proto.Uint64(3)}},
	})
	messageOptions := &descriptorpb.MessageOptions{}
	if badRule {
		proto.SetExtension(messageOptions, validate.E_Message, &validate.MessageRules{Cel: []*validate.Rule{{
			Id: new("broken"), Expression: new("this.name +"),
		}}})
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("request.proto"), Package: new("adaptertest"), Syntax: new("proto3"),
		Dependency: []string{"buf/validate/validate.proto"},
		MessageType: []*descriptorpb.DescriptorProto{{
			Name: new("Request"), Options: messageOptions,
			Field: []*descriptorpb.FieldDescriptorProto{{
				Name: new("name"), Number: proto.Int32(1),
				Label: descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL.Enum(),
				Type:  descriptorpb.FieldDescriptorProto_TYPE_STRING.Enum(), Options: fieldOptions,
			}},
		}},
	}, protoregistry.GlobalFiles)
	if err != nil {
		t.Fatal(err)
	}
	msg := dynamicpb.NewMessage(file.Messages().Get(0))
	msg.Set(msg.Descriptor().Fields().ByName("name"), protoreflect.ValueOfString(name))
	return msg
}

func TestSchemaValidation(t *testing.T) {
	v, err := bufvalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name string
		req  any
		code errorx.Code
	}{
		{name: "valid", req: message(t, "alice", false)},
		{name: "field violation", req: message(t, "a", false), code: errorx.InvalidArgument},
		{name: "invalid CEL", req: message(t, "alice", true), code: errorx.Internal},
		{name: "wrong endpoint type", req: struct{}{}, code: errorx.Internal},
		{name: "nil message", code: errorx.Internal},
		{name: "typed nil message", req: (*emptypb.Empty)(nil), code: errorx.Internal},
		{name: "typed nil dynamic message", req: (*dynamicpb.Message)(nil), code: errorx.Internal},
	} {
		t.Run(tt.name, func(t *testing.T) {
			calls := 0
			businessErr := errors.New("business error")
			ep := protovalidate.New(v)(func(ctx context.Context, req any) (any, error) {
				calls++
				if ctx != t.Context() || req != tt.req {
					t.Fatal("request or context changed")
				}
				return "response", businessErr
			})
			resp, err := ep(t.Context(), tt.req)
			if tt.code == 0 {
				if calls != 1 || resp != "response" || err != businessErr {
					t.Fatalf("next = %v, %v, calls = %d", resp, err, calls)
				}
				return
			}
			e, ok := errors.AsType[*errorx.Error](err)
			if !ok || e.Code != tt.code || calls != 0 || resp != nil {
				t.Fatalf("result = %v, %v; calls = %d", resp, err, calls)
			}
			if tt.code == errorx.InvalidArgument {
				if e.Reason != protovalidate.ReasonValidationFailed || e.Message != "request validation failed" {
					t.Fatalf("public error = %+v", e)
				}
				if _, ok := errors.AsType[*bufvalidate.ValidationError](err); !ok {
					t.Fatalf("validation cause was lost: %v", err)
				}
			} else if e.Reason != protovalidate.ReasonValidationError {
				t.Fatalf("internal reason = %s", e.Reason)
			}
			if tt.name == "invalid CEL" {
				if _, ok := errors.AsType[*bufvalidate.CompilationError](err); !ok {
					t.Fatalf("compilation cause was lost: %v", err)
				}
			}
		})
	}
}

type validatorFunc func(proto.Message) error

func (f validatorFunc) Validate(msg proto.Message, _ ...bufvalidate.ValidationOption) error {
	return f(msg)
}

func TestCancellationAndEvaluationFailure(t *testing.T) {
	for _, phase := range []string{"before", "during", "runtime failure"} {
		t.Run(phase, func(t *testing.T) {
			ctx, cancel := context.WithCancelCause(t.Context())
			defer cancel(nil)
			cause := errors.New("request aborted")
			calls := 0
			v := validatorFunc(func(proto.Message) error {
				calls++
				if phase == "during" {
					cancel(cause)
				}
				return &bufvalidate.RuntimeError{}
			})
			if phase == "before" {
				cancel(cause)
			}
			ep := protovalidate.New(v)(func(context.Context, any) (any, error) {
				t.Fatal("failed validation reached next")
				return nil, nil
			})
			_, err := ep(ctx, &emptypb.Empty{})
			if phase != "runtime failure" {
				if err != cause || (phase == "before" && calls != 0) || (phase == "during" && calls != 1) {
					t.Fatalf("error = %v, calls = %d", err, calls)
				}
			} else if _, ok := errors.AsType[*bufvalidate.RuntimeError](err); !ok || errorx.CodeOf(err) != errorx.Internal {
				t.Fatalf("runtime failure = %v", err)
			}
		})
	}
}

func TestConcurrentValidation(t *testing.T) {
	v, err := bufvalidate.New()
	if err != nil {
		t.Fatal(err)
	}
	ep := protovalidate.New(v)(func(context.Context, any) (any, error) { return "ok", nil })
	msg := message(t, "alice", false)
	var wg sync.WaitGroup
	for range 32 {
		wg.Go(func() {
			if _, err := ep(t.Context(), msg); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
}

func TestNilValidator(t *testing.T) {
	for _, v := range []bufvalidate.Validator{nil, validatorFunc(nil), (*validatorFunc)(nil)} {
		t.Run("nil validator", func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("nil validator did not panic")
				}
			}()
			protovalidate.New(v)
		})
	}
}
