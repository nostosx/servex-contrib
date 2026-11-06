package grpcx

import (
	"context"
	"errors"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/nostosx/servex/errorx"
)

func TestPublicErrorsRetainNativeAndApplicationCodes(t *testing.T) {
	server := NewServer(WithPublicErrors())
	for _, failure := range []error{
		status.Error(codes.NotFound, "private health details"),
		errorx.New(errorx.PermissionDenied, "PRIVATE_REASON", "private details").WithMetadata(map[string]string{"secret": "value"}),
		errors.New("private backend"),
	} {
		got := status.Convert(server.statusError(t.Context(), failure))
		want := toStatus(failure).Code()
		if want == codes.Unknown {
			want = codes.Internal
		}
		if got.Code() != want || strings.Contains(got.Message(), "private") || len(got.Details()) != 0 {
			t.Fatal(got)
		}
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if err := server.statusError(ctx, errors.New("private backend")); status.Code(err) != codes.Canceled {
		t.Fatal(err)
	}
	original := status.Error(codes.NotFound, "retained by default")
	if err := NewServer().statusError(t.Context(), original); status.Code(err) != codes.NotFound || status.Convert(err).Message() != "retained by default" {
		t.Fatal("default error behavior changed", err)
	}
}
