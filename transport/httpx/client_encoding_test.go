package httpx_test

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
)

type sequentialJSON struct {
	calls  int
	failAt int
	err    error
}

func (r *sequentialJSON) MarshalJSON() ([]byte, error) {
	r.calls++
	if r.calls == r.failAt {
		return nil, r.err
	}
	return fmt.Appendf(nil, `{"id":%d}`, r.calls), nil
}

func TestClientEncodingOrder(t *testing.T) {
	for _, path := range []string{"/echo", "/echo/{id}"} {
		t.Run(path, func(t *testing.T) {
			h := &capture{status: http.StatusOK}
			c := newTestClient(t, h, "http://example.com")
			req := &sequentialJSON{}
			if _, err := c.Endpoint(http.MethodPost, path, nil)(t.Context(), req); err != nil {
				t.Fatal(err)
			}
			wantPath := "/echo"
			if path != wantPath {
				wantPath = "/echo/1"
			}
			got := h.request()
			if req.calls != 2 || got.path != wantPath || got.body != `{"id":2}` {
				t.Fatalf("calls=%d request=%+v", req.calls, got)
			}
		})
	}
}

type countingRequestCodec struct{ calls int }

func (*countingRequestCodec) Name() string                { return "custom" }
func (*countingRequestCodec) Unmarshal([]byte, any) error { return nil }
func (c *countingRequestCodec) Marshal(any) ([]byte, error) {
	c.calls++
	return []byte("custom body"), nil
}

func TestClientCustomCodecKeepsJSONValidation(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		t.Run(fmt.Sprint(invalid), func(t *testing.T) {
			h := &capture{status: http.StatusOK}
			codec := &countingRequestCodec{}
			c := newTestClient(t, h, "http://example.com", httpx.WithCodec(codec))
			var req any = struct{ ID int }{ID: 42}
			if invalid {
				req = make(chan int)
			}
			_, err := c.Endpoint(http.MethodPost, "/echo", nil)(t.Context(), req)
			if invalid {
				if err == nil || !strings.HasPrefix(err.Error(), "httpx: encode request fields:") || codec.calls != 0 || h.request().method != "" {
					t.Fatalf("error=%v codec calls=%d request=%+v", err, codec.calls, h.request())
				}
			} else if err != nil || codec.calls != 1 || h.request().body != "custom body" {
				t.Fatalf("error=%v codec calls=%d request=%+v", err, codec.calls, h.request())
			}
		})
	}
}

func TestClientEncodingFailureStage(t *testing.T) {
	for _, failAt := range []int{1, 2} {
		t.Run(fmt.Sprint(failAt), func(t *testing.T) {
			h := &capture{status: http.StatusOK}
			c := newTestClient(t, h, "http://example.com")
			cause := errors.New("custom encoding failure")
			req := &sequentialJSON{failAt: failAt, err: cause}
			_, err := c.Endpoint(http.MethodPost, "/echo", nil)(t.Context(), req)
			prefix := "httpx: encode request fields:"
			if failAt == 2 {
				prefix = "httpx: encode request:"
			}
			if !errors.Is(err, cause) || !strings.HasPrefix(err.Error(), prefix) || req.calls != failAt || h.request().method != "" {
				t.Fatalf("error=%v calls=%d request=%+v", err, req.calls, h.request())
			}
		})
	}
}
