package httpx

import (
	"context"
	"encoding/json/jsontext"
	"errors"
	"net"
	"net/http"
	"net/url"
	"slices"
	"testing"
	"time"

	"github.com/nostosx/servex/errorx"
)

func TestStatusTable(t *testing.T) {
	tests := []struct {
		code   errorx.Code
		status int
	}{
		{errorx.OK, 200},
		{errorx.Canceled, 499},
		{errorx.Unknown, 500},
		{errorx.InvalidArgument, 400},
		{errorx.DeadlineExceeded, 504},
		{errorx.NotFound, 404},
		{errorx.AlreadyExists, 409},
		{errorx.PermissionDenied, 403},
		{errorx.ResourceExhausted, 429},
		{errorx.FailedPrecondition, 400},
		{errorx.Aborted, 409},
		{errorx.OutOfRange, 400},
		{errorx.Unimplemented, 501},
		{errorx.Internal, 500},
		{errorx.Unavailable, 503},
		{errorx.DataLoss, 500},
		{errorx.Unauthenticated, 401},
		{errorx.Code(99), 500},
	}
	for _, tt := range tests {
		if got := statusOf(tt.code); got != tt.status {
			t.Errorf("statusOf(%v) = %d, want %d", tt.code, got, tt.status)
		}
	}
}

func TestReverseStatusTable(t *testing.T) {
	tests := []struct {
		status int
		code   errorx.Code
	}{
		{400, errorx.InvalidArgument},
		{401, errorx.Unauthenticated},
		{403, errorx.PermissionDenied},
		{404, errorx.NotFound},
		{405, errorx.Unknown}, // not in the contract table
		{408, errorx.Unknown}, // not in the contract table
		{409, errorx.AlreadyExists},
		{418, errorx.Unknown},
		{429, errorx.ResourceExhausted},
		{499, errorx.Canceled},
		{500, errorx.Unknown},
		{501, errorx.Unimplemented},
		{502, errorx.Unknown}, // a proxy error must not become retryable
		{503, errorx.Unavailable},
		{504, errorx.DeadlineExceeded},
		{599, errorx.Unknown},
	}
	for _, tt := range tests {
		if got := codeOf(tt.status); got != tt.code {
			t.Errorf("codeOf(%d) = %v, want %v", tt.status, got, tt.code)
		}
	}
	// Codes with a dedicated status survive the round trip.
	for _, c := range []errorx.Code{
		errorx.Canceled, errorx.InvalidArgument, errorx.DeadlineExceeded, errorx.NotFound,
		errorx.AlreadyExists, errorx.PermissionDenied, errorx.ResourceExhausted,
		errorx.Unimplemented, errorx.Unavailable, errorx.Unauthenticated, errorx.Unknown,
	} {
		if got := codeOf(statusOf(c)); got != c {
			t.Errorf("codeOf(statusOf(%v)) = %v", c, got)
		}
	}
}

func TestCodecName(t *testing.T) {
	tests := []struct {
		in   string
		want string
		ok   bool
	}{
		{"application/json", "json", true},
		{"Application/JSON", "json", true},
		{"application/problem+json", "json", true},
		{"application/x-yaml", "yaml", true},
		{"application/x-protobuf", "proto", true},
		{"application/protobuf", "proto", true},
		{"application/proto", "proto", true},
		{"*/*", "", false},
		{"application/*", "", false},
		{"json", "", false},
		{"application/x-", "", false},
	}
	for _, tt := range tests {
		got, ok := codecName(tt.in)
		if got != tt.want || ok != tt.ok {
			t.Errorf("codecName(%q) = %q, %v; want %q, %v", tt.in, got, ok, tt.want, tt.ok)
		}
	}
}

func TestPathNames(t *testing.T) {
	tests := []struct {
		pattern string
		want    []string
	}{
		{"GET /v1/users/{id}", []string{"id"}},
		{"POST example.com/v1/{org}/repos/{name}/{$}", []string{"org", "name"}},
		{"/files/{path...}", []string{"path"}},
		{"GET /static/", nil},
		{"GET /{a}/{b}/{c...}", []string{"a", "b", "c"}},
	}
	for _, tt := range tests {
		if got := pathNames(tt.pattern); !slices.Equal(got, tt.want) {
			t.Errorf("pathNames(%q) = %v, want %v", tt.pattern, got, tt.want)
		}
	}
}

func TestTemplateExpand(t *testing.T) {
	fields := map[string]jsontext.Value{
		"id":    jsontext.Value(`42`),
		"name":  jsontext.Value(`"a b/c"`),
		"path":  jsontext.Value(`"x y/z"`),
		"flag":  jsontext.Value(`true`),
		"null":  jsontext.Value(`null`),
		"extra": jsontext.Value(`"q"`),
	}
	tests := []struct {
		path    string
		want    string
		used    []string
		wantErr bool
	}{
		{"/v1/users/{id}", "/v1/users/42", []string{"id"}, false},
		{"/v1/{name}/{flag}", "/v1/a%20b%2Fc/true", []string{"flag", "name"}, false},
		{"/files/{path...}", "/files/x%20y/z", []string{"path"}, false},
		{"/static/{$}", "/static/{$}", nil, false},
		{"/v1/{missing}", "", nil, true},
		{"/v1/{null}", "", nil, true},
	}
	for _, tt := range tests {
		got, used, err := parseTemplate(tt.path).expand(fields)
		if (err != nil) != tt.wantErr {
			t.Errorf("expand(%q) error = %v, wantErr %v", tt.path, err, tt.wantErr)
			continue
		}
		if err != nil {
			continue
		}
		var names []string
		for n := range used {
			names = append(names, n)
		}
		slices.Sort(names)
		if got != tt.want || !slices.Equal(names, tt.used) {
			t.Errorf("expand(%q) = %q, %v; want %q, %v", tt.path, got, names, tt.want, tt.used)
		}
	}
}

func TestAddQuery(t *testing.T) {
	q := make(url.Values)
	addQuery(q, "s", jsontext.Value(`"x"`))
	addQuery(q, "n", jsontext.Value(`1.5`))
	addQuery(q, "list", jsontext.Value(`[1, "two", null, true]`))
	addQuery(q, "null", jsontext.Value(`null`))
	addQuery(q, "obj", jsontext.Value(`{"a": 1}`))
	want := "list=1&list=two&list=true&n=1.5&obj=%7B%22a%22%3A1%7D&s=x"
	if got := q.Encode(); got != want {
		t.Errorf("query = %s, want %s", got, want)
	}
}

// Inner is embedded by bindTarget to test field promotion.
type Inner struct {
	Deep  string `json:"deep"`
	Shade string `json:"shade"`
}

// hidden is an unexported embedded struct whose exported fields are promoted.
type hidden struct {
	Secret string `json:"secret"`
}

type bindTarget struct {
	Inner
	*Ptr
	hidden
	Shade    string `json:"shade"`
	Named    Inner  `json:"named"`
	Skipped  string `json:"-"`
	NoTag    string
	Unsigned uint8             `json:"unsigned"`
	F32      float32           `json:"f32"`
	When     time.Time         `json:"when"`
	WhenPtr  *time.Time        `json:"when_ptr"`
	Bytes    []byte            `json:"bytes"`
	Map      map[string]string `json:"map"`
	Ints     []int64           `json:"ints"`
	private  string
}

// Ptr is embedded through a pointer.
type Ptr struct {
	Via string `json:"via"`
}

func TestBindValues(t *testing.T) {
	var dst bindTarget
	err := bindValues(&dst, map[string][]string{
		"deep":     {"d"},
		"shade":    {"outer"},
		"via":      {"p"},
		"secret":   {"s"},
		"named":    {"ignored"},
		"Skipped":  {"x"},
		"-":        {"x"},
		"NoTag":    {"n"},
		"unsigned": {"255"},
		"f32":      {"1.5"},
		"when":     {"2026-01-02T03:04:05Z"},
		"when_ptr": {"2026-01-02T03:04:05Z"},
		"bytes":    {"b"},
		"map":      {"m"},
		"ints":     {"1", "-2"},
		"private":  {"p"},
	})
	if err != nil {
		t.Fatalf("bindValues: %v", err)
	}
	want := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	switch {
	case dst.Deep != "d", dst.Inner.Shade != "", dst.Shade != "outer":
		t.Errorf("promotion: %+v", dst)
	case dst.Ptr == nil || dst.Via != "p":
		t.Errorf("embedded pointer: %+v", dst.Ptr)
	case dst.Secret != "s":
		t.Errorf("unexported embedded: %q", dst.Secret)
	case dst.Named != Inner{}, dst.Skipped != "", dst.Bytes != nil, dst.Map != nil, dst.private != "":
		t.Errorf("unsupported or excluded fields were bound: %+v", dst)
	case dst.NoTag != "n", dst.Unsigned != 255, dst.F32 != 1.5:
		t.Errorf("scalars: %+v", dst)
	case !dst.When.Equal(want), dst.WhenPtr == nil || !dst.WhenPtr.Equal(want):
		t.Errorf("text unmarshalers: %v %v", dst.When, dst.WhenPtr)
	case !slices.Equal(dst.Ints, []int64{1, -2}):
		t.Errorf("ints = %v", dst.Ints)
	}

	for name, vals := range map[string][]string{
		"unsigned": {"256"},
		"f32":      {"x"},
		"when":     {"yesterday"},
		"ints":     {"1", "x"},
	} {
		if err := bindValues(new(bindTarget), map[string][]string{name: vals}); err == nil {
			t.Errorf("bindValues(%s=%v) succeeded, want error", name, vals)
		}
	}

	// Non-struct destinations are left alone.
	m := map[string]string{}
	if err := bindValues(&m, map[string][]string{"a": {"b"}}); err != nil || len(m) != 0 {
		t.Errorf("map destination: %v, %v", m, err)
	}
	if err := bindValues((*bindTarget)(nil), map[string][]string{"shade": {"x"}}); err != nil {
		t.Errorf("nil destination: %v", err)
	}
}

// failingListener fails every Accept with err.
type failingListener struct{ err error }

func (l failingListener) Accept() (net.Conn, error) { return nil, l.err }
func (l failingListener) Close() error              { return nil }
func (l failingListener) Addr() net.Addr            { return &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1), Port: 1234} }

func TestServerErrReportsServeFailure(t *testing.T) {
	errAccept := errors.New("accept failed")
	s := NewServer()
	s.listen = func(_ context.Context, _, _ string) (net.Listener, error) {
		return failingListener{err: errAccept}, nil
	}
	if err := s.Start(t.Context()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if got := s.URL().String(); got != "http://127.0.0.1:1234" {
		t.Errorf("URL = %s", got)
	}
	err, ok := <-s.Err()
	if !ok || !errors.Is(err, errAccept) {
		t.Fatalf("Err() = %v, %v; want %v", err, ok, errAccept)
	}
	if _, ok := <-s.Err(); ok {
		t.Error("Err channel not closed after serving ended")
	}
	if err := s.Stop(t.Context()); err != nil && !errors.Is(err, http.ErrServerClosed) {
		t.Errorf("Stop: %v", err)
	}
}
