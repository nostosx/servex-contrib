package httpx_test

import (
	"bytes"
	"context"
	jsonv2 "encoding/json/v2"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/nostosx/servex-contrib/transport/httpx"
)

type benchmarkMessage struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Active bool   `json:"active"`
	Data   string `json:"data"`
}

func benchmarkPayload(size int) *benchmarkMessage {
	return &benchmarkMessage{ID: 42, Name: "alice", Active: true, Data: strings.Repeat("x", size)}
}

func benchmarkHTTPHandler(servex, query bool) http.Handler {
	pattern := "POST /echo"
	if query {
		pattern = "GET /echo/{id}"
	}
	if servex {
		s := httpx.NewServer()
		httpx.Route[benchmarkMessage](s, pattern, func(_ context.Context, req any) (any, error) { return req, nil })
		return s
	}
	mux := http.NewServeMux()
	mux.HandleFunc(pattern, func(w http.ResponseWriter, r *http.Request) {
		req := new(benchmarkMessage)
		if query {
			var err error
			req.ID, err = strconv.Atoi(r.PathValue("id"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			q := r.URL.Query()
			req.Name, req.Data = q.Get("name"), q.Get("data")
			req.Active, err = strconv.ParseBool(q.Get("active"))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		} else {
			data, err := io.ReadAll(http.MaxBytesReader(w, r.Body, httpx.DefaultMaxBodyBytes))
			if err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			if err := jsonv2.Unmarshal(data, req); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		data, err := jsonv2.Marshal(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write(data)
	})
	return mux
}

// No TCP or client: request/recorder creation is included equally on both sides.
func BenchmarkHTTPHandler(b *testing.B) {
	for _, size := range []int{128, 4096} {
		for _, mode := range []string{"Native", "Servex"} {
			b.Run(fmt.Sprintf("%s/bytes=%d", mode, size), func(b *testing.B) {
				h := benchmarkHTTPHandler(mode == "Servex", false)
				data, err := jsonv2.Marshal(benchmarkPayload(size))
				if err != nil {
					b.Fatal(err)
				}
				for b.Loop() {
					r := httptest.NewRequest(http.MethodPost, "/echo", bytes.NewReader(data))
					r.Header.Set("Content-Type", "application/json")
					w := httptest.NewRecorder()
					h.ServeHTTP(w, r)
					if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), data) {
						b.Fatalf("response: status=%d body=%s", w.Code, w.Body.Bytes())
					}
				}
			})
		}
	}
}

func BenchmarkHTTPQueryBinding(b *testing.B) {
	for _, mode := range []string{"Native", "Servex"} {
		b.Run(mode, func(b *testing.B) {
			h := benchmarkHTTPHandler(mode == "Servex", true)
			want, err := jsonv2.Marshal(&benchmarkMessage{ID: 42, Name: "alice", Active: true, Data: "hello"})
			if err != nil {
				b.Fatal(err)
			}
			for b.Loop() {
				r := httptest.NewRequest(http.MethodGet, "/echo/42?name=alice&active=true&data=hello", nil)
				w := httptest.NewRecorder()
				h.ServeHTTP(w, r)
				if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), want) {
					b.Fatalf("response: status=%d body=%s", w.Code, w.Body.Bytes())
				}
			}
		})
	}
}

func benchmarkNativeHTTPCall(ctx context.Context, hc *http.Client, url string, payload *benchmarkMessage) error {
	data, err := jsonv2.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url+"/echo", bytes.NewReader(data))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	resp, err := hc.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP status %d", resp.StatusCode)
	}
	data, err = io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	reply := new(benchmarkMessage)
	if err := jsonv2.Unmarshal(data, reply); err != nil {
		return err
	}
	if *reply != *payload {
		return fmt.Errorf("HTTP echo response differs from request")
	}
	return nil
}

// Each case reuses one warmed HTTP/1.1 keep-alive transport. Native is a typed,
// handwritten JSON endpoint, not an equivalent implementation of all servex features.
func benchmarkHTTPRoundTrip(b *testing.B, mode string, size int, fake bool) func() error {
	b.Helper()
	payload := benchmarkPayload(size)
	var hc *http.Client
	url := "http://benchmark.invalid"
	if fake {
		data, err := jsonv2.Marshal(payload)
		if err != nil {
			b.Fatal(err)
		}
		hc = &http.Client{Transport: benchmarkRoundTripper{data: data}}
	} else {
		s := httptest.NewServer(benchmarkHTTPHandler(mode != "Native", false))
		b.Cleanup(s.Close)
		url = s.URL
		tr := &http.Transport{MaxIdleConns: 64, MaxIdleConnsPerHost: 64}
		b.Cleanup(tr.CloseIdleConnections)
		hc = &http.Client{Transport: tr}
	}
	if mode != "ServexBoth" {
		return func() error { return benchmarkNativeHTTPCall(b.Context(), hc, url, payload) }
	}
	c, err := httpx.NewClient(b.Context(), url, httpx.WithHTTPClient(hc))
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = c.Close() })
	ep := httpx.Call[benchmarkMessage](c, http.MethodPost, "/echo")
	return func() error {
		resp, err := ep(b.Context(), payload)
		if err != nil {
			return err
		}
		if reply, ok := resp.(*benchmarkMessage); !ok || *reply != *payload {
			return fmt.Errorf("servex HTTP echo response differs from request")
		}
		return nil
	}
}

type benchmarkRoundTripper struct{ data []byte }

func (rt benchmarkRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	if _, err := io.Copy(io.Discard, req.Body); err != nil {
		return nil, err
	}
	_ = req.Body.Close()
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(bytes.NewReader(rt.data)), Request: req}, nil
}

func BenchmarkHTTPRoundTrip(b *testing.B) {
	for _, size := range []int{128, 4096} {
		for _, mode := range []string{"Native", "ServexServer", "ServexBoth"} {
			b.Run(fmt.Sprintf("%s/bytes=%d", mode, size), func(b *testing.B) {
				call := benchmarkHTTPRoundTrip(b, mode, size, false)
				if err := call(); err != nil {
					b.Fatal(err)
				}
				for b.Loop() {
					if err := call(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkHTTPRoundTripParallel(b *testing.B) {
	for _, mode := range []string{"Native", "ServexServer", "ServexBoth"} {
		b.Run(mode, func(b *testing.B) {
			call := benchmarkHTTPRoundTrip(b, mode, 128, false)
			if err := call(); err != nil {
				b.Fatal(err)
			}
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					if err := call(); err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

// A deterministic in-memory RoundTripper isolates client-side request building
// and codecs from TCP, server dispatch and scheduler effects.
func BenchmarkHTTPClientInMemory(b *testing.B) {
	for _, size := range []int{128, 4096} {
		for _, mode := range []string{"Native", "ServexBoth"} {
			b.Run(fmt.Sprintf("%s/bytes=%d", mode, size), func(b *testing.B) {
				call := benchmarkHTTPRoundTrip(b, mode, size, true)
				if err := call(); err != nil {
					b.Fatal(err)
				}
				for b.Loop() {
					if err := call(); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}
