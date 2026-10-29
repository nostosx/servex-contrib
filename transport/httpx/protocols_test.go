package httpx_test

import (
	"context"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/nostosx/servex-contrib/transport/httpx"
)

func TestProtocolConfiguration(t *testing.T) {
	t.Parallel()
	p := new(http.Protocols)
	p.SetHTTP1(true)
	p.SetUnencryptedHTTP2(true)
	s := httpx.NewServer(httpx.WithAddress("127.0.0.1:0"), httpx.WithProtocols(p))
	// Server options must not retain mutable caller-owned protocol settings.
	p.SetUnencryptedHTTP2(false)
	s.HandleHTTP("GET /protocol", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.ProtoMajor != 2 {
			t.Errorf("protocol=%s", r.Proto)
		}
		_, _ = io.WriteString(w, "h2c")
	}))
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	if err := s.Start(ctx); err != nil {
		t.Fatal(err)
	}
	defer func() {
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer stopCancel()
		if err := s.Stop(stopCtx); err != nil {
			t.Error(err)
		}
	}()
	clientProtocols := new(http.Protocols)
	clientProtocols.SetUnencryptedHTTP2(true)
	transport := &http.Transport{Protocols: clientProtocols}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.URL().String()+"/protocol", nil)
	if err != nil {
		t.Fatal(err)
	}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	body, err := io.ReadAll(res.Body)
	if err != nil || string(body) != "h2c" || res.ProtoMajor != 2 {
		t.Fatalf("response=%q protocol=%s error=%v", body, res.Proto, err)
	}
}
