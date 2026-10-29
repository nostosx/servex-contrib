//go:build ignore

// This disposable loopback fixture is built explicitly by run.mjs in a
// temporary module. It never changes library module requirements.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/coder/websocket"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/nostosx/servex-contrib/transport/connectx"
	"github.com/nostosx/servex-contrib/transport/httpx"
	"github.com/nostosx/servex-contrib/transport/ssex"
	"github.com/nostosx/servex-contrib/transport/websocketx"
	"github.com/nostosx/servex/errorx"
	"github.com/nostosx/servex/transport/rpc"
	"github.com/nostosx/servex/transport/sse"
)

type callState struct {
	Requests int           `json:"requests"`
	Canceled bool          `json:"canceled"`
	Finished bool          `json:"finished"`
	LastID   string        `json:"lastId"`
	Release  chan struct{} `json:"-"`
}

type fixture struct {
	mu    sync.Mutex
	calls map[string]*callState
}

func (f *fixture) begin(id string, r *http.Request) *callState {
	f.mu.Lock()
	defer f.mu.Unlock()
	s := f.calls[id]
	if s == nil {
		s = &callState{Release: make(chan struct{})}
		f.calls[id] = s
	}
	s.Requests++
	if r != nil {
		s.LastID = r.Header.Get("Last-Event-ID")
	}
	return s
}

func (f *fixture) finish(id string, canceled bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls[id].Finished = true
	f.calls[id].Canceled = canceled
}

func (f *fixture) control(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := r.URL.Query().Get("id")
	s := f.calls[id]
	if s == nil {
		http.Error(w, "unknown test", http.StatusNotFound)
		return
	}
	if r.URL.Path == "/release" {
		select {
		case <-s.Release:
		default:
			close(s.Release)
		}
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(s)
}

func (f *fixture) events(w http.ResponseWriter, r *http.Request) {
	id := r.URL.Query().Get("id")
	s := f.begin(id, r)
	stream, err := new(ssex.Acceptor).Accept(w, r)
	if err != nil {
		log.Print(err)
		return
	}
	if r.URL.Query().Get("mode") == "resume" {
		if r.Header.Get("Last-Event-ID") == "cursor-1" {
			_ = stream.Send(sse.Event{ID: "cursor-2", Type: "token", Data: "resumed"})
		} else {
			retry := 50 * time.Millisecond
			_ = stream.Send(sse.Event{ID: "cursor-1", Type: "token", Data: "initial", Retry: &retry})
		}
		return
	}
	if err := stream.Comment("heartbeat"); err != nil {
		return
	}
	if err := stream.Send(sse.Event{ID: "cursor-1", Type: "token", Data: "hello\n世界"}); err != nil {
		return
	}
	select {
	case <-s.Release:
		_ = stream.Send(sse.Event{Data: "done"})
		f.finish(id, false)
	case <-r.Context().Done():
		f.finish(id, true)
	}
}

func cors(allowed string, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == allowed {
			w.Header().Set("Access-Control-Allow-Origin", origin)
			w.Header().Set("Access-Control-Allow-Credentials", "true")
			w.Header().Add("Vary", "Origin")
			w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
			w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Connect-Protocol-Version, Connect-Timeout-Ms, Grpc-Timeout, X-Grpc-Web, X-User-Agent, X-Case-Id")
			w.Header().Set("Access-Control-Expose-Headers", "X-Reply, Trailer-X-End, Grpc-Status, Grpc-Message, Grpc-Status-Details-Bin")
		}
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func main() {
	assets := flag.String("assets", "", "directory containing browser bundle")
	flag.Parse()
	allowedListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	deniedListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	allowed := "http://" + allowedListener.Addr().String()
	denied := "http://" + deniedListener.Addr().String()
	f := &fixture{calls: make(map[string]*callState)}
	api := httpx.NewServer(httpx.WithAddress("127.0.0.1:0"), httpx.WithProtocols(connectx.Protocols()), httpx.WithHTTPMiddleware(func(next http.Handler) http.Handler { return cors(allowed, next) }))
	api.HandleHTTP("GET /events", http.HandlerFunc(f.events))
	api.HandleHTTP("GET /state", http.HandlerFunc(f.control))
	api.HandleHTTP("POST /release", http.HandlerFunc(f.control))
	accept := &websocketx.Acceptor{Options: websocket.AcceptOptions{OriginPatterns: []string{allowedListener.Addr().String()}, Subprotocols: []string{"echo"}}}
	api.HandleHTTP("GET /ws", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := accept.Accept(w, r)
		if err != nil {
			return
		}
		defer func() { _ = c.CloseNow() }()
		for {
			typ, data, err := c.Read(r.Context())
			if err != nil {
				return
			}
			if err := c.Write(r.Context(), typ, data); err != nil {
				return
			}
		}
	}))
	const prefix = "/servex.browser.v1.BrowserService/"
	api.HandleHTTP(prefix+"Unary", connectx.NewUnaryHandler(prefix+"Unary", connectx.UnaryFunc[wrapperspb.StringValue, wrapperspb.StringValue](func(_ context.Context, req *rpc.Request[wrapperspb.StringValue]) (*rpc.Response[wrapperspb.StringValue], error) {
		if req.Msg.Value == "error" {
			return nil, errorx.New(errorx.Unavailable, "BROWSER_DOWN", "backend unavailable").WithMetadata(map[string]string{"source": "browser-fixture"}).WithRetryable(false)
		}
		return &rpc.Response[wrapperspb.StringValue]{Msg: wrapperspb.String(req.Msg.Value), Header: http.Header{"X-Reply": {"browser"}}, Trailer: http.Header{"X-End": {"done"}}}, nil
	})))
	api.HandleHTTP(prefix+"Watch", connectx.NewServerStreamHandler(prefix+"Watch", func(ctx context.Context, req *rpc.Request[wrapperspb.StringValue], stream rpc.ServerStream[wrapperspb.StringValue]) error {
		id := req.Header.Get("X-Case-Id")
		s := f.begin(id, nil)
		stream.ResponseHeader().Set("X-Reply", "stream")
		stream.ResponseTrailer().Set("X-End", "done")
		if err := stream.Send(wrapperspb.String("first")); err != nil {
			return err
		}
		if req.Msg.Value == "error" {
			f.finish(id, false)
			return errorx.New(errorx.ResourceExhausted, "STREAM_LIMIT", "stream limit reached")
		}
		select {
		case <-s.Release:
			f.finish(id, false)
			return stream.Send(wrapperspb.String("second"))
		case <-ctx.Done():
			f.finish(id, true)
			return ctx.Err()
		}
	}))
	page := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/browser.js" {
			http.ServeFile(w, r, filepath.Join(*assets, "browser.js"))
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		_, _ = fmt.Fprint(w, "<!doctype html><meta charset=utf-8><title>servex browser acceptance</title><pre id=results>Ready</pre><script src=/browser.js></script>")
	})
	api.HandleHTTP("GET /{$}", page)
	api.HandleHTTP("GET /browser.js", page)
	uiServers := []*http.Server{{Handler: page}, {Handler: page}}
	go func() {
		if err := uiServers[0].Serve(allowedListener); err != nil && err != http.ErrServerClosed {
			log.Print(err)
		}
	}()
	go func() {
		if err := uiServers[1].Serve(deniedListener); err != nil && err != http.ErrServerClosed {
			log.Print(err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := api.Start(ctx); err != nil {
		log.Fatal(err)
	}
	ready := map[string]string{"api": strings.TrimSuffix(api.URL().String(), "/"), "allowed": allowed, "denied": denied}
	_ = json.NewEncoder(os.Stdout).Encode(ready)
	<-ctx.Done()
	shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_ = api.Stop(shutdown)
	for _, server := range uiServers {
		_ = server.Shutdown(shutdown)
	}
}
