package client

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
)

func TestChineseProviderLogin(t *testing.T) {
	for _, kind := range []ProviderKind{WeChat, WeChatWeb, QQ, Weibo} {
		t.Run(string(kind), func(t *testing.T) {
			t.Parallel()
			var calls atomic.Int32
			backend := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				w.Header().Set("Content-Type", "application/json")
				if err := r.ParseForm(); err != nil {
					t.Error(err)
				}
				if r.Form.Get("code_verifier") != "" {
					t.Error("undocumented PKCE sent")
				}
				switch r.URL.Path {
				case "/sns/oauth2/access_token":
					if r.Method != http.MethodGet || r.Form.Get("appid") != "app" || r.Form.Get("secret") != "secret" || r.Form.Get("code") != "code" {
						t.Error("bad WeChat exchange")
					}
					_, _ = io.WriteString(w, `{"access_token":"token","openid":"openid","unionid":"union"}`)
				case "/sns/userinfo":
					if r.Form.Get("openid") != "openid" || r.Form.Get("access_token") != "token" {
						t.Error("bad WeChat user query")
					}
					_, _ = io.WriteString(w, `{"openid":"openid","unionid":"union","nickname":"微信用户"}`)
				case "/oauth2.0/token", "/oauth2/access_token":
					if r.Form.Get("client_id") != "app" || r.Form.Get("client_secret") != "secret" || r.Form.Get("code") != "code" || r.Form.Get("redirect_uri") != "https://app.example/callback" {
						t.Error("bad QQ/Weibo exchange")
					}
					if kind == QQ && (r.Method != http.MethodGet || r.Form.Get("fmt") != "json") {
						t.Error("bad QQ token format")
					}
					if kind == Weibo && r.Method != http.MethodPost {
						t.Error("bad Weibo method")
					}
					_, _ = io.WriteString(w, `{"access_token":"token","uid":"9007199254740993"}`)
				case "/oauth2.0/me":
					if r.Form.Get("fmt") != "json" || r.Form.Get("access_token") != "token" {
						t.Error("QQ must request JSON")
					}
					_, _ = io.WriteString(w, `{"client_id":"app","openid":"openid"}`)
				case "/user/get_user_info":
					if r.Form.Get("oauth_consumer_key") != "app" || r.Form.Get("openid") != "openid" || r.Form.Get("access_token") != "token" {
						t.Error("bad QQ user query")
					}
					_, _ = io.WriteString(w, `{"ret":0,"nickname":"QQ用户"}`)
				case "/2/users/show.json":
					if r.Form.Get("uid") != "9007199254740993" || r.Form.Get("access_token") != "token" {
						t.Error("bad Weibo UID")
					}
					_, _ = io.WriteString(w, `{"id":9007199254740993,"idstr":"9007199254740993","screen_name":"微博用户"}`)
				default:
					t.Error("unexpected provider endpoint", r.URL.Path)
				}
			}))
			defer backend.Close()
			target, _ := url.Parse(backend.URL)
			store, _ := NewMemoryStore(10)
			provider, err := NewProvider(t.Context(), ProviderConfig{Kind: kind, ClientID: "app", ClientSecret: "secret", RedirectURL: "https://app.example/callback", HTTPClient: &http.Client{Transport: providerTransport{target: target}}}, store)
			if err != nil {
				t.Fatal(err)
			}
			authorization, err := provider.Begin(t.Context(), "browser")
			if err != nil {
				t.Fatal(err)
			}
			u, _ := url.Parse(authorization.URL)
			if u.Query().Get("code_challenge") != "" || u.Query().Get("nonce") != "" {
				t.Fatal("unsupported PKCE/nonce advertised")
			}
			if (kind == WeChat || kind == WeChatWeb) && (u.Query().Get("appid") != "app" || u.Query().Get("client_id") != "" || u.Fragment != "wechat_redirect") {
				t.Fatal("invalid WeChat authorization", u)
			}
			identity, err := provider.Complete(t.Context(), "browser", u.Query().Get("state"), "code")
			if err != nil || identity.Subject == "" || identity.Name == "" || identity.EmailVerified || identity.Email != "" {
				t.Fatalf("identity=%+v error=%v", identity, err)
			}
			if (kind == WeChat || kind == WeChatWeb || kind == QQ) && !strings.HasSuffix(identity.Issuer, "/appid/app") {
				t.Fatal("OpenID not application-scoped", identity)
			}
			if (kind == WeChat || kind == WeChatWeb) && identity.UnionID != "union" {
				t.Fatal("missing UnionID")
			}
			before := calls.Load()
			if _, err := provider.Complete(t.Context(), "browser", u.Query().Get("state"), "code"); !errors.Is(err, ErrInvalidState) || calls.Load() != before {
				t.Fatal("replay reached provider", err)
			}
		})
	}
}

type chinaTransport func(*http.Request) (*http.Response, error)

func (f chinaTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func TestChineseProviderRejectsInvalidIdentity(t *testing.T) {
	for _, tt := range []struct {
		name       string
		kind       ProviderKind
		path, body string
	}{
		{"WeChat error", WeChat, "/sns/oauth2/access_token", `{"errcode":40029,"errmsg":"sensitive"}`},
		{"WeChat missing OpenID", WeChat, "/sns/oauth2/access_token", `{"access_token":"sensitive"}`},
		{"WeChat subject mismatch", WeChat, "/sns/userinfo", `{"openid":"someone-else"}`},
		{"WeChat UnionID mismatch", WeChat, "/sns/userinfo", `{"openid":"openid","unionid":"other"}`},
		{"QQ wrong app", QQ, "/oauth2.0/me", `{"client_id":"wrong","openid":"openid"}`},
		{"QQ missing OpenID", QQ, "/oauth2.0/me", `{"client_id":"app"}`},
		{"QQ JSONP rejected", QQ, "/oauth2.0/me", `callback({"client_id":"app","openid":"openid"});`},
		{"QQ API error", QQ, "/user/get_user_info", `{"ret":1002}`},
		{"QQ missing ret", QQ, "/user/get_user_info", `{"nickname":"name"}`},
		{"Weibo wrong UID", Weibo, "/2/users/show.json", `{"id":43,"idstr":"43"}`},
		{"Weibo malformed UID", Weibo, "/oauth2/access_token", `{"access_token":"token","uid":"4e2"}`},
		{"Weibo API error", Weibo, "/2/users/show.json", `{"error_code":21314,"error":"sensitive"}`},
		{"oversized", WeChat, "/sns/userinfo", strings.Repeat(" ", (1<<20)+1)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			transport := chinaTransport(func(r *http.Request) (*http.Response, error) {
				body := `{"access_token":"token","openid":"openid","unionid":"union","uid":"42"}`
				if r.URL.Path == tt.path {
					body = tt.body
				}
				return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/json"}}, Body: io.NopCloser(strings.NewReader(body)), Request: r}, nil
			})
			store, _ := NewMemoryStore(10)
			p, err := NewProvider(t.Context(), ProviderConfig{Kind: tt.kind, ClientID: "app", ClientSecret: "secret", RedirectURL: "https://app.example/callback", HTTPClient: &http.Client{Transport: transport}}, store)
			if err != nil {
				t.Fatal(err)
			}
			a, _ := p.Begin(t.Context(), "browser")
			u, _ := url.Parse(a.URL)
			if _, err := p.Complete(t.Context(), "browser", u.Query().Get("state"), "code"); err == nil || strings.Contains(err.Error(), "sensitive") {
				t.Fatal("invalid identity or sensitive error", err)
			}
		})
	}
}

func TestChineseProviderRedactsCredentialURLs(t *testing.T) {
	for _, redirect := range []bool{false, true} {
		t.Run(map[bool]string{false: "transport error", true: "redirect"}[redirect], func(t *testing.T) {
			store, _ := NewMemoryStore(10)
			transport := chinaTransport(func(r *http.Request) (*http.Response, error) {
				if !redirect {
					return nil, errors.New(r.URL.String())
				}
				return &http.Response{StatusCode: http.StatusFound, Header: http.Header{"Location": {"https://attacker.example/"}}, Body: io.NopCloser(strings.NewReader("")), Request: r}, nil
			})
			p, err := NewProvider(t.Context(), ProviderConfig{Kind: QQ, ClientID: "app", ClientSecret: "never-log-me", RedirectURL: "https://app.example/callback", HTTPClient: &http.Client{Transport: transport}}, store)
			if err != nil {
				t.Fatal(err)
			}
			a, _ := p.Begin(t.Context(), "browser")
			u, _ := url.Parse(a.URL)
			_, err = p.Complete(t.Context(), "browser", u.Query().Get("state"), "private-code")
			if err == nil || strings.Contains(err.Error(), "never-log-me") || strings.Contains(err.Error(), "private-code") {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			cancel()
			if _, err := p.Complete(ctx, "browser", "state", "code"); !errors.Is(err, context.Canceled) {
				t.Fatal(err)
			}
		})
	}
	// Record decoding must keep large numeric Weibo IDs exact.
	var id struct {
		ID json.Number `json:"id"`
	}
	if err := json.Unmarshal([]byte(`{"id":9007199254740993}`), &id); err != nil || id.ID.String() != "9007199254740993" {
		t.Fatal(id, err)
	}
}
