package httpx

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strconv"
	"time"
)

// HTTPPolicy configures a bounded HTTP client for untrusted URLs. An injected
// application client may instead enforce its own network/proxy policy.
type HTTPPolicy struct {
	AllowPrivateNetwork bool
	MaxResponseBytes    int64
	Timeout             time.Duration
}

// ErrResponseTooLarge means the HTTP body exceeded the configured limit.
var ErrResponseTooLarge = errors.New("httpx: response body exceeds limit")

// NewRestrictedHTTPClient rejects URL credentials and private/reserved targets,
// validates every redirect and pins each connection to a checked DNS address.
// It does not inherit environment proxies, which would bypass address pinning.
// The caller owns idle connections; no retries or cookie jar are installed.
func NewRestrictedHTTPClient(p HTTPPolicy) (*http.Client, error) {
	if p.MaxResponseBytes == 0 {
		p.MaxResponseBytes = 16 << 20
	}
	if p.Timeout == 0 {
		p.Timeout = 30 * time.Second
	}
	if p.MaxResponseBytes < 1 || p.MaxResponseBytes > 1<<30 || p.Timeout < 0 {
		return nil, errors.New("httpx: invalid HTTP policy limits")
	}
	t := &http.Transport{MaxIdleConns: 100, IdleConnTimeout: 90 * time.Second, TLSHandshakeTimeout: 10 * time.Second, ExpectContinueTimeout: time.Second, ForceAttemptHTTP2: true}
	dialer := &net.Dialer{Timeout: 15 * time.Second, KeepAlive: 30 * time.Second}
	t.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, errors.New("httpx: invalid target address")
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, errors.New("httpx: target DNS lookup failed")
		}
		if len(ips) == 0 {
			return nil, errors.New("httpx: target has no addresses")
		}
		for _, ip := range ips {
			if !p.AllowPrivateNetwork && !publicAddress(ip) {
				return nil, errors.New("httpx: private or reserved target refused")
			}
		}
		var last error
		for _, ip := range ips {
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(ip.String(), port))
			if err == nil {
				return conn, nil
			}
			last = err
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		return nil, last
	}
	c := &http.Client{Timeout: p.Timeout, Transport: &boundedTransport{base: t, max: p.MaxResponseBytes}}
	c.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return errors.New("httpx: too many redirects")
		}
		if err := validHTTPURL(req.URL); err != nil {
			return err
		}
		if len(via) > 0 && via[len(via)-1].URL.Scheme == "https" && req.URL.Scheme != "https" {
			return errors.New("httpx: HTTPS downgrade refused")
		}
		return nil
	}
	return c, nil
}

func validHTTPURL(u *url.URL) error {
	if u == nil || u.Hostname() == "" || u.User != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return errors.New("httpx: HTTP(S) URL without credentials is required")
	}
	if port := u.Port(); port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return errors.New("httpx: invalid port")
		}
	}
	return nil
}

func publicAddress(ip netip.Addr) bool {
	ip = ip.Unmap()
	if !ip.IsValid() || !ip.IsGlobalUnicast() || ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() {
		return false
	}
	for _, cidr := range []string{"0.0.0.0/8", "100.64.0.0/10", "192.0.0.0/24", "192.0.2.0/24", "198.18.0.0/15", "198.51.100.0/24", "203.0.113.0/24", "240.0.0.0/4", "192.88.99.0/24", "64:ff9b::/96", "64:ff9b:1::/48", "100::/64", "2001::/23", "2001:db8::/32", "2002::/16", "3fff::/20", "5f00::/16"} {
		if netip.MustParsePrefix(cidr).Contains(ip) {
			return false
		}
	}
	return true
}

type boundedTransport struct {
	base *http.Transport
	max  int64
}

func (t *boundedTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if err := validHTTPURL(req.URL); err != nil {
		return nil, err
	}
	res, err := t.base.RoundTrip(req)
	if err != nil {
		return nil, err
	}
	if res.ContentLength > t.max {
		res.Body.Close()
		return nil, ErrResponseTooLarge
	}
	res.Body = &boundedBody{ReadCloser: res.Body, remaining: t.max}
	return res, nil
}
func (t *boundedTransport) CloseIdleConnections() { t.base.CloseIdleConnections() }

type boundedBody struct {
	io.ReadCloser
	remaining int64
}

func (b *boundedBody) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if b.remaining == 0 {
		var probe [1]byte
		n, err := b.ReadCloser.Read(probe[:])
		if n > 0 {
			return 0, ErrResponseTooLarge
		}
		return 0, err
	}
	if int64(len(p)) > b.remaining {
		p = p[:b.remaining]
	}
	n, err := b.ReadCloser.Read(p)
	b.remaining -= int64(n)
	return n, err
}
