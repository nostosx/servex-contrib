package redis

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"math"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode"

	goredis "github.com/redis/go-redis/v9"

	"github.com/nostosx/servex/app"
)

var ErrInvalidConfig = errors.New("redis: invalid configuration")

// Config contains serializable Redis connection and cache settings. Start with
// DefaultConfig before decoding. MasterName selects Sentinel; multiple Addrs
// without MasterName select Cluster, which supports DB zero only. MaxRetries
// zero disables node-command retries. MaxRedirects controls the Cluster loop
// (including MOVED/ASK and some network retries); zero disables that loop.
// Zero connection expiry disables that expiry.
// Runtime TLS objects and custom clients remain separate constructor inputs.
type Config struct {
	Addrs            []string      `json:"addrs"`
	MasterName       string        `json:"master_name"`
	Username         string        `json:"username"`
	Password         string        `json:"password"`
	SentinelUsername string        `json:"sentinel_username"`
	SentinelPassword string        `json:"sentinel_password"`
	DB               int           `json:"db"`
	Prefix           string        `json:"prefix"`
	DialTimeout      time.Duration `json:"dial_timeout"`
	ReadTimeout      time.Duration `json:"read_timeout"`
	WriteTimeout     time.Duration `json:"write_timeout"`
	PoolTimeout      time.Duration `json:"pool_timeout"`
	PoolSize         int           `json:"pool_size"`
	MinIdleConns     int           `json:"min_idle_conns"`
	ConnMaxIdleTime  time.Duration `json:"conn_max_idle_time"`
	ConnMaxLifetime  time.Duration `json:"conn_max_lifetime"`
	MaxRetries       int           `json:"max_retries"`
	MaxRedirects     int           `json:"max_redirects"`
	TLS              bool          `json:"tls"`
	ServerName       string        `json:"server_name"`
}

func DefaultConfig() Config {
	return Config{
		Addrs: []string{"127.0.0.1:6379"}, DialTimeout: 5 * time.Second,
		ReadTimeout: 5 * time.Second, WriteTimeout: 5 * time.Second,
		PoolTimeout: 6 * time.Second, PoolSize: 10, ConnMaxIdleTime: 5 * time.Minute, MaxRedirects: 3,
	}
}

// Validate reports field names, never addresses, passwords or raw configuration.
func (c Config) Validate() error {
	invalid := func(field string) error { return fmt.Errorf("%w: %s", ErrInvalidConfig, field) }
	if len(c.Addrs) == 0 {
		return invalid("addrs must not be empty")
	}
	for _, address := range c.Addrs {
		host, port, err := net.SplitHostPort(address)
		n, parseErr := strconv.Atoi(port)
		if err != nil || parseErr != nil || host == "" || n < 1 || n > 65535 || strings.IndexFunc(address, unicode.IsSpace) >= 0 || strings.ContainsAny(address, "/@?#") {
			return invalid("addrs must contain host:port values")
		}
		for _, digit := range port {
			if digit < '0' || digit > '9' {
				return invalid("addrs ports must be numeric")
			}
		}
	}
	if strings.TrimSpace(c.MasterName) != c.MasterName {
		return invalid("master_name must not contain surrounding whitespace")
	}
	if c.DB < 0 || (c.MasterName == "" && len(c.Addrs) > 1 && c.DB != 0) {
		return invalid("db must be nonnegative and zero for Cluster")
	}
	if c.MasterName == "" && (c.SentinelUsername != "" || c.SentinelPassword != "") {
		return invalid("sentinel credentials require master_name")
	}
	if c.DialTimeout <= 0 || c.ReadTimeout <= 0 || c.WriteTimeout <= 0 || c.PoolTimeout <= 0 {
		return invalid("dial/read/write/pool timeouts must be positive")
	}
	if c.PoolSize <= 0 || c.PoolSize > math.MaxInt32 || c.MinIdleConns < 0 || c.MinIdleConns > c.PoolSize {
		return invalid("pool_size and min_idle_conns are inconsistent")
	}
	if c.ConnMaxIdleTime < 0 || c.ConnMaxLifetime < 0 || c.MaxRetries < 0 || c.MaxRedirects < 0 {
		return invalid("connection expiry and retry budgets must be nonnegative")
	}
	if !c.TLS && c.ServerName != "" {
		return invalid("server_name requires tls")
	}
	return nil
}

// OpenOption configures runtime inputs that cannot be decoded from a file.
type OpenOption func(*openOptions)

type openOptions struct{ tlsConfig *tls.Config }

// WithTLSConfig injects an application-owned TLS policy. The policy is cloned
// during construction; certificate pools and callbacks must remain immutable.
func WithTLSConfig(config *tls.Config) OpenOption {
	return func(o *openOptions) {
		if config != nil {
			o.tlsConfig = config.Clone()
		}
	}
}

func (c Config) clientOptions() *goredis.UniversalOptions {
	idle, retries, redirects := c.ConnMaxIdleTime, c.MaxRetries, c.MaxRedirects
	if idle == 0 {
		idle = -1
	}
	if retries == 0 {
		retries = -1
	}
	if redirects == 0 {
		redirects = -1
	}
	return &goredis.UniversalOptions{
		Addrs: slices.Clone(c.Addrs), MasterName: c.MasterName, DB: c.DB,
		Username: c.Username, Password: c.Password,
		SentinelUsername: c.SentinelUsername, SentinelPassword: c.SentinelPassword,
		DialTimeout: c.DialTimeout, ReadTimeout: c.ReadTimeout, WriteTimeout: c.WriteTimeout,
		PoolTimeout: c.PoolTimeout, PoolSize: c.PoolSize, MinIdleConns: c.MinIdleConns,
		ConnMaxIdleTime: idle, ConnMaxLifetime: c.ConnMaxLifetime, MaxRetries: retries, MaxRedirects: redirects,
		ContextTimeoutEnabled: true,
	}
}

// Open constructs one owned Redis client and its cache without a readiness ping.
// Close on assembly failure; register Hook once for startup/readiness/shutdown.
// The client's native SDK may initialize its pools before Start. New remains the
// borrowed-client entry point for applications already managing a shared pool.
func Open(config Config, opts ...OpenOption) (*Cache, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	var o openOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	nativeOptions := config.clientOptions()
	if config.TLS {
		nativeOptions.TLSConfig = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: config.ServerName}
	}
	if o.tlsConfig != nil {
		nativeOptions.TLSConfig = o.tlsConfig.Clone()
	}
	client := goredis.NewUniversalClient(nativeOptions)
	return &Cache{client: client, prefix: config.Prefix, owner: &clientOwner{client: client, done: make(chan struct{})}}, nil
}

type clientOwner struct {
	client goredis.UniversalClient
	once   sync.Once
	done   chan struct{}
	err    error
}

// Client exposes the shared native client. It is borrowed; Cache.Close remains
// its sole owner when the cache was constructed by Open.
func (c *Cache) Client() goredis.UniversalClient { return c.client }

// Close releases only an Open-owned client, once. A New-created cache never
// closes its borrowed client. Later operations on an owned cache fail as closed.
func (c *Cache) Close() error {
	if c.owner == nil {
		return nil
	}
	c.owner.beginClose()
	<-c.owner.done
	return c.owner.err
}

func (o *clientOwner) beginClose() {
	o.once.Do(func() {
		go func() {
			o.err = o.client.Close()
			close(o.done)
		}()
	})
}

func (c *Cache) stop(ctx context.Context) error {
	if c.owner == nil {
		return nil
	}
	c.owner.beginClose()
	select {
	case <-c.owner.done:
		return c.owner.err
	default:
	}
	select {
	case <-c.owner.done:
		return c.owner.err
	case <-ctx.Done():
		return context.Cause(ctx)
	}
}

// Check pings Redis with the supplied context.
func (c *Cache) Check(ctx context.Context) error {
	if err := c.client.Ping(ctx).Err(); err != nil {
		return classify(ctx, "ping", err)
	}
	return nil
}

type cacheHook struct {
	name  string
	cache *Cache
}

// Hook checks readiness during startup and closes only an owned client on stop.
// Stop's context bounds waiting; SDK cleanup continues and later calls join it.
// Borrowed clients retain their caller's lifecycle owner.
func Hook(name string, cache *Cache) app.Hook {
	if strings.TrimSpace(name) == "" || cache == nil {
		panic("redis: hook requires a name and cache")
	}
	return &cacheHook{name: name, cache: cache}
}

func (h *cacheHook) Name() string { return h.name }
func (h *cacheHook) Start(ctx context.Context) error {
	if err := h.cache.Check(ctx); err != nil {
		return errors.Join(err, h.cache.stop(ctx))
	}
	return nil
}
func (h *cacheHook) Check(ctx context.Context) error { return h.cache.Check(ctx) }
func (h *cacheHook) Stop(ctx context.Context) error  { return h.cache.stop(ctx) }
