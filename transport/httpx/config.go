package httpx

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
	"unicode"
)

// ServerConfig contains the file-decodable settings of an HTTP server. Start
// with DefaultServerConfig before decoding, so omitted fields retain defaults.
// Zero timeouts retain net/http fallback semantics, zero MaxHeaderBytes uses its
// default, and zero MaxBodyBytes disables the endpoint body limit.
// Middleware, loggers, codecs, health checks and protocols are runtime options.
type ServerConfig struct {
	Name              string        `json:"name"`
	Address           string        `json:"address"`
	ReadTimeout       time.Duration `json:"read_timeout"`
	ReadHeaderTimeout time.Duration `json:"read_header_timeout"`
	WriteTimeout      time.Duration `json:"write_timeout"`
	IdleTimeout       time.Duration `json:"idle_timeout"`
	MaxHeaderBytes    int           `json:"max_header_bytes"`
	MaxBodyBytes      int64         `json:"max_body_bytes"`
}

// DefaultServerConfig returns the same basic defaults as NewServer.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{Name: "http", Address: ":8000", MaxBodyBytes: DefaultMaxBodyBytes}
}

// Validate checks scalar settings without listening or invoking middleware.
func (c ServerConfig) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("httpx: invalid server config: name is required")
	}
	if err := validateServerAddress(c.Address); err != nil {
		return err
	}
	if c.ReadTimeout < 0 || c.ReadHeaderTimeout < 0 || c.WriteTimeout < 0 || c.IdleTimeout < 0 {
		return errors.New("httpx: invalid server config: timeouts must not be negative")
	}
	if c.MaxHeaderBytes < 0 || c.MaxBodyBytes < 0 {
		return errors.New("httpx: invalid server config: limits must not be negative")
	}
	return nil
}

// NewServerFromConfig validates cfg, applies runtime options, validates the
// effective scalar settings, then constructs the server without listening.
// Options may override scalar settings; invalid overrides are rejected before
// HTTP middleware is assembled. This function never loads configuration files.
func NewServerFromConfig(cfg ServerConfig, opts ...ServerOption) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	o := serverOptions{
		name: cfg.Name, addr: cfg.Address,
		readTimeout: cfg.ReadTimeout, readHeaderTimeout: cfg.ReadHeaderTimeout,
		writeTimeout: cfg.WriteTimeout, idleTimeout: cfg.IdleTimeout,
		maxHeaderBytes: cfg.MaxHeaderBytes, maxBodyBytes: cfg.MaxBodyBytes,
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	effective := ServerConfig{
		Name: o.name, Address: o.addr,
		ReadTimeout: o.readTimeout, ReadHeaderTimeout: o.readHeaderTimeout,
		WriteTimeout: o.writeTimeout, IdleTimeout: o.idleTimeout,
		MaxHeaderBytes: o.maxHeaderBytes, MaxBodyBytes: o.maxBodyBytes,
	}
	if err := effective.Validate(); err != nil {
		return nil, err
	}
	return newServer(o), nil
}

func validateServerAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.IndexFunc(address, unicode.IsSpace) >= 0 || strings.ContainsAny(host, "/\\?#@") || port == "" {
		return errors.New("httpx: invalid server config: address must be host:port")
	}
	if strings.Contains(host, ":") || strings.HasPrefix(address, "[") {
		if _, err := netip.ParseAddr(host); err != nil {
			return errors.New("httpx: invalid server config: invalid IP address")
		}
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return errors.New("httpx: invalid server config: address port must be numeric")
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n > 65535 {
		return errors.New("httpx: invalid server config: address port is out of range")
	}
	return nil
}
