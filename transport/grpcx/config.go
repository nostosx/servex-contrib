package grpcx

import (
	"errors"
	"math"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"time"
	"unicode"

	"google.golang.org/grpc"
)

// ServerConfig contains the file-decodable settings of a gRPC server. Start
// with DefaultServerConfig before decoding. Zero message limits select the gRPC
// defaults: 4 MiB received and math.MaxInt32 bytes sent. HealthInterval must be
// positive, and only takes effect when WithHealth installs health checking.
// Credentials, middleware, health checkers and listeners remain runtime options.
type ServerConfig struct {
	Name           string        `json:"name"`
	Address        string        `json:"address"`
	MaxRecvMsgSize int           `json:"max_recv_msg_size"`
	MaxSendMsgSize int           `json:"max_send_msg_size"`
	HealthInterval time.Duration `json:"health_interval"`
}

// DefaultServerConfig returns the same address/name and health cadence as
// NewServer, with explicit gRPC message defaults.
func DefaultServerConfig() ServerConfig {
	return ServerConfig{
		Name: "grpc", Address: ":9000", MaxRecvMsgSize: 4 << 20,
		MaxSendMsgSize: math.MaxInt32, HealthInterval: defaultHealthInterval,
	}
}

// Validate checks settings without constructing a gRPC server or listening.
func (c ServerConfig) Validate() error {
	if strings.TrimSpace(c.Name) == "" {
		return errors.New("grpcx: invalid server config: name is required")
	}
	if err := validateServerAddress(c.Address); err != nil {
		return err
	}
	if c.MaxRecvMsgSize < 0 || c.MaxRecvMsgSize > math.MaxInt32 || c.MaxSendMsgSize < 0 || c.MaxSendMsgSize > math.MaxInt32 {
		return errors.New("grpcx: invalid server config: message limits must be between zero and 2147483647")
	}
	if c.HealthInterval <= 0 {
		return errors.New("grpcx: invalid server config: health_interval must be positive")
	}
	return nil
}

// NewServerFromConfig validates cfg and effective scalar runtime overrides
// before constructing the server. Configuration message limits are applied
// after WithGRPCOptions, so native options cannot silently override them.
// It never loads files or opens a listener; register services before Start.
func NewServerFromConfig(cfg ServerConfig, opts ...ServerOption) (*Server, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	o := serverOptions{name: cfg.Name, addr: cfg.Address, healthInterval: cfg.HealthInterval}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	effective := cfg
	effective.Name, effective.Address, effective.HealthInterval = o.name, o.addr, o.healthInterval
	if err := effective.Validate(); err != nil {
		return nil, err
	}
	if cfg.MaxRecvMsgSize == 0 {
		cfg.MaxRecvMsgSize = 4 << 20
	}
	if cfg.MaxSendMsgSize == 0 {
		cfg.MaxSendMsgSize = math.MaxInt32
	}
	o.grpcOpts = append(o.grpcOpts, grpc.MaxRecvMsgSize(cfg.MaxRecvMsgSize), grpc.MaxSendMsgSize(cfg.MaxSendMsgSize))
	return newServer(o), nil
}

func validateServerAddress(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil || strings.IndexFunc(address, unicode.IsSpace) >= 0 || strings.ContainsAny(host, "/\\?#@") || port == "" {
		return errors.New("grpcx: invalid server config: address must be host:port")
	}
	if strings.Contains(host, ":") || strings.HasPrefix(address, "[") {
		if _, err := netip.ParseAddr(host); err != nil {
			return errors.New("grpcx: invalid server config: invalid IP address")
		}
	}
	for _, digit := range port {
		if digit < '0' || digit > '9' {
			return errors.New("grpcx: invalid server config: address port must be numeric")
		}
	}
	n, err := strconv.Atoi(port)
	if err != nil || n > 65535 {
		return errors.New("grpcx: invalid server config: address port is out of range")
	}
	return nil
}
