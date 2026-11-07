package consul

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"

	"github.com/hashicorp/consul/api"
)

// Config contains file-decodable Consul connection settings. Registration
// identity and the decision to register remain application-owned.
type Config struct {
	Address    string `json:"address"`
	Scheme     string `json:"scheme"`
	Datacenter string `json:"datacenter"`
	ACLToken   string `json:"acl_token"`
}

// DefaultConfig selects a local HTTP Consul agent without credentials.
func DefaultConfig() Config {
	return Config{Address: "127.0.0.1:8500", Scheme: "http"}
}

// Validate checks TCP connection settings without contacting Consul. For Unix
// sockets or additional native SDK options, construct a client and use New.
func (c Config) Validate() error {
	if c.Scheme != "http" && c.Scheme != "https" {
		return errors.New("consul: scheme must be http or https")
	}
	host, port, err := net.SplitHostPort(c.Address)
	if err != nil || strings.TrimSpace(host) == "" || strings.ContainsAny(c.Address, " \t\r\n/\\?#@") {
		return errors.New("consul: address must be host:port")
	}
	if _, err := url.Parse(c.Scheme + "://" + c.Address); err != nil {
		return errors.New("consul: invalid address")
	}
	n, err := strconv.ParseUint(port, 10, 16)
	if err != nil || n == 0 {
		return errors.New("consul: port must be between 1 and 65535")
	}
	return nil
}

// NewFromConfig constructs a registry without network requests. Registry options
// remain explicit, and the application owns registration and deregistration.
func NewFromConfig(cfg Config, opts ...Option) (*Registry, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	client, err := api.NewClient(&api.Config{
		Address: cfg.Address, Scheme: cfg.Scheme,
		Datacenter: cfg.Datacenter, Token: cfg.ACLToken,
	})
	if err != nil {
		return nil, err
	}
	return New(client, opts...), nil
}
