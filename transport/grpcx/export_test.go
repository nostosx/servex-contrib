package grpcx

import (
	"maps"
	"net"
	"slices"
	"time"
)

// WithListener makes the server serve on ln instead of listening on its
// address. It is exported for tests only.
func WithListener(ln net.Listener) ServerOption {
	return func(o *serverOptions) { o.listener = ln }
}

// WithHealthInterval sets how often health Watch streams re-evaluate the
// checker. It is exported for tests only.
func WithHealthInterval(d time.Duration) ServerOption {
	return func(o *serverOptions) { o.healthInterval = d }
}

// ConnCount returns the number of pooled node connections.
func (c *Client) ConnCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.conns)
}

// ConnAddrs returns the sorted addresses of the pooled node connections.
func (c *Client) ConnAddrs() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return slices.Sorted(maps.Keys(c.conns))
}
