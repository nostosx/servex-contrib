// Package consul implements discovery.Registrar and discovery.Discoverer on
// top of the Consul agent API. Instances are registered as agent services
// with a TTL check kept passing by a heartbeat, and watched through blocking
// health queries that return passing instances only.
package consul
