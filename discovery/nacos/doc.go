// Package nacos implements discovery.Registrar and discovery.Discoverer on
// top of the Nacos naming client. Every endpoint of an instance is
// published as an ephemeral Nacos instance, and watchers follow changes
// through naming subscriptions. Instance weights map to Nacos weights.
package nacos
