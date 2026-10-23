// Package etcd implements discovery.Registrar and discovery.Discoverer on
// top of etcd v3. Instances are stored as JSON under
// <namespace>/<name>/<id>, attached to a lease that is kept alive while the
// instance is registered and granted again when it is lost.
package etcd
