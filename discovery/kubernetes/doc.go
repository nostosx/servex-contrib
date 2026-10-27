// Package kubernetes implements discovery.Discoverer on top of Kubernetes
// EndpointSlices (discovery.k8s.io/v1). The service name is the name of a
// Kubernetes Service, ready endpoints become instances and every named port
// becomes an endpoint URL whose scheme is the port name. EndpointSlices
// carry no weight: instances have Weight 0, the selector default.
package kubernetes
