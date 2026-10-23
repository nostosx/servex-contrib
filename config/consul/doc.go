// Package consul provides a config.Source backed by the Consul KV store. A
// Source reads a single key holding an encoded document, or every key under
// a prefix merged into one tree, and reports changes with blocking queries.
package consul
