// Package etcd provides a config.Source backed by etcd. A Source reads a
// single key holding an encoded document, or every key under a prefix
// merged into one tree, and reports changes with an etcd watch.
package etcd
