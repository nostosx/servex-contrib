// Package p2c provides a Power of Two Choices selector.Selector: every call
// samples two distinct nodes at random and picks the one with the lower load,
// estimated from a peak-sensitive EWMA of its latency and its number of
// in-flight calls. Call outcomes reported through Done update the estimate;
// failures are recorded with a latency penalty so that failing nodes are
// avoided until they recover.
package p2c
