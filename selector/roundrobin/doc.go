// Package roundrobin provides a smooth weighted round-robin selector.Selector
// using the algorithm of nginx: picks follow the node weights exactly over
// every cycle and are spread evenly instead of in bursts.
package roundrobin
