// Package redis implements the servex cache.Cache contract on top of a
// go-redis redis.UniversalClient, so a standalone, sentinel or cluster
// deployment can back cache.Loader and any other cache.Cache consumer.
package redis
