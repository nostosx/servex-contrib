module github.com/nostosx/servex-contrib/transport/httpx

go 1.27.3

require (
	github.com/nostosx/servex v1.4.0
	github.com/nostosx/servex-contrib/broker/memory v1.0.0
	github.com/nostosx/servex-contrib/cache/memory v1.0.0
	github.com/nostosx/servex-contrib/database/sql v1.0.0
	github.com/nostosx/servex-contrib/discovery/memory v1.0.0
	github.com/nostosx/servex-contrib/encoding/json v1.0.0
	github.com/nostosx/servex-contrib/jobqueue/memory v1.0.0
	github.com/nostosx/servex-contrib/selector v1.2.0
	github.com/nostosx/servex-contrib/transport/client v1.0.0
)

require (
	github.com/nostosx/kitx v1.0.0 // indirect
	github.com/nostosx/servex-contrib/metrics/noop v1.0.0 // indirect
	github.com/nostosx/servex-contrib/tracing/noop v1.0.0 // indirect
)
