module github.com/nostosx/servex-contrib/transport/httpx

go 1.27.3

require (
	github.com/nostosx/servex v1.7.0
	github.com/nostosx/servex-contrib/discovery/memory v1.0.1
	github.com/nostosx/servex-contrib/encoding/json v1.0.1
	github.com/nostosx/servex-contrib/job/memory v1.0.0
	github.com/nostosx/servex-contrib/messaging/memory v1.0.0
	github.com/nostosx/servex-contrib/selector v1.2.1
	github.com/nostosx/servex-contrib/storage/cache/memory v1.0.0
	github.com/nostosx/servex-contrib/storage/database/sql v1.0.0
	github.com/nostosx/servex-contrib/transport/client v1.1.0
)

require (
	github.com/nostosx/kitx v1.0.0 // indirect
	github.com/nostosx/servex-contrib/observability/metrics/noop v1.0.0 // indirect
	github.com/nostosx/servex-contrib/observability/tracing/noop v1.0.0 // indirect
)
