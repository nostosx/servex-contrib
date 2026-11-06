# servex-contrib

[English](README.md) | 简体中文

[servex](https://github.com/nostosx/servex) 契约的具体实现，包括后端适配器、编解码器、
节点选择、可观测性与中间件。核心保留契约和最小框架运行时；仅依赖标准库或 kitx 的
具体实现也放在本仓库。

当前源码使用 Go 1.27.3 和已发布的 **servex v1.7.0**。本轮 contrib 改动仍未发布，
部分迁移后的 contrib 模块需要配套源码。历史版本保留各自路径与依赖，见
[CHANGELOG.md](CHANGELOG.md)。源码检查直接使用公开 core，无需同级 servex 目录。

## 目录职责

| 目录 | 职责 |
|---|---|
| `transport` | HTTP、gRPC、Connect、SSE、WebSocket、CORS 与共享客户端引擎 |
| `messaging` | Kafka、NATS、RabbitMQ 与内存发布订阅后端 |
| `job` | Asynq、PGMQ 与内存任务执行后端 |
| `observability` | 相互独立的日志、指标与追踪实现 |
| `storage` | 缓存、数据库、向量、文本检索与 S3 对象存储实现 |
| `config`、`discovery`、`selector` | 配置源、服务发现与节点选择 |
| `middleware` | endpoint、application、client、messaging、job 执行边界的显式策略 |
| `auth`、`session`、`ratelimit`、`validation` | 独立选用的身份／登录、浏览器会话、配额与校验实现 |
| `scheduler`、`coordination` | 进程内／数据库调度、锁与选主 |
| `lifecycle`、`di/fx`、`di/do` | 资源所有权与原生 DI 生命周期桥接；应用默认推荐 Fx 装配 |

## 模块

当前有 64 个独立 Go 模块，仓库根目录没有 go.mod。模块路径为
`github.com/nostosx/servex-contrib/<目录>`。独立依赖集合避免选用一个后端时引入其他 SDK；
基础能力可以显式复用，但依赖图必须保持单向。

目录按能力命名，而不是按消费它的中间件边界命名：认证 SDK 位于 `auth`，校验 SDK 位于
`validation`，限流后端位于 `ratelimit`，浏览器 Session 位于 `session`。通用 middleware
与 HTTP 模块不会引入这些可选 SDK，应用需显式选择并装配所需模块。

| 模块 | 职责 | 主要依赖 |
|---|---|---|
| [`config/consul`](config/consul) | Consul KV 上的 `config.Source`、`config.Watchable` | `github.com/hashicorp/consul/api` |
| [`config/etcd`](config/etcd) | etcd 上的配置读取与监听 | `go.etcd.io/etcd/client/v3` |
| [`config/nacos`](config/nacos) | Nacos 配置服务上的读取与监听 | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`config/viper`](config/viper) | `config.Config`、文件／环境变量源、快照与校验 | `github.com/spf13/viper` |
| [`coordination/etcd`](coordination/etcd) | 租约锁、选主及 fencing revision | etcd 官方 concurrency SDK |
| [`di/do`](di/do) | 原生 samber/do 生命周期与就绪桥接；应用负责装配 | github.com/samber/do/v2 |
| [`di/fx`](di/fx) | 默认推荐的原生 Fx 装配与 servex 生命周期接入 | `go.uber.org/fx` |
| [`discovery/consul`](discovery/consul) | Consul agent 上的 `discovery.Registrar`、`discovery.Discoverer` | `github.com/hashicorp/consul/api` |
| [`discovery/etcd`](discovery/etcd) | 基于 etcd lease 的注册与发现 | `go.etcd.io/etcd/client/v3` |
| [`discovery/kubernetes`](discovery/kubernetes) | 基于 EndpointSlice 的服务发现 | `k8s.io/client-go` |
| [`discovery/memory`](discovery/memory) | 内存注册与发现 | 标准库、core |
| [`discovery/nacos`](discovery/nacos) | Nacos naming 服务上的注册与发现 | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`encoding/json`](encoding/json) | JSON `encoding.Codec` | encoding/json/v2 |
| [`encoding/proto`](encoding/proto) | 名为 `proto` 的 `encoding.Codec` | `google.golang.org/protobuf` |
| [`encoding/yaml`](encoding/yaml) | 名为 `yaml` 的 `encoding.Codec` | `go.yaml.in/yaml/v3` |
| [`job/asynq`](job/asynq) | asynq 上的 `job.Enqueuer`、`job.Runner` | `github.com/hibiken/asynq` |
| [`job/memory`](job/memory) | 内存任务队列 `job.Enqueuer` / `Runner` | 标准库、core、kitx |
| [`job/pgmq`](job/pgmq) | 事务入队、投递 fencing 与持久工作者 | database/sql, PGMQ |
| [`lifecycle`](lifecycle) | 显式资源所有权、装配失败清理及逆序关闭 | 标准库、core |
| [`messaging/kafka`](messaging/kafka) | Apache Kafka 上的 `messaging.Publisher`、`messaging.Subscriber` | `github.com/twmb/franz-go` |
| [`messaging/memory`](messaging/memory) | 内存发布与订阅 | 标准库、core、kitx |
| [`messaging/nats`](messaging/nats) | NATS JetStream 上的发布与订阅 | `github.com/nats-io/nats.go` |
| [`messaging/rabbitmq`](messaging/rabbitmq) | RabbitMQ（AMQP 0-9-1）上的发布与订阅 | `github.com/rabbitmq/amqp091-go` |
| [`middleware`](middleware) | 各执行层独立的日志、指标、追踪，以及显式鉴权、校验、授权、隔离与一致性策略 | 标准库、servex、Noop 遥测 |
| [`auth/jwt`](auth/jwt) | JWT 签发与类型化验证，供应用鉴权回调使用 | `github.com/golang-jwt/jwt/v5` |
| [`auth/oauth2`](auth/oauth2) | PKCE 授权码流程、浏览器绑定的一次性 state、刷新及客户端凭证 | Go 官方 `golang.org/x/oauth2` |
| [`auth/oauth2redis`](auth/oauth2redis) | 与 Session 共用 Redis 连接池的原子 OAuth2 state 存储 | Redis 官方 `github.com/redis/go-redis/v9` |
| [`auth/oidc`](auth/oidc) | OIDC 发现、ID Token 签名与 JWKS 验证，供应用鉴权回调使用 | `github.com/coreos/go-oidc/v3` |
| [`auth/password`](auth/password) | Argon2id 密码哈希、受限参数验证及升级判断 | Go 官方 `golang.org/x/crypto/argon2` |
| [`auth/login`](auth/login) | OAuth2/OIDC 或密码登录、Session 轮换及统一业务身份 | OAuth2、OIDC、Session 及 auth 中间件 |
| [`session`](session) | 自有服务端 Session、安全 Cookie、原子 Token 轮换及有界内存存储 | 标准库 |
| [`session/redis`](session/redis) | 共享 Session 存储、原子版本校验与轮换、TTL；支持单机/Sentinel | Redis 官方 `github.com/redis/go-redis/v9` |
| [`ratelimit/rate`](ratelimit/rate) | 有 key 数量上限的本地令牌桶，供通用限流中间件使用 | `golang.org/x/time/rate` |
| [`ratelimit/redis`](ratelimit/redis) | 多副本共享的 Redis 原子令牌桶，支持 context、等待提示和后端错误 | `github.com/redis/go-redis/v9` |
| [`validation/playground`](validation/playground) | 基于结构体标签的校验 `endpoint.Middleware` | `github.com/go-playground/validator/v10` |
| [`validation/protovalidate`](validation/protovalidate) | 基于 Protobuf schema/CEL 的校验 `endpoint.Middleware` | `buf.build/go/protovalidate` |
| [`observability/logger/slog`](observability/logger/slog) | `logger.Logger` | `log/slog` |
| [`observability/logger/zap`](observability/logger/zap) | `logger.Logger` | `go.uber.org/zap` |
| [`observability/metrics/noop`](observability/metrics/noop) | `metrics.Meter` 空操作后端 | 标准库、core |
| [`observability/metrics/otel`](observability/metrics/otel) | OpenTelemetry 上的 `metrics.Meter` | `go.opentelemetry.io/otel/metric` |
| [`observability/metrics/prometheus`](observability/metrics/prometheus) | Prometheus 上的 `metrics.Meter` | `github.com/prometheus/client_golang` |
| [`observability/tracing/noop`](observability/tracing/noop) | `tracing.Tracer` / `Propagator` 空操作后端 | 标准库、core |
| [`observability/tracing/otel`](observability/tracing/otel) | OpenTelemetry 上的 `tracing.Tracer`、`tracing.Propagator` | `go.opentelemetry.io/otel/trace` |
| [`scheduler/cron`](scheduler/cron) | 进程内周期任务、取消、重叠抑制与应用生命周期 | 社区 `github.com/robfig/cron/v3` |
| [`scheduler/pgcron`](scheduler/pgcron) | 持久化命名 SQL 调度 | database/sql, pg_cron |
| [`selector`](selector) | 加权随机、平滑加权轮询与 P2C 节点选择 | 标准库、servex |
| [`storage/cache/loader`](storage/cache/loader) | 显式 Codec、owner 共享执行与写回策略的缓存加载器 | 标准库、core、kitx/singleflight |
| [`storage/cache/memory`](storage/cache/memory) | 内存 `cache.Cache` | kitx/ttlcache |
| [`storage/cache/redis`](storage/cache/redis) | 单机、哨兵或集群 Redis 上的 `cache.Cache` | `github.com/redis/go-redis/v9` |
| [`storage/database/goose`](storage/database/goose) | 显式 SQL 版本迁移、状态及 PostgreSQL 验收 | 社区 `github.com/pressly/goose/v3` |
| [`storage/database/gorm`](storage/database/gorm) | GORM 原生操作观测、生命周期与健康检查，包含 PostgreSQL 验收 | `gorm.io/gorm` |
| [`storage/database/mongodb`](storage/database/mongodb) | MongoDB 驱动的追踪、指标、生命周期与健康检查 | `go.mongodb.org/mongo-driver/v2` |
| [`storage/database/postgres`](storage/database/postgres) | 驱动无关的 SQL 辅助与显式扩展管理 | database/sql |
| [`storage/database/sql`](storage/database/sql) | database/sql 追踪、指标、生命周期与就绪检查 | 标准库、core、Noop 遥测 |
| [`storage/s3`](storage/s3) | 对象上传下载、删除、元数据、分页及预签名 | AWS 官方 Go SDK v2 |
| [`storage/search/postgres`](storage/search/postgres) | 原生、ParadeDB、PGroonga 与 trigram 文本检索 | database/sql |
| [`storage/vector/pgvector`](storage/vector/pgvector) | 向量记录、精确／ANN 检索及调用方事务 | database/sql, pgvector |
| [`transport/client`](transport/client) | HTTP/gRPC 复用的发现、选择与调用引擎 | 标准库、core、kitx/backoff |
| [`transport/connectx`](transport/connectx) | Connect/gRPC/gRPC-Web 类型化 unary 与流式 RPC | `connectrpc.com/connect` |
| [`transport/cors`](transport/cors) | 显式来源策略、凭证配置与预检处理 | 社区 `github.com/rs/cors` |
| [`transport/grpcx`](transport/grpcx) | gRPC 服务端及支持中间件、发现和节点选择的客户端 | `google.golang.org/grpc` |
| [`transport/httpx`](transport/httpx) | HTTP 服务端与客户端 | net/http、JSON 编解码器、selector |
| [`transport/ssex`](transport/ssex) | SSE 事件/心跳发送及有界流式客户端 | 标准库 |
| [`transport/websocketx`](transport/websocketx) | WebSocket 握手、消息、Ping、大小限制与关闭状态 | `github.com/coder/websocket` |

## 配置与基础装配

基础设施配置结构体归对应 contrib 组件；应用只组合自己的顶层配置，core 保留配置契约。
当前提供以下配置入口：

| 组件 | 可解码配置 | 构造入口 |
|---|---|---|
| slog / Zap | `Config`、`DefaultConfig`、`Validate` | `New(config)`；已有原生对象用 `Wrap(native)` |
| GORM PostgreSQL | `DatabaseConfig`、`DefaultDatabaseConfig`、`Validate` | `OpenPostgres(name, config, tracer, meter, options...)` |
| database/sql | `DatabaseConfig`、`DefaultDatabaseConfig`、`Validate` | `OpenFromConfig(config, options...)` |
| HTTP / gRPC 服务端 | `ServerConfig`、`DefaultServerConfig`、`Validate` | `NewServerFromConfig(config, options...)` |
| Redis 缓存 | `Config`、`DefaultConfig`、`Validate` | `Open(config, options...)`；`New(client)` 借用连接池 |

先填默认值，再解码合并后的文件／环境变量快照。未设置字段保留默认值；显式零值和 false
遵循各组件说明。下面的 slog、gorm、httpx 均来自对应 contrib 模块：

```go
type Config struct {
    Log      slog.Config         `json:"log"`
    Database gorm.DatabaseConfig `json:"database"`
    HTTP     httpx.ServerConfig  `json:"http"`
}

cfg := Config{
    Log: slog.DefaultConfig(),
    Database: gorm.DefaultDatabaseConfig(),
    HTTP: httpx.DefaultServerConfig(),
}
if err := settings.Snapshot().Unmarshal("", &cfg); err != nil { return err }
if err := errors.Join(cfg.Log.Validate(), cfg.Database.Validate(), cfg.HTTP.Validate()); err != nil {
    return err
}
```

`settings` 由 [config/viper](config/viper) 加载，例如 file.New("config.toml") 与
cfgviper.Env("APP_")；`APP_DATABASE__DSN` 覆盖 database.dsn。Viper 按 JSON 标签解码字段，
并将 "30m" 等字符串转为 Duration。配置重载应在 WithValidator 中重复默认值／解码／校验流程；
新快照通过校验，不代表运行中的连接池、监听器或日志输出会自动重建。

Writer、Tracer、Meter、TLS 对象、原生 GORM 配置、回调和中间件通过运行时参数／Option 注入。
在 app.go 中将选用的配置提供给原生 Fx，普通构造函数调用组件配置入口。构造后立即安排清理，
覆盖装配和启动失败；自有与借用资源的 Close／Hook 行为见各组件 README。

## 应用组合

默认推荐在应用的 `app.go` 使用原生 Fx，保留普通构造函数，每份资源只设一个生命周期负责人。
[di/fx](di/fx) 桥接 Fx 与 servex 的启动和退出流程，也支持手动装配或 [di/do](di/do)。

应用显式选择中间件和执行顺序。[middleware](middleware) 按 endpoint、application、client、
messaging、job 分为五个普通包，分别处理类型化业务、整次出站调用、节点尝试和消息／任务原生结果。
SDK 适配器保持独立模块，不自动安装预设链。

可运行应用见 [servex-examples](https://github.com/nostosx/servex-examples)；PostgreSQL 能力归属与
集成测试 fixture 见 [storage/database/postgres](storage/database/postgres)。已有应用需要迁移
导入路径和依赖，才能使用当前 API。

## 开发

使用 Go 1.27.3、Task v3.53.1 和 Bash。在 contrib 根目录选择需要验证的模块：

```sh
bash .github/scripts/check-source.sh check lint vulncheck \
  MODULES='middleware transport/httpx'
```

该入口从公共代理解析 servex v1.7.0，仅对尚未发布的 contrib 依赖建立仓库外临时映射。
不会修改正式清单。全部模块选择、IDE 工作区、真实服务和浏览器验收见
[贡献指南](CONTRIBUTING.md)；正式发布还需通过无替换的依赖校验，见
[发布流程](RELEASING.md)。

### 测试

单元测试无需外部服务。服务集成测试使用 integration 标签和显式临时 fixture，跳过测试不算通过验收。
命令和环境变量统一维护在[贡献指南](CONTRIBUTING.md#service-integrations)。

## 许可证

采用 [MIT 许可证](LICENSE)。每个模块目录均保留许可证副本，供独立模块归档使用。
