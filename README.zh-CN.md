# servex-contrib

[English](README.md) | 简体中文

[servex](https://github.com/nostosx/servex) 契约的具体实现，包括后端适配器、编解码器、
节点选择算法、遥测后端与中间件。核心保留契约和共享运行机制；仅依赖标准库或 kitx 的
具体实现同样放在本仓库。

2026-10-29 的配套发布使用 servex v1.4.0 核心契约。下表已发布模块均通过公开
Go 代理解析依赖，无需本地源码替换。API 迁移见核心 BACKEND-MIGRATION.md。

2026-10-28 发布 middleware v1.3.0、transport/grpcx v1.2.0，以及 auth/oidc、di/do、
ratelimit/redis 的首个 v1.0.0。有核心依赖的模块使用 servex v1.3.0；auth/oidc 不依赖核心。
其他已发布模块保留各自版本及声明的核心依赖，具体见 CHANGELOG。上述已发布版本的依赖通过 Go 模块代理下载，
独立检出即可，无需同级源码或本地替换。
可运行应用见独立的 [servex-examples](https://github.com/nostosx/servex-examples) 项目。

middleware 模块自 `middleware/v1.1.0` 起提供类型化 CQRS 日志、指标与追踪，
用法见[类型化应用中间件](middleware/README.md#typed-application-middleware)。
各模块的破坏性改动及迁移方式见 CHANGELOG 中该模块的版本小节。

## 模块

下表每一行都是独立的 Go 模块，路径为 `github.com/nostosx/servex-contrib/<目录>`。
模块使用 servex 的公开 API，并可显式依赖 JSON、selector、Noop 遥测等基础实现。
依赖保持单向，避免引入无关后端 SDK；互操作测试可以显式组合多个实现。

| 模块 | 版本 | 实现 | 主要依赖 |
|---|---|---|---|
| [`cache/loader`](cache/loader) | v1.0.0 | 显式 Codec、owner 共享执行与写回策略的缓存加载器 | 标准库、core、kitx/singleflight |
| [`transport/client`](transport/client) | v1.0.0 | HTTP/gRPC 复用的发现、选择与调用引擎 | 标准库、core、kitx/backoff |
| [`transport/httpx`](transport/httpx) | v1.1.0 | HTTP 服务端与客户端 | net/http、JSON 编解码器、selector |
| [`tracing/noop`](tracing/noop) | v1.0.0 | `tracing.Tracer` / `Propagator` 空操作后端 | 标准库、core |
| [`metrics/noop`](metrics/noop) | v1.0.0 | `metrics.Meter` 空操作后端 | 标准库、core |
| [`jobqueue/memory`](jobqueue/memory) | v1.0.0 | 内存任务队列 `jobqueue.Enqueuer` / `Runner` | 标准库、core、kitx |
| [`encoding/json`](encoding/json) | v1.0.0 | JSON `encoding.Codec` | encoding/json/v2 |
| [`discovery/memory`](discovery/memory) | v1.0.0 | 内存注册与发现 | 标准库、core |
| [`database/sql`](database/sql) | v1.0.0 | database/sql 追踪、指标、生命周期与就绪检查 | 标准库、core、Noop 遥测 |
| [`cache/memory`](cache/memory) | v1.0.0 | 内存 `cache.Cache` | kitx/ttlcache |
| [`broker/memory`](broker/memory) | v1.0.0 | 内存发布与订阅 | 标准库、core、kitx |
| [`auth/jwt`](auth/jwt) | v1.0.0 | JWT 签发与类型化验证，供应用鉴权回调使用 | `github.com/golang-jwt/jwt/v5` |
| [`auth/oauth2`](auth/oauth2) | v1.0.0 | PKCE 授权码流程、浏览器绑定的一次性 state、刷新及客户端凭证 | Go 官方 `golang.org/x/oauth2` |
| [`auth/oauth2redis`](auth/oauth2redis) | v1.0.0 | 与 Session 共用 Redis 连接池的原子 OAuth2 state 存储 | Redis 官方 `github.com/redis/go-redis/v9` |
| [`auth/login`](auth/login) | v1.0.0 | OAuth2/OIDC 或密码登录、Session 轮换及统一业务身份 | OAuth2、OIDC、Session 及 auth 中间件 |
| [`lifecycle`](lifecycle) | v1.0.0 | 显式资源所有权、装配失败清理及逆序关闭 | 标准库、core |
| [`auth/password`](auth/password) | v1.0.0 | Argon2id 密码哈希、受限参数验证及升级判断 | Go 官方 `golang.org/x/crypto/argon2` |
| [`storage/s3`](storage/s3) | v1.0.0 | 对象上传下载、删除、元数据、分页及预签名 | AWS 官方 Go SDK v2 |
| [`scheduler/cron`](scheduler/cron) | v1.0.0 | 进程内周期任务、取消、重叠抑制与应用生命周期 | 社区 `github.com/robfig/cron/v3` |
| [`database/goose`](database/goose) | v1.0.0 | 显式 SQL 版本迁移、状态及 PostgreSQL 验收 | 社区 `github.com/pressly/goose/v3` |
| [`coordination/etcd`](coordination/etcd) | v1.0.0 | 租约锁、选主及 fencing revision | etcd 官方 concurrency SDK |
| [`transport/cors`](transport/cors) | v1.0.0 | 显式来源策略、凭证配置与预检处理 | 社区 `github.com/rs/cors` |
| [`session`](session) | v1.0.0 | 自有服务端 Session、安全 Cookie、原子 Token 轮换及有界内存存储 | 标准库 |
| [`session/redis`](session/redis) | v1.0.0 | 共享 Session 存储、原子版本校验与轮换、TTL；支持单机/Sentinel | Redis 官方 `github.com/redis/go-redis/v9` |
| [`database/gorm`](database/gorm) | v1.0.0 | GORM 原生操作观测、生命周期与健康检查，包含 PostgreSQL 验收 | `gorm.io/gorm` |
| [`auth/oidc`](auth/oidc) | v1.0.0 | OIDC 发现、ID Token 签名与 JWKS 验证，供应用鉴权回调使用 | `github.com/coreos/go-oidc/v3` |
| [`broker/kafka`](broker/kafka) | v1.2.0 | Apache Kafka 上的 `broker.Publisher`、`broker.Subscriber` | `github.com/twmb/franz-go` |
| [`broker/nats`](broker/nats) | v1.2.0 | NATS JetStream 上的发布与订阅 | `github.com/nats-io/nats.go` |
| [`broker/rabbitmq`](broker/rabbitmq) | v1.2.1 | RabbitMQ（AMQP 0-9-1）上的发布与订阅 | `github.com/rabbitmq/amqp091-go` |
| [`cache/redis`](cache/redis) | v1.1.0 | 单机、哨兵或集群 Redis 上的 `cache.Cache` | `github.com/redis/go-redis/v9` |
| [`config/consul`](config/consul) | v1.2.0 | Consul KV 上的 `config.Source`、`config.Watchable` | `github.com/hashicorp/consul/api` |
| [`config/etcd`](config/etcd) | v1.2.0 | etcd 上的配置读取与监听 | `go.etcd.io/etcd/client/v3` |
| [`config/nacos`](config/nacos) | v1.2.0 | Nacos 配置服务上的读取与监听 | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`config/viper`](config/viper) | v1.0.1 | `config.Config`, file/env sources, snapshots and validation | `github.com/spf13/viper` |
| [`database/mongodb`](database/mongodb) | v1.2.0 | MongoDB 驱动的追踪、指标、生命周期与健康检查 | `go.mongodb.org/mongo-driver/v2` |
| [`di/do`](di/do) | v1.2.0 | 基于 samber/do 实现 `di.Container`，接入生命周期与就绪检查，提供集中装配示例 | `github.com/samber/do/v2` |
| [`discovery/consul`](discovery/consul) | v1.2.0 | Consul agent 上的 `discovery.Registrar`、`discovery.Discoverer` | `github.com/hashicorp/consul/api` |
| [`discovery/etcd`](discovery/etcd) | v1.2.0 | 基于 etcd lease 的注册与发现 | `go.etcd.io/etcd/client/v3` |
| [`discovery/kubernetes`](discovery/kubernetes) | v1.2.0 | 基于 EndpointSlice 的服务发现 | `k8s.io/client-go` |
| [`discovery/nacos`](discovery/nacos) | v1.2.0 | Nacos naming 服务上的注册与发现 | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`encoding/proto`](encoding/proto) | v1.1.0 | 名为 `proto` 的 `encoding.Codec` | `google.golang.org/protobuf` |
| [`encoding/yaml`](encoding/yaml) | v1.2.0 | 名为 `yaml` 的 `encoding.Codec` | `go.yaml.in/yaml/v3` |
| [`jobqueue/asynq`](jobqueue/asynq) | v1.3.0 | asynq 上的 `jobqueue.Enqueuer`、`jobqueue.Runner` | `github.com/hibiken/asynq` |
| [`logger/slog`](logger/slog) | v1.0.0 | `logger.Logger` | `log/slog` |
| [`logger/zap`](logger/zap) | v1.0.0 | `logger.Logger` | `go.uber.org/zap` |
| [`metrics/otel`](metrics/otel) | v1.2.0 | OpenTelemetry 上的 `metrics.Meter` | `go.opentelemetry.io/otel/metric` |
| [`metrics/prometheus`](metrics/prometheus) | v1.2.0 | Prometheus 上的 `metrics.Meter` | `github.com/prometheus/client_golang` |
| [`middleware`](middleware) | v1.5.0 | 鉴权、熔断、日志、指标、限流、恢复、重试、超时、追踪与校验 | 标准库、servex、kitx、Noop 遥测 |
| [`ratelimit/rate`](ratelimit/rate) | v1.0.0 | 有 key 数量上限的本地令牌桶，供通用限流中间件使用 | `golang.org/x/time/rate` |
| [`ratelimit/redis`](ratelimit/redis) | v1.0.0 | 多副本共享的 Redis 原子令牌桶，支持 context、等待提示和后端错误 | `github.com/redis/go-redis/v9` |
| [`selector`](selector) | v1.2.0 | 加权随机、平滑加权轮询与 P2C 节点选择 | 标准库、servex、kitx |
| [`tracing/otel`](tracing/otel) | v1.1.0 | OpenTelemetry 上的 `tracing.Tracer`、`tracing.Propagator` | `go.opentelemetry.io/otel/trace` |
| [`transport/grpcx`](transport/grpcx) | v1.3.0 | gRPC 服务端及支持中间件、发现和节点选择的客户端 | `google.golang.org/grpc` |
| [`transport/ssex`](transport/ssex) | v1.0.0 | SSE 事件/心跳发送及有界流式客户端 | 标准库 |
| [`transport/websocketx`](transport/websocketx) | v1.0.0 | WebSocket 握手、消息、Ping、大小限制与关闭状态 | `github.com/coder/websocket` |
| [`transport/connectx`](transport/connectx) | v1.0.0 | Connect/gRPC/gRPC-Web 类型化 unary 与流式 RPC | `connectrpc.com/connect` |
| [`validation/playground`](validation/playground) | v1.0.0 | 基于结构体标签的校验 `endpoint.Middleware` | `github.com/go-playground/validator/v10` |
| [`validation/protovalidate`](validation/protovalidate) | v1.0.0 | 基于 Protobuf schema/CEL 的校验 `endpoint.Middleware` | `buf.build/go/protovalidate` |

校验 SDK 和 x/time/rate 仅由对应的可选模块持有，core 与通用 middleware
保持现有依赖边界。校验适配器显式接收已配置的 SDK validator；本地限流器传入
`middleware/ratelimit.New` 组合使用。通用 middleware 中不依赖 SDK 的请求自身校验
和令牌桶实现继续保留。

构造函数显式接收依赖。适配器接收调用方创建的 SDK 客户端时，除非 API 另有说明，
其所有权仍由调用方保留。错误通过 `errorx` 分类：不可达后端通常为可重试的
`Unavailable`，参数错误为 `InvalidArgument`，关闭后的调用为不可重试的
`FailedPrecondition`。契约实现均有编译期接口断言。

## 版本

auth/oidc、di/do、ratelimit/redis 自 v1.0.0 起提供；gRPC 流式中间件自
transport/grpcx v1.2.0、`ratelimit.NewContext` 自 middleware v1.3.0 起提供。
流式调用需要显式配置 `WithStreamMiddleware` / `WithClientStreamMiddleware`，不会自动重放消息。
OIDC 模块验证 ID Token，不替代任意 OAuth Access Token 的验证或 introspection。

现有已发布模块的首个正式版本均为 v1.0.0；本轮后端与协议适配器已按下表版本发布。各模块独立发布，标签包含完整目录前缀，
例如 `broker/kafka/v1.0.0` 或 `middleware/v1.0.0`：

在应用自身的 Go 模块中执行 `go get`。新应用可以从以下步骤开始：

```sh
mkdir servex-demo
cd servex-demo
go mod init example.com/servex-demo
go get github.com/nostosx/servex-contrib/cache/redis@v1.1.0
```

从模块表选择所需实现，将对应 Usage 中的完整示例保存为 `main.go`，
准备好示例依赖的后端后，在应用目录执行 `go mod tidy` 和 `go run .`。
本仓库根目录没有 `go.mod`，不要在这里执行这些应用命令。
一个模块展示多个完整程序时，应将每个程序放在独立目录中。

各模块发布版本选用其 CHANGELOG 小节注明的 servex 版本，无需本地替换。根目录的 `v1.0.0` 标签
不会发布这些嵌套模块。目录前缀用于 Git 标签，`go get` 的版本参数仍是
`@v1.0.0`。CI 负责验证，不创建标签或发布版本。
Go 1.27.3 是工具链要求，与模块版本独立。

## 运行示例

Usage 示例使用本地开发地址。运行前应启动专用的临时后端；安装 Go 模块
不会启动服务。各模块 README 说明所需数据、stream 或集群配置。
启动服务器、监听配置的程序会持续运行，按 Ctrl+C 退出。
middleware 和 selector 示例只展示装配，不会启动服务或发起请求。
OpenTelemetry 指标示例需要应用先安装 SDK provider 才会记录数据。

示例省略生产环境的凭证、TLS 和访问策略。连接远程后端前，请在传入的
SDK 客户端上完成这些配置。监听所有网卡的服务器用于本地测试时，应限制
监听地址或网络访问。服务发现必须公布调用方可达的地址；回环地址只适合
同一主机上的客户端。

## 应用组合

应用装配集中在 `app.go`，业务保留普通 Go 构造函数。`auth/login` 组合浏览器
登录和 Session，通过 `middleware/auth.FromContext[P]` 读取统一业务身份。
`auth/oauth2redis`、`session/redis` 和缓存可借用同一个 Redis 客户端，分别使用
独立 key 前缀。`database/gorm.OpenPostgres` 的 `GORM()` 与 `SQL()` 共用一个
连接池；Goose 使用同一个 `SQL()` 执行显式迁移，不在每个副本启动时自动建表。

`lifecycle.Scope` 显式管理 SDK 资源，构造或启动失败也会逆序清理；借用的
客户端仍由调用方关闭。`middleware/profile` 将日志、追踪和指标配置用于原生
HTTP、gRPC unary、完整 stream 及 worker，各作用范围显式添加鉴权和执行策略。
`di/do.ProvideConstructors` 直接注册普通构造函数；S3 列表返回 Store 相对 key，
可以直接用于 Get/Delete。完整应用见
[servex-examples](https://github.com/nostosx/servex-examples)。

组合模块使用已发布依赖。运行
`MODULES='auth/login auth/oauth2redis' task check lint vulncheck` 即可验证，
无需相邻仓库或源码替换。

## 开发

HTTP/gRPC、编解码与常用中间件的基准结果、profile 发现、测量范围和复跑命令见
[性能测量报告](PERFORMANCE.md)。
后续四项优化的行为回归、分配变化和前后对比见[优化结果](PERFORMANCE-OPTIMIZATION.md)。

使用 Go 1.27.3、Task v3.53.1 和 Bash。已发布版本通过 Go 模块代理解析依赖；
本轮配套版本无需本地源码替换。

根目录不是 Go 模块。在根目录运行下列命令，Task 会逐模块执行并设置 `GOWORK=off`：

```sh
task check
task lint
task vulncheck
```

`check` 检查格式、现代化和 tidy 差异，覆盖默认及 `integration` 构建的 vet、构建，
检查 middleware、selector、cache/loader、transport/client 的依赖边界，并运行随机顺序 race 测试。
`lint` 和漏洞扫描也覆盖两种构建。`task fmt`、`task fix`、`task tidy` 会修改文件。
通过 `MODULES` 选择模块：

```sh
MODULES='cache/redis' task check lint vulncheck
MODULES='middleware selector' task check
```

工具安装、依赖边界和测试要求见 [CONTRIBUTING.md](CONTRIBUTING.md)，
变更见 [CHANGELOG.md](CHANGELOG.md)，模块发布见 [RELEASING.md](RELEASING.md)，
私密报告见 [SECURITY.md](SECURITY.md)。

应用可以直接用 `go get` 安装所需模块，无需本地替换。各模块 README 介绍对应的公开 API。

### 测试

单元测试使用进程内 fixture，无需外部服务，包括 kfake、内嵌 NATS、内嵌 etcd、
miniredis、client-go fake、httptest 与 OpenTelemetry 内存 exporter。

服务集成测试放在带 `integration` 构建标签的 `*_integration_test.go` 中，
通过环境变量读取地址，并创建、清理自己的 topic、stream、key 或命名空间。
直接运行带标签的 Go 测试会跳过未配置的服务，成功退出不能证明集成测试实际执行。

| 变量 | 模块 | 示例值 |
|---|---|---|
| `SERVEX_IT_KAFKA_BROKERS` | `broker/kafka` | `127.0.0.1:9092`，多个地址以逗号分隔 |
| `SERVEX_IT_NATS_URL` | `broker/nats` | `nats://127.0.0.1:4222`，启用 JetStream |
| `SERVEX_IT_RABBITMQ_URL` | `broker/rabbitmq` | `amqp://guest:guest@127.0.0.1:5672/` |
| `SERVEX_IT_REDIS_ADDR` | `cache/redis`、`jobqueue/asynq`、`ratelimit/redis`、`session/redis`、`auth/oauth2redis` | `127.0.0.1:6379` |
| `SERVEX_IT_CONSUL_ADDR` | `config/consul`、`discovery/consul` | `127.0.0.1:8500` |
| `SERVEX_IT_ETCD_ENDPOINTS` | `config/etcd`、`discovery/etcd`、`coordination/etcd` | `127.0.0.1:2379`，多个地址以逗号分隔 |
| `SERVEX_IT_NACOS_ADDR` | `config/nacos`、`discovery/nacos` | `127.0.0.1:8848`，HTTP API |
| `SERVEX_IT_NACOS_GRPC_PORT` | `config/nacos`、`discovery/nacos` | 可选，默认为 HTTP 端口加 1000 |
| `SERVEX_IT_MONGODB_URI` | `database/mongodb` | `mongodb://127.0.0.1:27017` |
| `SERVEX_IT_POSTGRES_DSN` | `database/gorm`、`database/goose` | `host=127.0.0.1 port=5432 user=servex password=servex dbname=servex_it sslmode=disable` |
| `SERVEX_IT_S3_ENDPOINT` | `storage/s3` | `http://127.0.0.1:9000`（专用 MinIO 测试实例，见模块文档） |
| `SERVEX_IT_KUBECONFIG` | `discovery/kubernetes` | kubeconfig 文件路径 |

准备独立的临时服务后运行，例如：

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='cache/redis' task test INTEGRATION=true
```

Task 不启动服务。集成入口会拒绝缺少配置、缺失测试、跳过或失败的结果。
没有服务集成测试的模块会被略过；所选模块全部没有集成测试时，入口报错。
`task check` 仍是独立的单元与静态检查入口。

[CI 工作流](.github/workflows/ci.yml) 包含所有模块的检查矩阵、独立 lint 与漏洞扫描、
9 组临时 Docker 服务任务，以及 Kubernetes kind 任务。
矩阵检查会核对模块及集成测试目录；服务任务等待就绪后只运行 `TestIntegration*`，
拒绝缺失、跳过、失败或未完成的结果，并清理服务容器或 kind 集群。

各任务独立运行，支持 pull request、main 推送与手动触发。任务只检出本仓库，
发布前需要刷新配套依赖版本及校验和。单元检查通过不能替代真实服务的集成验证。

## 许可证

采用 [MIT 许可证](LICENSE)。每个模块目录均保留许可证副本，供独立模块归档使用。

配置、日志及后端迁移使用公开发布的 servex v1.4.0。本轮从核心迁出九个后端模块，
加权随机选择加入现有 selector 模块的 `selector/random` 包。第二批新增 cache/loader 和 transport/client 两个模块，承载 Loader 与共享客户端引擎。
App、health.Group、注册生命周期与轻量 Consumer/Worker 适配明确保留在核心。API 变更见核心的 `BACKEND-MIGRATION.md`。
