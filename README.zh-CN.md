# servex-contrib

[English](README.md) | 简体中文

[servex](https://github.com/nostosx/servex) 的扩展实现，包含依赖第三方 SDK 的
服务适配器，以及不依赖第三方 SDK 的 `middleware`、`selector` 两个模块。
核心保留契约及参考实现，本仓库按依赖拆分其他实现。

各模块尚未发布，当前通过相对 `replace` 使用本地 servex。
kitx 使用已发布的 v1.0.0，由 Go 模块代理下载，无需本地检出。
可运行应用见独立的 [servex-examples](https://github.com/nostosx/servex-examples) 项目。

## 模块

下表每一行都是独立的 Go 模块，路径为 `github.com/nostosx/servex-contrib/<目录>`。
模块只使用 servex 的公开 API，不互相导入；选择一个适配器不会引入其他适配器的 SDK。

| 模块 | 实现 | 主要依赖 |
|---|---|---|
| [`broker/kafka`](broker/kafka) | Apache Kafka 上的 `broker.Publisher`、`broker.Subscriber` | `github.com/twmb/franz-go` |
| [`broker/nats`](broker/nats) | NATS JetStream 上的发布与订阅 | `github.com/nats-io/nats.go` |
| [`broker/rabbitmq`](broker/rabbitmq) | RabbitMQ（AMQP 0-9-1）上的发布与订阅 | `github.com/rabbitmq/amqp091-go` |
| [`cache/redis`](cache/redis) | 单机、哨兵或集群 Redis 上的 `cache.Cache` | `github.com/redis/go-redis/v9` |
| [`config/consul`](config/consul) | Consul KV 上的 `config.Source`、`config.Watchable` | `github.com/hashicorp/consul/api` |
| [`config/etcd`](config/etcd) | etcd 上的配置读取与监听 | `go.etcd.io/etcd/client/v3` |
| [`config/nacos`](config/nacos) | Nacos 配置服务上的读取与监听 | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`database/mongodb`](database/mongodb) | MongoDB 驱动的追踪、指标、生命周期与健康检查 | `go.mongodb.org/mongo-driver/v2` |
| [`discovery/consul`](discovery/consul) | Consul agent 上的 `discovery.Registrar`、`discovery.Discoverer` | `github.com/hashicorp/consul/api` |
| [`discovery/etcd`](discovery/etcd) | 基于 etcd lease 的注册与发现 | `go.etcd.io/etcd/client/v3` |
| [`discovery/kubernetes`](discovery/kubernetes) | 基于 EndpointSlice 的服务发现 | `k8s.io/client-go` |
| [`discovery/nacos`](discovery/nacos) | Nacos naming 服务上的注册与发现 | `github.com/nacos-group/nacos-sdk-go/v2` |
| [`encoding/proto`](encoding/proto) | 名为 `proto` 的 `encoding.Codec` | `google.golang.org/protobuf` |
| [`encoding/yaml`](encoding/yaml) | 名为 `yaml` 的 `encoding.Codec` | `go.yaml.in/yaml/v3` |
| [`jobqueue/asynq`](jobqueue/asynq) | asynq 上的 `jobqueue.Enqueuer`、`jobqueue.Runner` | `github.com/hibiken/asynq` |
| [`metrics/otel`](metrics/otel) | OpenTelemetry 上的 `metrics.Meter` | `go.opentelemetry.io/otel/metric` |
| [`metrics/prometheus`](metrics/prometheus) | Prometheus 上的 `metrics.Meter` | `github.com/prometheus/client_golang` |
| [`middleware`](middleware) | 鉴权、熔断、日志、指标、限流、恢复、重试、超时、追踪与校验 | 标准库、servex、kitx |
| [`selector`](selector) | 平滑加权轮询与 P2C 节点选择 | 标准库、servex、kitx |
| [`tracing/otel`](tracing/otel) | OpenTelemetry 上的 `tracing.Tracer`、`tracing.Propagator` | `go.opentelemetry.io/otel/trace` |
| [`transport/grpcx`](transport/grpcx) | gRPC 服务端及支持中间件、发现和节点选择的客户端 | `google.golang.org/grpc` |

构造函数显式接收依赖。适配器接收调用方创建的 SDK 客户端时，除非 API 另有说明，
其所有权仍由调用方保留。错误通过 `errorx` 分类：不可达后端通常为可重试的
`Unavailable`，参数错误为 `InvalidArgument`，关闭后的调用为不可重试的
`FailedPrecondition`。契约实现均有编译期接口断言。

## 版本

21 个模块的首个正式版本均为 v1.0.0，尚未发布。各模块独立发布，
标签包含完整目录前缀，例如 `broker/kafka/v1.0.0` 或
`middleware/v1.0.0`。以下安装命令仅在对应版本发布后可用：

```sh
go get github.com/nostosx/servex-contrib/broker/kafka@v1.0.0
```

目前各模块仍依赖 servex 的占位版本并保留本地替换：

```text
require github.com/nostosx/servex v0.0.0-00010101000000-000000000000
replace github.com/nostosx/servex => ../../../servex   # middleware 和 selector 使用 ../../servex
```

发布 contrib 前，须先选择已发布的 servex 版本、移除对应替换，并验证 kitx 依赖。
根目录的 `v1.0.0` 标签不会发布这些嵌套模块。CI 不创建标签或发布版本。
Go 1.27.3 是工具链要求，与模块版本独立。

## 开发

使用 Go 1.27.3、GNU Make、Bash 和 Python 3，并将项目放在同一级：

```text
workspace/
├── servex/
└── servex-contrib/
```

根目录不是 Go 模块。在根目录运行下列命令，Make 会逐模块执行并设置 `GOWORK=off`：

```sh
make check
make lint
make vulncheck
```

`check` 检查格式、现代化和 tidy 差异，覆盖默认及 `integration` 构建的 vet、构建，
检查 middleware、selector 的依赖边界，并运行随机顺序 race 测试。
`lint` 和漏洞扫描也覆盖两种构建。`make fmt`、`make fix`、`make tidy` 会修改文件。
通过 `MODULES` 选择模块：

```sh
MODULES='cache/redis' make check lint vulncheck
MODULES='middleware selector' make check
```

工具安装、依赖边界和测试要求见 [CONTRIBUTING.md](CONTRIBUTING.md)，
变更见 [CHANGELOG.md](CHANGELOG.md)，模块发布见 [RELEASING.md](RELEASING.md)，
私密报告见 [SECURITY.md](SECURITY.md)。

使用未发布模块的应用也必须声明替换，因为 Go 不继承依赖模块中的 `replace`。
假设应用与两个仓库同级：

```sh
go mod edit -require=github.com/nostosx/servex-contrib/cache/redis@v0.0.0-00010101000000-000000000000
go mod edit -replace=github.com/nostosx/servex-contrib/cache/redis=../servex-contrib/cache/redis
go mod edit -replace=github.com/nostosx/servex=../servex
go mod tidy
```

按实际目录替换所用的每个 contrib 模块及 core；kitx v1.0.0 会自动下载。
各模块 README 中的示例对应当前本地 API。

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
| `SERVEX_IT_REDIS_ADDR` | `cache/redis`、`jobqueue/asynq` | `127.0.0.1:6379` |
| `SERVEX_IT_CONSUL_ADDR` | `config/consul`、`discovery/consul` | `127.0.0.1:8500` |
| `SERVEX_IT_ETCD_ENDPOINTS` | `config/etcd`、`discovery/etcd` | `127.0.0.1:2379`，多个地址以逗号分隔 |
| `SERVEX_IT_NACOS_ADDR` | `config/nacos`、`discovery/nacos` | `127.0.0.1:8848`，HTTP API |
| `SERVEX_IT_NACOS_GRPC_PORT` | `config/nacos`、`discovery/nacos` | 可选，默认为 HTTP 端口加 1000 |
| `SERVEX_IT_MONGODB_URI` | `database/mongodb` | `mongodb://127.0.0.1:27017` |
| `SERVEX_IT_KUBECONFIG` | `discovery/kubernetes` | kubeconfig 文件路径 |

准备独立的临时服务后运行，例如：

```sh
SERVEX_IT_REDIS_ADDR=127.0.0.1:6379 MODULES='cache/redis' make integration
```

Make 不启动服务。集成入口会拒绝缺少配置、缺失测试、跳过或失败的结果。
没有服务集成测试的模块会被略过；所选模块全部没有集成测试时，入口报错。
`make check` 仍是独立的单元与静态检查入口。

[CI 工作流](.github/workflows/ci.yml) 包含 21 模块检查矩阵、独立 lint 与漏洞扫描、
8 组临时 Docker 服务任务，以及 Kubernetes kind 任务。
矩阵检查会核对模块及集成测试目录；服务任务等待就绪后只运行 `TestIntegration*`，
拒绝缺失、跳过、失败或未完成的结果，并清理服务容器或 kind 集群。

各任务独立运行，支持 pull request、main 推送与手动触发。
手动触发可选择 `servex-ref`，否则使用 core 默认分支；这是同级源码联调，
不能作为已发布版本兼容性的证明。目前工作流尚未在 GitHub 上执行。
runner 必须能读取 servex；私有仓库还需要相应凭据。
容器拉取、服务就绪和 kind 执行须在远程 CI 中验证。

## 许可证

采用 [MIT 许可证](LICENSE)。每个模块目录均保留许可证副本，供独立模块归档使用。
