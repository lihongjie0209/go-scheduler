# Go Scheduler

基于 Go、PostgreSQL 和 gRPC 的多租户 HTTP 定时任务平台后端。默认以单二进制模式运行，提供任务 CRUD、Cron/单次/固定间隔调度、手动触发、运行记录、执行租约和失败重试。

## 组件

- `scheduler server`：默认部署入口；REST API 与调度 Core 位于同一进程，通过内存 gRPC 通信，不需要 etcd；同时监听内部 gRPC 端口供 Executor 注册和回报。
- `scheduler api-server` / `scheduler core`：分布式部署入口；Kubernetes 内可使用无头 Service + gRPC DNS 服务发现，集群外可使用 etcd。
- `scheduler executor`：脚本、HTTP、Docker 与 Kubernetes Job 执行器。
- `scheduler migrate` / `scheduler bootstrap`：数据库迁移和首次初始化。
- `schedulerctl`：独立的 API 运维客户端，支持账号密码、JWT 和 API Key 认证。发行包只包含 `scheduler` 与 `schedulerctl` 两个二进制。
- PostgreSQL：任务和运行状态的唯一事实来源。
- etcd：仅在集群外选择分布式入口时用于服务注册与发现；Kubernetes 部署不需要 etcd。

## 本地启动

默认模式只需要 PostgreSQL。所有进程使用环境变量配置：

```bash
export DATABASE_URL='postgres://postgres:postgres@127.0.0.1:5432/scheduler?sslmode=disable'
export SERVICE_TOKEN='replace-with-a-long-random-token'
export MASTER_KEY="$(openssl rand -base64 32)"
export JWT_SECRET="$(openssl rand -base64 48)"

make migrate-up
TENANT_NAME=demo ADMIN_EMAIL=admin@example.com ADMIN_PASSWORD='replace-with-strong-password' go run ./cmd/scheduler bootstrap

HTTP_ADDRESS=:8080 go run ./cmd/scheduler server
```

项目不包含 Web UI，根路径返回 404；管理和运维使用 `schedulerctl` 或 `/api/v1` REST API。`make build` 构建统一的 `scheduler`、独立的 `schedulerctl` 和压测工具。

`bootstrap` 只显示一次 API Key。调用 REST API 时使用 `Authorization: Bearer <api_key>`。

创建每 10 秒执行一次的任务：

```bash
curl -X POST http://127.0.0.1:8080/api/v1/jobs \
  -H "Authorization: Bearer $API_KEY" \
  -H 'Content-Type: application/json' \
  -d '{"name":"demo","schedule_type":"cron","schedule_expression":"*/10 * * * * *","timezone":"Asia/Shanghai","target_url":"https://example.com/hook","http_method":"POST","enabled":true}'
```

## 关键配置

| 环境变量 | 默认值 | 说明 |
|---|---:|---|
| `DATABASE_URL` | 必填 | PostgreSQL DSN |
| `SERVICE_TOKEN` | 必填 | 内存或网络 gRPC 的 API/Core 共享令牌 |
| `MASTER_KEY` | 必填 | Base64 编码的 32 字节 AES-GCM Header 加密主密钥 |
| `MASTER_KEY_VERSION` | `1` | 主密钥版本号 |
| `JWT_SECRET` | 必填 | 至少 32 字节的本地账号 JWT 签名密钥 |
| `COOKIE_SECURE` | `true` | Refresh Cookie 是否仅通过 HTTPS 发送；仅本地 HTTP 开发设置为 `false` |
| `PREVIOUS_SERVICE_TOKEN` | 空 | 轮换期间兼容旧令牌 |
| `GRPC_TLS_CERT` / `GRPC_TLS_KEY` | 空 | Core/standalone gRPC 服务端证书和私钥；必须同时配置 |
| `GRPC_TLS_CA` / `GRPC_TLS_SERVER_NAME` | 空 | API Server 连接 Core 时使用的 CA 和证书名称 |
| `EXECUTOR_GRPC_TLS_CA` / `EXECUTOR_GRPC_TLS_SERVER_NAME` | 空 | Core 连接 Executor 时使用的 CA 和证书名称 |
| `HTTP_ADDRESS` | `:8080` | API 监听地址 |
| `API_CONTEXT_PATH` | 空 | API URL 前缀，例如 `/scheduler`；健康检查、指标、REST、回调和执行器注册均使用此前缀 |
| `PUBLIC_BASE_URL` | `http://127.0.0.1:8080` | HTTP 任务异步回调使用的公开 API 地址 |
| `DISCOVERY_MODE` | `etcd` | 分布式 API/Core 的服务发现模式：`etcd` 或 `kubernetes`；单进程 `server` 不使用该配置 |
| `CORE_GRPC_TARGET` | `dns:///scheduler-core:9090` | Kubernetes 模式下 API 和 Executor 连接 Core 无头 Service 的 gRPC DNS target |
| `WORKERS` | `64` | 单 Core 最大并发下发数；执行任务本身在 Executor 运行，不长期占用该槽位 |
| `API_DATABASE_MAX_CONNS` / `API_DATABASE_MIN_CONNS` | `8` / `1` | API PostgreSQL 连接池上限与保底连接数 |
| `CORE_DATABASE_MAX_CONNS` / `CORE_DATABASE_MIN_CONNS` | `24` / `2` | 调度、执行状态和通知 PostgreSQL 连接池上限与保底连接数 |
| `HISTORY_RETENTION` | `2160h` | 历史记录和运行分区保留期，必须不超过 90 天 |
| `SMTP_ADDRESS` | 空 | 邮件告警 SMTP 地址，例如 `smtp.example.com:587` |
| `SMTP_USERNAME` / `SMTP_PASSWORD` / `SMTP_FROM` | 空 | SMTP 凭据和发件人 |
| `SMTP_TLS_MODE` | `starttls` | SMTP 传输模式：强制 STARTTLS、`tls` 隐式 TLS，或显式选择 `none` 明文兼容模式 |

生产环境应为 PostgreSQL、公开 HTTP API 和跨主机内部 gRPC 配置 TLS。HTTP、脚本和容器任务的网络访问不做目标地址限制。

配置 `API_CONTEXT_PATH=/scheduler` 后，API readiness 地址变为 `/scheduler/health/ready`，REST 地址变为 `/scheduler/api/v1`。`PUBLIC_BASE_URL` 和 `ADVERTISE_HTTP_ADDRESS` 可以继续填写不带前缀的服务地址，服务会自动附加 context path。`schedulerctl --server` 应填写完整 HTTP 地址。Executor 使用独立的 `SCHEDULER_GRPC_ADDRESS`，不受 HTTP context path 影响。

Executor 连接 Core/standalone 时可设置 `SCHEDULER_GRPC_TLS_CA` 和 `SCHEDULER_GRPC_TLS_SERVER_NAME`；Executor 自身的 gRPC 服务端可设置 `EXECUTOR_GRPC_TLS_CERT` 和 `EXECUTOR_GRPC_TLS_KEY`。生产跨主机部署应同时启用两个方向的 TLS。

单进程模式的 API/Core 调用使用 `bufconn` 内存传输，同时监听内部 gRPC 端口供 Executor 注册、回报和接收调度。需要 API/Core 独立扩缩容时可继续使用分布式入口：集群外设置 `DISCOVERY_MODE=etcd`；Kubernetes 内设置 `DISCOVERY_MODE=kubernetes`，API 和 Executor 使用 `dns:///scheduler-core:9090`，由无头 Service 只发布 ready Core Pod，并由 gRPC `round_robin` 动态更新连接。Kubernetes 模式不访问 Kubernetes API、不需要 ServiceAccount RBAC，也不连接 etcd。动态 Executor 心跳直接写入 PostgreSQL TTL 投影；多 Core 仍依靠 PostgreSQL 行锁避免重复执行。参考 [`deploy/kubernetes`](deploy/kubernetes) 清单。

`pg_partman` 是可选增强项。迁移会检测扩展是否存在且当前账号是否可安装：可用时配置月分区并由服务每小时调用 pg_partman maintenance；不可用时，迁移先创建近 90 天及未来 3 个月的月分区，Scheduler 再通过 PostgreSQL advisory lock 每小时续建并删除无活跃运行的过期分区。多个服务实例可以共享同一普通 PostgreSQL，不需要超级用户或 pg_partman。

## Go Executor SDK

`pkg/executor` 可将 Go 函数注册为命名 handler。Core 通过 gRPC `Dispatch` 下发并立即释放调度 worker；Executor 异步运行 handler，通过 Scheduler gRPC 回报 Rolling 日志和最终状态，并提供幂等下发、取消、状态查询和 TTL 注册。最终状态遇到瞬时 gRPC 故障时会在 2 分钟总时限内指数退避重试；认证、权限、参数或已消费 token 等永久错误不会重试。执行器仅保留最近 24 小时、最多 10,000 条终态幂等记录，活跃任务不参与清理。外部系统的 HTTP 异步 callback 统一由 API Server 接收，再通过 gRPC 转交 Core 状态机。

```go
server, _ := executor.NewServer(executor.Options{SchedulerURL: "http://scheduler.invalid"})
_ = server.Handle("invoiceHandler", func(ctx context.Context, task executor.Task) error {
    return task.Logger.Info("processing " + task.Input)
})
// 将 server 包装成 executor.NewGRPCServer 并注册到 grpc.Server。
```

完整的 handler 与心跳注册示例见 [`pkg/executor`](pkg/executor)。

## Script Executor

Shell、Python、Node.js、PHP 与 PowerShell 脚本由独立 `script-executor` 执行，Scheduler Core 不加载解释器。脚本以版本化任务字段 `script_language`、`script_source` 保存，执行器使用固定解释器和 0700 临时文件，不拼接 shell 命令；任务输入通过 `SCHEDULER_INPUT` 环境变量传入。

执行器可通过 `EXECUTOR_LABELS=linux,gpu,zone-a` 注册最多 32 个标签。任务的 `required_executor_labels` 必须全部出现在节点上，`excluded_executor_labels` 任意命中即排除；普通路由、FAILOVER/BUSYOVER 和广播分片使用同一套过滤规则。运维人员在手工触发时显式传入的执行器地址属于强制覆盖，会绕过标签约束。

```bash
docker build -f deploy/script-executor/Dockerfile -t go-scheduler-script-executor .
docker run --rm -p 9999:9999 \
  -v go-scheduler-executor-state:/var/lib/go-scheduler/executor \
  -e SCHEDULER_GRPC_ADDRESS=scheduler-server:9090 \
  -e SCHEDULER_TOKEN="$SCHEDULER_TOKEN" \
  -e EXECUTOR_GROUP_ID="$GROUP_ID" \
  -e EXECUTOR_NODE_ID=script-1 \
  -e EXECUTOR_ADVERTISE_ADDRESS=script-executor:9999 \
  go-scheduler-script-executor
```

脚本任务使用 `executor_handler: "__script__"`。默认允许 `shell,python,nodejs,php,powershell`，可通过 `SCRIPT_LANGUAGES` 收窄。Executor 按资源模型使用独立并发池：`EXECUTOR_SCRIPT_MAX_CONCURRENCY` 默认 32，`EXECUTOR_HTTP_MAX_CONCURRENCY` 默认 1000，`EXECUTOR_DOCKER_MAX_CONCURRENCY` 默认 100；旧的 `EXECUTOR_MAX_CONCURRENCY` 仅作为脚本池兼容默认值。Kubernetes 不占用节点并发池，其全局活动 Job 上限由绑定集群的 `max_concurrent_jobs` 配置并由 Core/PG 调度准入控制。任一节点池达到上限时，该类型的新任务返回 `ResourceExhausted` 并进入 Core 的失败/重试状态机，其他类型不受影响；重复派发不重复占槽。收到 SIGTERM 后执行器立即注销并拒绝新派发，在 `EXECUTOR_SHUTDOWN_TIMEOUT`（默认 30 秒）内等待已接收任务，超时则取消进程内 handler。Executor 在确认 Dispatch 前先持久化完整执行请求及首次接受时的绝对执行截止时间，执行结果也会在回报 Core 前原子落盘到 `EXECUTOR_STATE_DIR`（默认 `./executor-state`，官方镜像为 `/var/lib/go-scheduler/executor`）。重启后 Docker/Kubernetes 任务按稳定 external execution ID 重新接管且不会重置任务 timeout，shell/HTTP 等进程内任务则明确报告失败并交由 Core 重试策略处理；Core 确认完成回执后才删除状态。该目录可能包含 callback token、任务输入、Docker 仓库凭据和 Kubernetes 凭据，权限固定为 0700/0600，必须为每个 Executor 节点独占并挂载加密或受限持久卷，不得共享或备份到低信任位置；落盘失败、执行记录达到 `EXECUTOR_EXECUTION_MAX_PENDING`（默认 10,000）或完成回执积压达到 `EXECUTOR_COMPLETION_MAX_PENDING`（默认 10,000）后节点会拒绝新 Dispatch，防止静默退化或耗尽磁盘。源码及单次 stdout/stderr 总输出分别限制为 1 MiB，任务超时会终止整个脚本进程组；Linux executor 异常退出时，内核 parent-death signal 会终止解释器，避免孤儿脚本继续产生副作用。自建 Alpine 镜像固定安装 Python 3、Node.js 22、PHP 8.3 CLI 与 PowerShell 7.6.4；PowerShell 使用微软官方 Alpine tar.gz，构建时校验官方 SHA256。镜像和完整 use case Testcontainers 测试会真实执行这些运行时。

## Docker Image Executor

Docker Image 任务与脚本任务使用同一个 `script-executor`。设置 `DOCKER_ENABLED=true` 后，执行器同时注册 `__script__` 和 `__docker__`；任务定义使用 `script_language: "docker"`、`executor_handler: "__docker__"`，`script_source` 保存版本化 JSON 定义。容器使用 run ID 确定性命名并带受管标签；executor 异常重启后，同一运行重派会校验标签、等待并接管原容器，恢复日志和退出码后清理，不会启动第二个容器。名称被非本运行占用时会拒绝接管。

```json
{
  "image": "registry.example.com/jobs/reconcile:v1.4.0",
  "command": ["/app/reconcile"],
  "args": ["--once"],
  "env": {"LOG_LEVEL": "info"},
  "pull_policy": "always",
  "network": "none",
  "read_only_root": true,
  "memory_mb": 512,
  "cpus": 1
}
```

默认不附加网络、只读根文件系统、capability、PID、CPU 或内存策略，容器继承 Docker Runtime 的默认行为。可以通过 `network` 指定 `bridge`、`host` 或自定义 Docker network，通过 `read_only_root`、`memory_mb` 和 `cpus` 显式收窄单个任务。任务定义仍不提供 privileged、宿主目录挂载或 Docker socket 透传字段；Executor 本身属于可信高权限组件，应在基础设施层隔离。

私有仓库凭据可在 Docker 任务的 `docker_registry_auth` 中配置。密码使用服务端密钥环 AES-GCM 加密后存入 PostgreSQL，查询任务只返回 `server`、`username` 和 `configured`，不会回显密码。Core 仅在派发该任务时通过受认证的 gRPC 连接发送凭据；Executor 为本次 `docker pull` 创建权限为 `0600` 的临时 `config.json`，拉取结束立即删除，不执行全局 `docker login`。

```json
{
  "docker_registry_auth": {
    "server": "registry.example.com",
    "username": "robot",
    "password": "secret",
    "configured": true
  }
}
```

更新任务时省略该字段会保留原凭据；修改仓库或用户名时必须重新提交密码；设置 `clear_docker_registry_auth: true` 才会明确删除。配置凭据要求 API Server 已配置数据加密密钥，否则请求会失败。未配置任务级凭据时仍可使用 Executor 已有的 Docker 标准 `config.json`：

```bash
docker build -f deploy/script-executor/Dockerfile -t go-scheduler-executor .
docker run --rm -p 9999:9999 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "$HOME/.docker:/docker-auth:ro" \
  -v go-scheduler-executor-state:/var/lib/go-scheduler/executor \
  -e DOCKER_CONFIG=/docker-auth \
  -e DOCKER_ENABLED=true \
  -e SCHEDULER_GRPC_ADDRESS=scheduler-server:9090 \
  -e SCHEDULER_TOKEN="$SCHEDULER_TOKEN" \
  -e EXECUTOR_GROUP_ID="$GROUP_ID" \
  -e EXECUTOR_NODE_ID=executor-1 \
  -e EXECUTOR_ADVERTISE_ADDRESS=executor:9999 \
  go-scheduler-executor
```

执行器容器目前只发布 `linux/amd64`：其 Alpine 运行时包含 PowerShell，而上游仅提供 x64 的 musl 归档。独立 Go 二进制仍同时发布 amd64 和 arm64；在 PowerShell 上游提供 arm64 musl 构建前，不发布功能残缺的 arm64 执行器镜像。

Kubernetes 中也可以把共享仓库凭据以 Secret 挂载为 `/docker-auth/config.json`。挂载宿主 Docker Socket 等价于授予执行器宿主机高权限，生产环境建议使用隔离的专用 Worker 节点或远程受限 Docker Engine。

## Kubernetes Job Executor

统一执行器设置 `KUBERNETES_ENABLED=true` 后注册 `__kubernetes__` handler。集群是租户级资源，支持完整 kubeconfig，或 API Server + ServiceAccount Token + CA；凭据通过 `MASTER_KEY` 加密存入 PostgreSQL，读取 API 永不返回明文。每个集群通过 `max_concurrent_jobs`（CLI：`--max-concurrent-jobs`，默认 100）声明该集群的全局活动 Job 容量。Kubernetes 任务绑定 `kubernetes_cluster_id`，同时仍按执行器组及 `required_executor_labels` / `excluded_executor_labels` 选择能够访问目标集群的执行节点。

```json
{
  "executor_group_id": "...",
  "executor_handler": "__kubernetes__",
  "script_language": "kubernetes",
  "kubernetes_cluster_id": "...",
  "required_executor_labels": ["kubernetes", "prod-network"],
  "script_source": "{\"image\":\"registry.example.com/jobs/reconcile:v1\",\"args\":[\"--once\"],\"image_pull_secrets\":[\"registry-auth\"]}"
}
```

Kubernetes Job 名称使用整条重试链稳定不变的首个 run ID。执行器重启或连接中断不会删除 Job；重派后执行器查询并接管已有 Job，完成后补采 Pod 日志和回调。Job 默认设置 `ttlSecondsAfterFinished=86400`，由集群 TTL Controller 延迟回收。私有镜像使用目标 namespace 中已有的 `imagePullSecrets`。

## 命令行客户端

调度性能验收口径见 [调度性能基准](docs/performance-benchmark.md)，架构、性能和安全风险记录见 [代码库审计](docs/code-audit.md)。`/metrics` 提供 `scheduler_dispatch_delay_seconds` 和 `scheduler_worker_saturation_ticks_total`，用于观察运行从计划时间到 worker 开始处理的延迟以及执行槽位饱和情况；`scheduler_run_claim_duration_seconds`、`scheduler_run_claim_attempts_total` 和 `scheduler_run_claimed_total` 用于识别 PostgreSQL Claim 延迟、错误、空轮询和 Kubernetes 容量锁等待；`scheduler_executor_command_queue_pending`、`scheduler_executor_command_queue_oldest_pending_age_seconds` 和 `scheduler_executor_command_queue_collect_success` 用于监控执行器取消指令积压。正式结论仍以 HTTP sink 记录的端到端数据为准。worker 全部占用时，引擎不会提前 claim 更多运行或阻塞调度循环，到期任务入队、异步 callback 超时和维护工作仍按调度 tick 继续运行。

单进程模式内部使用两个独立 PostgreSQL 连接池：API 池处理认证、查询和控制面请求，Core 池处理调度 claim、运行状态和通知，API 流量无法耗尽 Core 的连接配额。`scheduler_database_pool_connections`、`scheduler_database_pool_empty_acquires_total` 和 `scheduler_database_pool_acquire_duration_seconds_total` 按 `pool=api|core` 输出池状态与等待压力。

配置连接池时应确保 PostgreSQL 的 `max_connections` 能覆盖所有服务实例的 API 与 Core 上限之和，并为迁移、运维和监控连接预留空间。增加池上限不能替代慢查询与长事务治理。

```bash
make build

# 账号密码认证；JWT 仅保存在本次进程内
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --email admin@example.com --password 'SchedulerDemo123!' dashboard

# API Key 或已有 JWT；API Key 自动携带所属租户
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs list

# 主动摘除自动注册的执行器；SDK 正常退出时会自动执行，异常退出由 TTL 兜底
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" executors unregister "$GROUP_ID" "$NODE_ID"

# 查询不可变脚本版本，并用任务乐观锁版本原子回滚
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs script-versions list "$JOB_ID"
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs script-versions rollback \
  "$JOB_ID" "$SCRIPT_VERSION_ID" --version "$JOB_VERSION" --remark "restore stable"

# 每日运行趋势；日期范围最多 90 天，默认最近 14 天 UTC
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" reports runs \
  --from 2026-08-01 --to 2026-08-13 --timezone Asia/Shanghai

# 分批清理指定任务的终态运行；不会删除 pending/running/waiting_callback
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" runs purge \
  --before 2026-08-01T00:00:00Z --job "$JOB_ID" --limit 1000

# 从 JSON 创建任务
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs create -f job.json

# 使用 jobs get 返回的完整定义修改任务；version 用于乐观锁
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs update "$JOB_ID" -f updated-job.json

# 原子启动/停止，不覆盖任务配置；version 来自 jobs get/list
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs start "$JOB_ID" --version 1
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs stop "$JOB_ID" --version 2

# 终止待执行、运行中或等待异步回调的运行
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" runs cancel "$RUN_ID" --reason maintenance

# 设置和查询父任务成功后的子任务
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs dependencies set "$PARENT_JOB_ID" \
  --child "$CHILD_JOB_ID_1" --child "$CHILD_JOB_ID_2"
./bin/schedulerctl --server http://127.0.0.1:18080 \
  --token "$SCHEDULER_TOKEN" jobs dependencies get "$PARENT_JOB_ID"
```

调度核心采用四层测试：`make test` 运行代码级单元测试，`make integration-module` 运行 PostgreSQL/etcd 单模块 Testcontainers 测试，`make integration-cross` 运行 PostgreSQL + Core gRPC 跨模块测试，`make integration-usecase` 运行真实 `schedulerctl` 进程到 API、gRPC、Core 和 PostgreSQL 的完整用例。etcd 测试用于保留的分布式模式，默认单进程部署不启动 etcd。`make integration` 顺序执行后三层。

任务的 `overlap_policy` 对齐 XXL-JOB 阻塞策略：`serial` 将后续运行排队，`discard_later` 将已有运行或排队时的新触发记为 `skipped`，`cover_early` 取消旧运行和队列后执行最新触发。`parallel` 是 Go Scheduler 保留的扩展策略。旧值 `queue`、`skip` 在 API 兼容期内分别规范化为 `serial`、`discard_later`。

`runs cancel` 对取消操作保持幂等；运行中的 HTTP 请求会收到 context cancellation，等待异步回调的 token 会立即失效，已经成功、失败、超时或跳过的终态不会被改写。

父任务只有在最终执行成功后才触发子任务，同步 HTTP 成功和异步 callback 成功采用相同语义。失败、取消或超时不会触发；每个父运行对每个子任务最多派发一次。依赖限制在同一租户内，并拒绝自依赖和有向环。子运行的 `trigger_type` 为 `dependency`，`parent_run_id` 可用于 CLI 链路追踪。

失败重试与 XXL-JOB 一样为每次 attempt 创建独立运行记录，旧 attempt 保持 `failed` 或 `timed_out` 终态；新记录的 `trigger_type=retry`、`attempt` 递增，并通过 `retry_of_run_id` 连接。中间失败不产生最终告警，重试预算耗尽后才写入告警 outbox。可用 `schedulerctl runs get ID` 查询单次运行。

任务可继续使用 `target_url` 直连，也可配置 `executor_group_id` 与 `executor_handler`。执行器通过 `schedulerctl executors register GROUP_ID NODE_ID --address URL --ttl 30` 周期心跳；Core 仅选择 TTL 内存活节点。组模式统一调用节点的 `POST /run`，运行记录包含 `executor_node_id` 和 `executor_address`。路由支持 FIRST、LAST、ROUND、RANDOM、HASH、LFU、LRU；HASH 与 XXL-JOB 一样使用 MD5 和每地址 100 个虚拟节点，ROUND/LFU/LRU 状态按 job 隔离并保存在 PostgreSQL，多个 Core 实例共享同一原子选路序列。

执行器组也支持 XXL-JOB 的手工地址模式：`schedulerctl executors groups create --name static --strategy first --mode manual --address http://worker-a:9999 --address http://worker-b:9999`。手工地址会规范化、去重并持久化为静态节点，不需要心跳；使用 `executors groups update ID ... --version N` 原子替换地址，使用 `executors groups delete ID --version N` 删除未被任务引用的组。

主动路由还支持 FAILOVER 和 BUSYOVER。执行器分别实现 `GET /health` 与 `POST /idle`（请求包含 `job_id`）：FAILOVER 选择首个健康节点，BUSYOVER 选择首个对该任务空闲的节点。探测按 node ID 稳定排序并逐节点限时执行，网络探测期间不持有 PostgreSQL 事务锁。

分片广播使用 `sharding_broadcast` 路由策略。每次人工、定时或依赖触发都会对当前存活节点生成独立运行，下发 `broadcast_group_id`、`broadcast_index` 和 `broadcast_total`；动态节点按 node ID、手工节点按规范化地址稳定排序，失败重试固定在原节点和原分片。可通过 `schedulerctl runs --broadcast-group ID` 查看同一批广播及其重试轨迹。

执行器派发数据还包含 `log_url` 和 `callback_token`。执行器可在运行期间向 `log_url` POST `{"token":"...","entries":[{"entry_id":"唯一且可重试","stream":"stdout","content":"..."}]}` 追加 Rolling 日志；每批最多 100 条、单条最多 64 KiB。通过 `schedulerctl runs logs RUN_ID` 分页读取，或使用 `--follow` 持续跟随。

HTTP 执行器向目标服务发送 `Idempotency-Key`、`X-Go-Scheduler-Execution-ID`、`X-Go-Scheduler-Run-ID` 和 `X-Go-Scheduler-Job-ID`。同一运行因网络中断或 Executor 重启而被重新派发时标识保持不变；业务失败产生的新 retry Run 使用新的标识，因此会真正发起下一次执行。Kubernetes Job 使用相同的单次 Run 身份恢复已有 Job，不会错误复用上一轮已失败的 Job。

周期调度支持 `fixed_rate` 与 `fixed_delay`，表达式均为正整数秒。`fixed_rate` 按原计划时刻推进；`fixed_delay` 等当前计划运行及其全部 retry/广播分片终止后，再从最终完成时刻计算下一次。旧的 `fixed_interval` 继续作为 `fixed_rate` 兼容类型。

Cron 使用包含秒字段的 6 字段表达式，并按任务配置的 IANA 时区计算。兼容 XXL-JOB 的 Quartz 日历语义：`?`、月末 `L/L-n`、最近工作日 `nW/LW/L-nW`、最后一个指定星期 `nL` 和第 n 个指定星期 `n#1..5`。数字星期采用 Quartz 编号 `1=SUN` 到 `7=SAT`，例如 `0 0 9 ? * 2#1` 表示每月第一个星期一 09:00。

保存任务前可使用 `schedulerctl jobs preview --type cron --expression '0 0 9 L * ?' --timezone Asia/Shanghai --count 5` 预览未来触发时间。`--after` 接受 RFC3339；默认从当前时间开始，默认返回 5 次、最多 100 次，调用不会创建任务或运行记录。

调度中心恢复时，`misfire_policy=skip` 忽略停机期间的计划，`fire_once` 在恢复时执行一次；扩展策略 `catch_up` 按原历史计划时刻补跑，并由 `max_catch_up` 限制单次恢复数量。所有策略都会把下一次计划推进到未来，避免反复处理同一批积压。

人工触发可使用 `schedulerctl jobs trigger JOB_ID --idempotency-key KEY --input VALUE`。同一租户、任务和幂等键即使并发提交，也只创建并执行一次，后续请求返回首次运行及首次参数对应的结果；幂等键最多 200 字节，运行参数最多 1 MiB。

需要临时绕过执行器组注册地址时，可重复传入 `--address`：`schedulerctl jobs trigger JOB_ID --address http://worker-a:9999 --address http://worker-b:9999`。覆盖地址只对该人工运行及其 retry 生效，不修改执行器组；仍按任务组的 FIRST/ROUND/LFU 等策略路由，分片广播会按覆盖地址生成分片。同一幂等键始终沿用首次提交的地址集合。

执行目标返回 HTTP 202 时，Core 将运行置为 `waiting_callback`，目标随后使用派发头中的 callback URL 和一次性 token 报告结果。失败回调或超过 `callback_timeout_seconds` 未回调时，同样遵守 `max_retries`：每次创建独立 retry attempt 并指数退避，只有预算耗尽后的最终失败才告警。已消费、过期或取消运行的 token 返回 404。

通知采用多订阅模型，支持 `webhook`、`email` 和 `dingtalk` provider。每个订阅可通过 `--events pending,running,waiting_callback,succeeded,failed,timed_out,cancelled,skipped,exhausted` 监听完整运行生命周期；`--all-jobs=false --job-ids ID1,ID2` 可绑定指定任务，默认订阅全部任务。每个渠道可配置 `--max-attempts`、`--backoff-initial-seconds` 和 `--backoff-max-seconds`，投递耗尽后进入 `dead`，不会永久阻塞 Outbox。

通用 Webhook 配置支持地址、自定义 Header 和 JSON Go 模板，例如 `schedulerctl notifications create --kind webhook --name ops --events succeeded,exhausted --config '{"url":"https://hooks.example.com/job","template":"{\"run\":\"{{.Payload.run_id}}\",\"status\":\"{{.Payload.status}}\"}"}'`。钉钉机器人支持 `none`、`access_token` 和 `hmac_sha256` 认证，例如 `--kind dingtalk --config '{"url":"https://oapi.dingtalk.com/robot/send","auth_type":"hmac_sha256","access_token":"...","secret":"...","template":"任务 {{.Payload.job_id}} 状态 {{.Payload.status}}"}'`。渠道配置必须使用主密钥环整体加密后才能落库；缺少密钥时创建和更新会失败，不会降级为明文存储。历史明文记录仍可读取，并会在下一次更新时加密。

Email 渠道使用严格邮箱地址列表，并支持主题和正文模板，例如 `--kind email --config '{"to":["ops@example.com"],"subject":"任务 {{.Payload.job_id}} {{.Payload.status}}","template":"run={{.Payload.run_id}} error={{.Payload.error}}"}'`。模板渲染后的主题禁止换行，避免邮件头注入；省略模板时保持原有 JSON 生命周期事件正文。

使用 `schedulerctl notifications history` 查询投递历史，可用 `--channel-id`、`--job-id`、`--status pending|delivered|dead` 和 `--limit` 过滤。响应包含更多记录时会返回 `next_cursor`，通过 `--cursor <next_cursor>` 稳定读取下一页。投递语义为 at-least-once；通用 Webhook 会携带 `Idempotency-Key` 和 `X-Go-Scheduler-Event-ID`，接收方应按事件 ID 幂等。

通知渠道支持完整生命周期管理：`notifications update ID --version N ...` 可更新事件范围、任务关联与重试策略；省略 `--config` 会保留已有加密配置，只有替换地址、认证或模板时才需要重新提交。切换 provider 类型时必须同时提供新配置。`notifications enable|disable ID --version N` 用于启停，`notifications delete ID --version N` 软删除并保留投递历史。所有修改都使用列表返回的 `version` 做乐观锁；停用或删除会把尚未完成的投递置为 `dead`，已经发出的外部请求仍遵循 at-least-once 语义。

所有全局参数都有同名环境变量：`SCHEDULER_URL`、`SCHEDULER_TOKEN`、`SCHEDULER_EMAIL`、`SCHEDULER_PASSWORD`、`SCHEDULER_TENANT`。自动化环境建议使用 `--password-stdin` 或 `SCHEDULER_TOKEN`，避免密码进入 shell 历史。运行 `schedulerctl --help` 查看任务、运行记录、触发、删除、健康检查和补全命令。
