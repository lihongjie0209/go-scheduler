# 代码库审计：架构、性能与安全

审计基线：`v0.1.4` 之后的主分支，2026-08-13。本文记录可由代码、测试或基准证明的结论；未经过固定环境多轮对照的数据不作为容量承诺。

## 总体结论

项目已经形成清晰的控制面、调度核心和执行器边界，单进程与 etcd 集群模式复用同一套 Core/Store 逻辑。调度状态由 PostgreSQL 条件更新、事务和租约驱动，执行器通过 gRPC 接收任务，异步外部执行状态可在重启后恢复。主要剩余风险不在基本功能，而在长期运行资源边界、保留大量历史数据后的查询成本，以及可信执行器带来的高权限执行面。

本轮已修复：

- 配置类型错误不再静默回退，进程会 fail-fast；
- HTTP/gRPC 监听端口在启动 hook 中同步绑定，端口冲突会直接使启动失败并回滚已打开的 listener；
- Core 的 Executor gRPC 连接池增加 256 个连接上限、引用计数和空闲 LRU 淘汰；
- Notifier 将 30 秒租约内的 claim 批次收窄为 20 条并以 10 路有界并发投递，Webhook/SMTP 单次 deadline 为 10 秒；
- 通知升级为可扩展 provider 订阅模型：Webhook、Email、钉钉可按完整运行生命周期和全局/任务范围过滤，支持每渠道有界指数退避、死信及投递历史查询；
- 生命周期事件与任务状态转换在同一 PostgreSQL 事务内写入 Outbox，即使当前没有订阅也保留可审计事件，再由异步匹配器按渠道范围生成投递；
- 高频辅助历史清理改为每 10 秒最多删除每表 10,000 行，终态运行清理仍按小时逐租户执行，避免无上限 DELETE 长事务和高频租户扫描；
- 并发删除 tenant owner 通过事务和租户行锁串行化，保持至少一个 owner；
- 服务令牌改为恒定时间比较；
- Argon2 哈希参数在内存分配前验证上下限，阻止异常哈希触发资源耗尽；
- 登录接口按来源 IP 和邮箱摘要执行有界本地限流，成功认证后重置窗口；
- 不存在或禁用账号同样执行生产参数的 dummy Argon2 校验，降低邮箱时序枚举风险；
- API→Core、Executor→Core/standalone 和 Core→Executor 均支持可选 TLS，并有真实 TLS gRPC 握手测试；
- Docker 执行器按产品约束默认继承 Docker 原生网络和权限策略，网络、只读根文件系统、CPU 和内存限制均改为显式可选；
- API、兼容 HTTP Executor 和 Docker 任务定义拒绝首个对象后的第二个 JSON 值，避免校验层与执行层对请求内容产生歧义；
- migration 23 为 Claim 活跃集合、过期租约、幂等记录、Outbox 和依赖派发清理增加针对性索引。

## 架构审计

### 当前边界

```text
scheduler server ─┬─ HTTP API / auth
                  ├─ in-process gRPC Core
                  ├─ scheduler engine / notifier
                  └─ PostgreSQL

api-server ── gRPC + etcd discovery ── scheduler core ── gRPC ── executor
     │                                      │                         │
     └──────────── PostgreSQL ──────────────┘                 script/http/docker/k8s
```

- `internal/core` 持有调度状态机和派发逻辑；`internal/store` 持有 PostgreSQL 原子性和租约语义。
- 单进程模式使用 bufconn 连接 API 与 Core，不依赖 etcd，也没有网络内跳。
- 集群模式由 etcd 发现 Core 和 Executor，任务一致性仍以 PostgreSQL 为准；etcd 不是运行状态事实来源。
- 外部 Kubernetes Job 使用确定性 execution ID 查询既有 Job，Executor 重启后可继续观察而不是重复创建。

### 已确认问题

| 严重度 | 问题 | 影响 | 状态 |
| --- | --- | --- | --- |
| 高 | 并发删除两个 owner 可同时通过预检查 | tenant 可能失去全部 owner | 已修复并增加 PostgreSQL 并发测试 |
| 中 | Executor 地址连接永久缓存 | 节点滚动后连接和 goroutine 持续增长 | 已修复，连接池有界且不淘汰使用中的连接 |
| 中 | 配置解析错误静默使用默认值 | 错误容量或 Cookie 策略仍能启动 | 已修复并增加表驱动测试 |
| 低 | 多个运行入口仍位于可导入的 `cmd/*` package | 组合入口可用，但应用装配边界不够纯粹 | 待后续迁移到 `internal/app`，不影响运行正确性 |

### 剩余架构风险

- 通知投递是 at-least-once：进程在外部服务接受请求后、数据库确认前崩溃时仍可能重复发送。Webhook 已发送 `Idempotency-Key` 与 `X-Go-Scheduler-Event-ID`，消费方仍必须按事件 ID 幂等。
- Executor 连接池上限当前为编译期常量 256。超过该规模的集群应先通过容量测试，再决定是否开放配置。

## 性能审计

### 已有证据

最新同机 1,000 任务 burst 三轮中位数为 514.27 次/秒，P99 为 1.943 秒，零丢失、零重复。低水位批量补充将 `ClaimRuns` 调用从 282 次降至 83 次，SQL 总执行时间约从 818 ms 降至 169 ms。详细环境和历史数据见 [performance-benchmark.md](performance-benchmark.md)。

`pg_stat_statements` 证据表明主要成本是运行状态写入、Claim 和事务往返，而不是普通任务查询。已有批量到期入队、单活跃节点快路径、批量日志写入和原子回调终态更新。

### 本轮数据库改进

Migration 23 增加：

- `job_runs_active_concurrency_idx`：Claim 的 job/tenant 活跃计数只扫描运行中集合，不随终态历史线性增长；
- `job_runs_expired_lease_idx`：恢复过期 running 租约；
- `job_run_idempotency_created_idx`、`job_run_idempotency_run_idx`：保留期和按 run 清理；
- `outbox_published_idx`：已发布事件清理；
- `job_dependency_dispatches_created_idx`、`job_dependency_dispatches_child_run_idx`：依赖历史清理。

这些索引已在 PostgreSQL 16 Testcontainers 中应用，并在 20,000 条终态历史、10 条活跃运行的数据集上确认活跃集合查询采用索引计划。它们主要改善大历史量和恢复场景，不应使用空数据库 burst 数字夸大收益。

### 剩余性能工作

- 构造包含大量终态历史、少量 active/pending 的数据集，对 migration 23 前后执行 `EXPLAIN (ANALYZE, BUFFERS)` 和至少五轮 Claim/cleanup 对照。
- steady、catch-up 和 recovery 场景尚未达到文档要求的五轮正式容量验收。
- migration 23 在已有大表上创建索引会占用 I/O 和锁资源，生产升级需维护窗口；后续可评估非事务 `CREATE INDEX CONCURRENTLY` 的独立迁移流程。
- 当前辅助历史批次上限提供约每表每日 8,640 万行的理论清理能力；实际能力受行宽、级联删除、WAL 和 autovacuum 影响，正式容量验收需监控 oldest-age 水位，而不能只观察单轮耗时。

## 安全审计

### 已有防护

- 用户密码使用 Argon2id；JWT 固定 HS256、issuer 和过期时间；refresh token 单次轮换并检测重放。
- API key、refresh token 和回调 token 只在 PostgreSQL 保存 SHA-256 哈希。
- Job headers、Kubernetes 凭据和通知配置使用 AES-GCM 加密后落库。
- HTTP 请求体限制为 1 MiB；脚本、Docker 和 Kubernetes 日志限制为 1 MiB；API 错误不返回数据库内部信息。
- SQL 使用参数绑定；脚本和 Docker 参数不经 shell 拼接。
- 租户写操作执行角色检查，平台管理接口执行 platform-admin 检查。

### 本轮修复

| 严重度 | 问题 | 修复 |
| --- | --- | --- |
| 中 | gRPC Bearer token 普通字符串比较 | 使用 `crypto/subtle.ConstantTimeCompare` |
| 中 | 数据库中的异常 Argon2 参数可触发超大分配 | 限制 memory、iterations、parallelism、salt 和 hash 长度 |
| 中 | 匿名登录请求可持续消耗 Argon2 CPU | 增加有界的来源 IP + 邮箱窗口限流，成功后重置 |
| 高 | 并发 owner 删除破坏授权治理不变量 | PostgreSQL 事务和 tenant 行锁 |
| 中 | 非法布尔/整数/Duration 环境变量静默回退 | 启动边界严格校验 |
| 高 | `all_jobs=true` 携带其他租户 `job_ids` 可绕过任务归属校验并回显 UUID | Core/Store 双层拒绝矛盾范围，增加跨租户 Testcontainers 断言 |
| 中 | 通知租约过期后旧 Worker 可覆盖新 Worker 状态 | 所有完成、重试和死信回写增加 owner + lease fencing |
| 中 | Webhook/DingTalk 网络错误可能把 URL Token 持久化到通知历史 | 网络错误脱敏，持久化错误限制为合法 UTF-8 的 4 KiB |
| 中 | 通知渠道无法更新、停用或删除，只能直接修改数据库 | migration 26 增加版本和软删除，提供 gRPC/REST/CLI 完整生命周期管理 |
| 中 | 普通 JSON 请求和 Docker 定义会忽略首个值后的尾随文档 | 所有流式解码入口在首个值后强制要求 EOF，并增加 unit 与 PostgreSQL API 集成断言 |

`govulncheck v1.6.0 ./...` 未发现可达漏洞。依赖图中存在 19 个模块级公告，但当前代码没有调用受影响符号；仍应由 CI 在每次依赖更新后复查。

### 接受风险与待处理项

- 按产品要求，脚本、HTTP 和 Docker 任务不做目标网络白名单；Docker 默认也不强制 drop capabilities、只读根文件系统或 PID 限制。Executor 因此属于可信高权限组件，必须部署在独立节点/namespace，并由基础设施实施宿主机和凭据隔离。
- 所有跨进程 gRPC 方向都支持 TLS，但为了兼容本地开发，未配置证书时仍允许明文。生产必须配置 CA/证书或由 service mesh 强制 mTLS；应用原生双向客户端证书校验仍是后续增强项。
- `/metrics` 默认未鉴权，应只通过内网 Service 或网络策略暴露。
- 执行器取消指令通过 PostgreSQL 租约队列持久化；Docker 容器和 Kubernetes Job 使用稳定外部执行 ID 在执行器重启后恢复取消，并在删除前校验托管标签。应对 `scheduler_executor_command_queue_pending` 和最老积压时长设置告警。
- Executor 完成回报先写节点本地 durable outbox，再异步发送给 Core；Core 短暂不可达或 Executor 重启不会丢失已经完成的结果。该目录包含短期 callback token，部署必须使用每节点独占的受限持久卷。
- 应用内登录限流是单实例状态。集群模式还应由网关实施共享限流，避免攻击者把额度乘以 API 实例数。
- Kubernetes 配置允许 `insecure_skip_tls_verify`，这是显式运维选项；生产审计应检测并告警，而不是静默启用。

## 验证记录

- `go test ./...`：通过；
- `go test -race ./...`：通过；
- `go vet ./...`：通过；
- PostgreSQL migration 23 Testcontainers：通过；
- PostgreSQL 并发 owner 删除 Testcontainers：通过；
- PostgreSQL 辅助历史清理批次上限与重复调用收敛 Testcontainers：通过；
- `govulncheck v1.6.0 ./...`：无可达漏洞。

完整集成回归和镜像构建仍以 GitHub Actions 为最终门禁。
