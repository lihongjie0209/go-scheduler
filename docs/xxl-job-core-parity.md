# XXL-JOB 核心能力对齐矩阵

本文只跟踪调度核心后端与 `schedulerctl`，项目不包含 Web UI。认证、授权、用户和租户管理仅作为调用前提记录，不进入核心能力优先级。状态以当前仓库代码和可执行测试为准：`已覆盖` 表示已有实现与自动化证据，`部分` 表示语义或测试层级尚未完整，`待实现` 表示核心链路缺失。

对齐目标是调度语义和可运维能力等价，不要求复刻 Java/JVM 的实现细节。XXL-JOB `BEAN` 对应 Go Executor SDK 的命名 handler；`GLUE_GROOVY` 是 JVM 进程内动态编译机制，在 Go 平台由命名 handler 或隔离的源码脚本执行与不可变版本回滚等价承载。Python 2 已停止维护，不进入生产运行时支持范围；Shell、Python 3、Node.js、PHP、PowerShell 五种仍可维护的 XXL-JOB 外部脚本运行时全部支持。

## 验证层级

每项新增能力按 TDD 顺序交付，并在矩阵中记录以下证据：

1. `U`：代码级单元测试，不依赖外部服务。
2. `M`：Testcontainers 单模块集成测试，例如 Store + PostgreSQL。
3. `X`：Testcontainers 跨模块集成测试，例如 PostgreSQL + Core gRPC。
4. `E`：Testcontainers 完整 use case，覆盖 CLI → API → gRPC → Core → PostgreSQL，并按能力需要加入 etcd/执行目标。

## 能力矩阵

| 领域 | XXL-JOB 能力 | Go 调度器现状 | CLI | 状态 | 四层证据/下一步 |
|---|---|---|---|---|---|
| 任务管理 | 新建、修改、删除、查询 | 完整 CRUD；修改和删除使用乐观锁，API 的 protobuf JSON 请求/响应兼容 int64 字符串；查询不泄露敏感 headers，更新省略 headers 时保留原值 | `jobs create/update/get/list/delete` | 已覆盖 | U: Job 校验、protobuf JSON 与 CLI update；M: PG 跨 Store 修改/读取、陈旧版本冲突、删除；X: gRPC 完整 CRUD；E: CLI→API→gRPC→Core→PG 完整 CRUD、409/404 与隐藏 header 保留 |
| 任务管理 | 启动、停止 | 原子切换并保留配置，启动重算 next_run_at，停止清空 next_run_at，带乐观锁 | `jobs start/stop --version` | 已覆盖 | U: `job_lifecycle_test.go`/CLI；M: Store 状态机；X/E: `internal/integration/lifecycle_integration_test.go` |
| 触发 | 人工/API 触发、临时地址覆盖 | 支持运行参数、最多 200 字节幂等键和最多 100 个临时 HTTP(S) Executor 地址；覆盖地址随运行持久化且不污染组注册，适配普通/主动/广播路由及 retry；同键固定首次输入与地址 | `jobs trigger ID --idempotency-key KEY --input VALUE [--address URL ...]`，`runs --job` | 已覆盖 | U: 请求/地址校验与 CLI payload；M: migration 16、PG 地址持久化、跨 Store ROUND 状态、直接 HTTP 任务拒绝；X: gRPC 规范化、幂等与 Claim；E: CLI 覆盖故障注册节点→双真实 Executor ROUND→运行回显实际/覆盖地址 |
| 调度 | Cron | 支持秒级 6 字段 Cron、IANA 时区和 DST；兼容 Quartz `?`、`L`、`L-n`、`nW`、`LW`、`L-nW`、`nL`、`n#1..5`；数字周严格采用 Quartz `1=SUN..7=SAT`，包括列表、范围、步长及不存在第五周时跳月 | 通过 `jobs create/update/get`，运行用 `runs --job` | 已覆盖 | U: 秒级、时区、DST、日历扩展、边界和数字周；M: PG 创建/跨 Store 读取月末计划；X: gRPC 创建 LW、读取及非法表达式 InvalidArgument；E: CLI 创建 L 任务→强制 misfire→Core 真实 HTTP→CLI 运行与下次月末计划 |
| 调度 | 未来触发时间预览 | 对未保存的 Cron/once/fixed schedule 从指定 RFC3339 时刻预览 1–100 次，默认 5 次；支持 IANA 时区与全部 Quartz 扩展，耗尽的 once 提前结束，严格只读 | `jobs preview --type --expression [--timezone --after --count]` | 已覆盖且扩展 | U: 月末 5 次、once 耗尽、数量/请求边界；M: Testcontainers PG 前后 jobs/runs 计数不变；X: gRPC `2#1` 三个月结果与 InvalidArgument；E: viewer CLI→API→gRPC→Core，上海时区 5 次月末结果且 PG 零写入 |
| 调度 | 固定间隔、固定速度、固定延迟 | `fixed_rate` 按计划时间连续推进；`fixed_delay` 入队后清空 next_run_at，仅在整个逻辑执行（含 retry/广播分片）最终终态后按完成时间重排；`fixed_interval` 保留为 fixed_rate 兼容别名 | 通过 `jobs create/update/get`；运行轨迹用 `runs` | 已覆盖 | U: Next/Due、misfire、校验；M: migration 12、多 Store 唯一入队、retry 延后与广播组终态；X: gRPC 创建及 next_run_at 状态；E: CLI 创建，真实 250ms HTTP 执行后验证下一次从完成时刻延迟 |
| 调度 | 调度过期：忽略、立即一次 | `skip` 忽略积压；`fire_once` 恢复时仅执行一次；扩展 `catch_up` 按历史计划时刻补跑且受 `max_catch_up` 限制；三者恢复后均将 next_run_at 推进到未来，多 Core 不重复入队 | 通过 `jobs create/update/get`；结果用 `runs --job` | 已覆盖且扩展 | U: 精确 scheduled_at、上限与 next；M: 两 Store 并发恢复，三策略分别 0/1/3；X: gRPC 创建与查询三策略；E: CLI 创建、Core 停机积压后恢复、真实 HTTP 与 CLI 运行均为 0/1/3 |
| 执行控制 | 超时 | HTTP deadline 取消并以 `timed_out` 终止当前 attempt，可按重试预算继续 | `runs get ID` 查看轨迹 | 已覆盖 | U: 超时分类；M: PG 终态/重试/outbox；X: gRPC 轨迹；E: CLI 到真实 HTTP 超时后成功重试 |
| 执行控制 | 失败重试 | 每次重试创建独立 `retry` 运行，指数退避，attempt 递增并记录 `retry_of_run_id`；仅最终失败告警 | `runs get ID`、`runs --job` | 已覆盖 | U: 预算/退避/CLI；M: 原子终止、新 attempt 与最终告警；X/E: gRPC 与完整 CLI 用例 |
| 执行控制 | 阻塞策略：串行、丢弃后续、覆盖之前 | serial/discard_later/cover_early；跨 Core 通过 PG 取消运行，parallel 为扩展 | 通过任务 CRUD/trigger/runs | 已覆盖 | U: `block_policy_test.go`；M: Store 状态机；X/E: gRPC、CLI 与真实 HTTP 取消用例 |
| 执行控制 | 终止运行 | pending/running/waiting_callback 原子取消，跨 Core 取消 HTTP，回调 token 失效，重复操作幂等 | `runs cancel ID --reason` | 已覆盖 | U: cancel reason/CLI；M: Store 各状态；X: gRPC；E: CLI 到真实 HTTP context cancellation |
| 编排 | 父任务成功触发子任务 | 同租户 DAG；同步/异步最终成功触发；失败不触发；按父运行幂等；记录 parent_run_id | `jobs dependencies get/set`、`runs` | 已覆盖 | U: ID 规范化/CLI；M: PG 成功、失败、callback、环与幂等；X: gRPC；E: CLI 到两级 HTTP 执行 |
| 执行器 | 自动注册、主动下线、手工地址、组管理、心跳与失效清理 | `automatic` 组通过 TTL 心跳注册，支持幂等主动下线，Go SDK 正常退出立即注销、异常退出由 TTL 兜底；`manual` 组持久化静态 HTTP(S) 地址并拒绝动态注册/注销；组更新/删除带乐观锁，被任务引用时禁止删除；动态过期节点不参与路由，静态节点始终可路由 | `executors groups create/update/list/delete --mode --address`、`executors register/unregister/list` | 已覆盖 | U: SDK DELETE、模式/URL 规范化和 CLI 请求；M: migration 15、跨 Store 注销幂等、静态组拒绝注销、静态路由/更新/冲突/删除；X: gRPC 注册/注销与 SDK 经 API 正常退出；E: CLI→API→gRPC 完整 SDK 执行后退出并验证节点立即消失，另覆盖手工地址替换与真实路由 |
| 路由 | FIRST/LAST/ROUND/RANDOM/HASH/LFU/LRU | 七种策略均已实现；HASH 使用 MD5 + 100 虚拟节点；ROUND/LFU/LRU 按 job 隔离并由 PG 锁事务维护；运行记录保存节点 | `executors`、`runs` | 已覆盖 | U: 七策略算法与校验；M: migration、TTL、跨连接原子 LFU/计数；X: 七策略 gRPC 配置；E: CLI 创建组、注册两个真实 executor 并逐策略执行断言 |
| 路由 | FAILOVER/BUSYOVER | FAILOVER 按 node_id 顺序调用 `/health` 并选择首个 2xx；BUSYOVER 调用 `/idle` 携带 job_id 并选择首个空闲节点；逐节点 Context 限时且不持有 PG 锁 | `executors groups create --strategy failover/busyover`、`runs` | 已覆盖 | U: HTTP 探测、跳过、全失败与 Context；M: migration 9/TTL 候选；X: gRPC 配置；E: CLI 注册故障/繁忙和健康 executor，真实探测后仅后者执行 |
| 路由 | 分片广播 | SHARDING_BROADCAST 在触发时按 node_id 稳定快照存活节点，每节点创建独立运行并固定 `shard_index/shard_total` 与执行地址；人工、定时和依赖触发均原子广播，失败仅重试原分片 | `executors groups create --strategy sharding_broadcast`、`runs --broadcast-group` | 已覆盖 | U: 分片规划/策略校验/CLI 查询；M: migration 10、人工与定时原子入队、并发领取、幂等与分片重试；X: gRPC 创建/触发/过滤；E: CLI→API→Core→双 executor，单分片失败后固定节点重试 |
| 执行模式 | Bean/脚本/GLUE/HTTP、源码版本回溯 | 支持直连 HTTP；Go Executor SDK 命名 handler 等价承载 Bean/Groovy 的处理器语义；独立 Script Executor 支持 Shell、Python 3、Node.js、PHP、PowerShell 源码，固定解释器+0700 临时文件执行、输入环境变量、进程组超时取消、1 MiB 源码/输出边界及 stdout/stderr Rolling 日志；脚本创建和源码变更生成不可变 revision，普通配置更新不制造版本；回滚在 PG 事务中校验任务乐观锁、恢复源码并生成新审计 revision；明确不嵌入 JVM Groovy 编译器，也不提供已 EOL 的 Python 2 | `executors`；`jobs create/update`；`jobs script-versions list/rollback` | 已覆盖（跨语言等价） | U: 五语言校验及本机可用运行时真实执行；M: migration 17–19、五语言持久化/版本历史，自建 Alpine 镜像实际执行 Node.js/PHP/PowerShell；X: gRPC 五语言定义及回滚；E: CLI→API→gRPC→Core→自建容器真实执行 PowerShell并回传日志，另有 Shell/Node.js/PHP SDK use case；解释器不加载到 Core 进程 |
| 结果 | 同步结果、异步回调 | HTTP 2xx 同步完成；202 转入 waiting_callback 并使用限时一次性 token；失败 callback 和 callback 超时与同步失败一样按 max_retries 创建独立 attempt、保留 lineage/参数/分片，指数退避；仅最终失败告警；成功触发依赖，取消/过期/已消费 token 均失效 | `runs get ID`、`runs --job` | 已覆盖且扩展 | U: token/失败分类及 callback 退避；M: 成功依赖、取消/重放、失败/超时 retry、中间告警抑制、最终告警；X: gRPC 失败 callback、retry lineage 与 token 重放；E: CLI 触发→真实 executor 202→失败 callback→retry 再派发→成功 callback，CLI 验证 failed/succeeded attempts 与旧 token 404 |
| 日志 | 调度结果、执行结果、Rolling 日志 | Core 派发前生成限时运行 token；executor 可按 `entry_id` 幂等追加 stdout/stderr 分块；PG 全局单调游标分页、租户隔离和保留期清理；同步/异步及广播运行均使用同一协议 | `runs logs ID [--after N --limit N]`、`runs logs ID --follow` | 已覆盖 | U: 批次/大小/stream 校验与 CLI 游标；M: migration 11、token、幂等、分页、租户隔离和清理；X: bufconn 上传/读取；E: 真实 executor 从派发 payload 上传，CLI 经 API/gRPC 读取 |
| 告警 | 失败邮件、扩展告警 | 仅 retry 耗尽后的最终失败生成 outbox；webhook/email 渠道按“事件×渠道”独立投递、PG SKIP LOCKED 多 Core 抢占、指数退避；一个渠道失败不会重复已成功渠道；无渠道自动消费 | `notifications create/list` | 已覆盖 | U: 配置校验、退避与 CLI；M: migration 13、两 Store 独占、逐渠道重试/完成；X: gRPC 渠道创建查询；E: CLI 配置双 webhook，最终失败后健康渠道 1 次、暂时失败渠道 2 次 |
| 报表 | 任务/运行/实例统计 | dashboard 摘要；运行报表按 IANA 时区、1–90 天范围统计每日总量/成功/失败/活跃/取消/跳过并补齐零值日期，失败包含 failed/timed_out | `dashboard`；`reports runs --from/--to/--timezone` | 已覆盖（核心运行趋势） | U: 日期默认值、范围、时区与 CLI 参数；M: PG 时区跨日、零值、租户隔离；X: gRPC 分类统计；E: CLI→API→gRPC→Core→PG，真实 HTTP 成功/失败各一次 |
| 清理 | 调度日志与执行器日志清理 | `HISTORY_RETENTION` 每小时分租户批量清理终态运行及关联 Rolling 日志/幂等键/依赖派发表，绝不删除活跃运行；pg_partman 可用时调用其维护过程，不可用时通过 PG advisory lock 自动续建和清理月分区；支持按任务和截止时间人工清理 | `runs purge --before RFC3339 [--job ID] [--limit N]` | 已覆盖 | U: gRPC 参数、分区范围、时间戳/批次边界与 CLI 请求；M: 自建 pg_partman 与标准 PostgreSQL 两种 Testcontainers、任务过滤、批次、关联日志、自动保留、活跃保护；X: gRPC 删除计数、NotFound 与幂等；E: CLI→API→gRPC→Core→PG，真实成功/失败运行分批清理 |
| 高可用 | 调度中心集群、执行器集群 | API/Core 多实例，Core 使用 etcd 15 秒租约注册，API gRPC round_robin 动态发现；实例 draining/撤销会立即从地址集移除；多 Engine 通过 PG `SKIP LOCKED` 保证单次执行 | `health`、所有业务命令自动故障切换 | 已覆盖 | U: resolver 过滤 draining 与发布空地址集；M: etcd 双实例独立租约和撤销；X: PG+etcd+双 Store/Core/gRPC，验证双实例均接流量及单实例下线后连续调用；E: CLI→API→etcd resolver→双 Core/Engine→PG→真实 HTTP，下线一个 Core 前后各执行一次且无重复 |
| 部署 | 单二进制 API + Core | `scheduler-server` 在同一进程运行 REST API、内存 gRPC Core、Engine 与通知 Worker；不连接 etcd，不开放 Core gRPC 端口；Compose 默认只启动 PostgreSQL、迁移、初始化与该服务 | 所有 `schedulerctl` 命令 | 已覆盖 | U: 内存 gRPC 认证、调用和关闭；M/X/E: 复用 PostgreSQL、Core gRPC 与 CLI use case，并由默认 Compose 验证单容器链路 |

认证授权已经具备 JWT、API Key、tenant RBAC 和 gRPC 服务令牌，后续仅在核心 use case 中作为前置条件使用，不单独占用近期对齐迭代。

## 已交付切片

1. 任务生命周期、人工触发、Cron/固定速率/固定延迟与 misfire。
2. 超时、重试、阻塞策略、运行终止与父子任务编排。
3. 执行器协议、注册心跳、主动下线、七种基础路由、故障/忙碌转移与分片广播。
4. 异步回调、Rolling 日志、失败告警、运行报表与历史清理。
5. etcd 服务发现、双 Core 高可用、Go Executor SDK 与 Shell/Python Script Executor。
6. 执行器组自动/手工注册模式、乐观锁更新/删除和静态地址真实路由。
7. Quartz Cron 日历扩展 `L/W/#` 与 1–7 数字周语义。
8. 人工触发临时 Executor 地址覆盖、幂等持久化与路由。
9. 未保存调度定义的未来触发时间预览。
10. 脚本源码不可变版本历史、备注与原子回滚。
11. Node.js 与 PHP 脚本运行时、自建镜像及完整执行验证。
12. PowerShell 7.6.4 固定运行时、官方 SHA256 校验与容器端到端执行。

本轮核心能力对齐已经完成。后续新增能力继续以调度核心语义差距为准：每个切片必须先出现能描述缺口的失败测试，再实现到单元、模块集成、跨模块集成和端到端四层测试全部通过，并同步更新本矩阵；认证授权只维持核心调用所需的基础能力。
