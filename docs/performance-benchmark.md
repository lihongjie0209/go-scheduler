# 调度性能基准

本文定义单节点和集群模式的性能验收口径。正式容量数据只能来自固定、独占且可重复创建的压测环境；GitHub Actions 用于发现明显瓶颈和保存可复现证据，不作为生产容量承诺。

## 比较原则

- 固定项目提交 SHA、镜像摘要、压测工具版本和配置。
- 每轮记录宿主机、容器 CPU/内存限制以及 PostgreSQL 版本和配置。
- 使用无业务耗时的 HTTP sink，端到端延迟统一按计划时刻到 sink 首次收到请求计算。
- 调度器、数据库、执行器和压测机分开采集 CPU、RSS、磁盘 IOPS、网络以及数据库连接数。
- 正式容量测试每个场景至少独立运行 5 次，报告中位数、离散程度和原始结果；单次 CI 结果只用于定位数量级问题。
- 认证、任务创建等控制面性能单独测试，不能与调度和派发数据混合。

## 核心场景

### 同时到期（burst）

预先创建 100、1,000、10,000 个任务，让任务在同一计划时刻到期。执行目标立即返回 200。记录从计划时间到 sink 收到请求的端到端延迟，并验证无丢失、无重复。

### 持续调度（steady）

按 100、500、1,000、2,000 次/秒的目标速率持续 15 分钟。前 3 分钟预热，统计后 10 分钟，最后 2 分钟观察积压清空。逐级增加速率，直到错误率超过 0.1%、P99 超过 1 秒或积压持续增长。

### 积压恢复（catch-up）

暂停执行目标 60 秒后恢复，测量积压峰值、恢复吞吐和清空耗时，并固定每轮使用的 misfire/阻塞策略。

### 恢复能力（recovery）

稳定负载期间分别重启调度进程、执行节点和数据库连接，记录恢复时间、重复执行和丢失数量。外部 Kubernetes Job 另测执行器重启后的重新接管延迟，不把 Kubernetes 创建耗时归因于调度器。

## 指标定义

| 指标 | 定义 |
| --- | --- |
| 调度吞吐 | sink 每秒收到的唯一执行请求数 |
| 调度延迟 | sink 接收时间减任务计划时间，报告 P50/P90/P95/P99/P99.9/max |
| 引擎派发延迟 | `scheduler_dispatch_delay_seconds`，计划时间到本项目 worker 开始处理 |
| worker 饱和 | `scheduler_worker_saturation_ticks_total`，没有空闲执行槽位的调度 tick 数 |
| 数据库池等待 | `scheduler_database_pool_empty_acquires_total` 和 `scheduler_database_pool_acquire_duration_seconds_total`，按 API/Core 池区分 |
| 端到端完成延迟 | 任务计划时间到运行进入成功终态 |
| 错误率 | 失败、超时和丢失执行数除以期望执行数 |
| 重复率 | 同一逻辑触发被 sink 收到超过一次的比例 |
| 积压 | 到期但尚未开始处理的运行数量及其最大年龄 |
| 单位成本 | 每 1,000 次执行的调度器/数据库 CPU 秒、峰值 RSS 和数据库写入量 |

延迟必须使用压测控制端生成的计划时间和唯一触发 ID 计算。Prometheus 指标用于定位内部瓶颈，不能代替 sink 端到端数据。

## 验收输出

每次正式测试保存以下产物：

- 环境清单、容器限制、内核和硬件信息；
- 配置文件、任务定义、随机种子和执行命令；
- 每次运行的原始事件数据与 Prometheus 快照；
- 分位数、吞吐、错误/重复/丢失和资源成本汇总；
- 火焰图或数据库慢查询证据，以及基于证据提出的优化项；
- 优化前后至少 5 轮结果的统计比较。

优化结论必须来自同环境下至少 5 轮前后对照，并且不能以错误率、资源消耗或语义正确性退化换取吞吐。

## 共享 HTTP sink

`scheduler-bench` 提供独立于调度器的黑盒执行目标：

```bash
go run ./cmd/scheduler-bench serve --listen :19090
```

压测控制器应在触发前把唯一事件 ID 和计划时间写入 sink，任务执行 URL 只携带事件 ID：

```bash
curl -X POST http://127.0.0.1:19090/api/v1/expect \
  -H 'Content-Type: application/json' \
  -d '{"events":[{"id":"run-0001","scheduled_at":"2026-08-13T12:00:00Z"}]}'

curl -X POST 'http://127.0.0.1:19090/execute?id=run-0001'
curl http://127.0.0.1:19090/api/v1/report
```

sink 对每个 ID 只使用首次到达时间计算延迟，并单独统计重复、未知和非法请求。`POST /api/v1/reset` 清空上一轮数据。正式压测时 sink 应部署在独立节点，避免与调度器争用资源。

## 单节点 CI 基准

`.github/workflows/standalone-benchmark.yml` 通过 `workflow_dispatch` 创建全新的 PostgreSQL 数据卷和单进程 `scheduler-server`，运行同时到期场景并上传以下原始证据：

- sink 端到端分位数、吞吐和逐秒报告；
- 调度器 Prometheus 指标；
- 容器 CPU、内存和网络快照；
- PostgreSQL 中的运行状态和派发延迟；
- 完整容器日志、提交 SHA 和 runner 环境。

工作流要求 missing、duplicate、unexpected 和 invalid 全部为零，否则失败。默认参数为 1,000 个任务、16 个执行 worker 和 1 秒调度间隔，可从 Actions 页面调整。

## 本地等价命令

`scheduler-bench load` 生成稳定的事件 ID、UTC Quartz Cron 表达式和统一计划时刻。控制面地址与任务实际访问的 sink 地址可以分开，以支持 Docker 网络：

```bash
# Go Scheduler；API key 自带租户时可省略 BENCH_TENANT_ID
BENCH_TOKEN=gsk_xxx BENCH_TENANT_ID=tenant-id \
  scheduler-bench load --system go \
  --server http://127.0.0.1:18080 \
  --sink http://benchmark-sink:19090/execute \
  --sink-control http://127.0.0.1:19090/execute \
  --run-id burst-go-001 --count 1000 --concurrency 16 \
  --scheduled-at 2026-08-14T02:00:00Z > burst-go-001.json
```

`--executor` 仅在需要把目标请求经过共享执行器转发时使用；单节点调度器基准直接请求 sink，避免额外代理影响结果。服务进程必须使用 UTC 时区，装载结束时间晚于计划时刻时整轮实验作废。
