# PostgreSQL 与 pg_partman 镜像

本目录从固定版本的上游源码自行编译 `pg_partman`，运行层只基于 PostgreSQL 官方镜像，不依赖第三方扩展镜像。

该镜像是可选优化，不是应用启动前提。连接不提供 pg_partman 的托管或标准 PostgreSQL 时，数据库迁移会跳过扩展配置，`scheduler-server`/`scheduler-core` 使用 advisory lock 自动创建和清理月分区。

```bash
docker build \
  --build-arg POSTGRES_MAJOR=16 \
  --build-arg PG_PARTMAN_VERSION=5.5.0 \
  -t company/go-scheduler-postgres:16-partman-5.5.0 \
  -f deploy/postgres/Dockerfile .
```

升级时同时修改 Dockerfile 默认参数、Compose 参数和集成测试镜像标签，并执行：

```bash
make integration
```

生产环境启用后台维护：

```conf
shared_preload_libraries = 'pg_partman_bgw'
pg_partman_bgw.dbname = 'scheduler'
pg_partman_bgw.role = 'partman_maintainer'
pg_partman_bgw.interval = 3600
```

`partman_maintainer` 应为非超级用户，并拥有 `job_runs` 分区集、目标 schema 的建表权限和 `partman` schema 使用权限。月分区和 90 天 retention 由数据库迁移配置。
