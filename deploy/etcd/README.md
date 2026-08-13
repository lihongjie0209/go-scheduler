# etcd 生产配置

etcd 只保存 API Server 和 Scheduler Core 的临时服务注册，不保存任务、锁或业务配置。生产集群建议部署三个节点，并启用 TLS、认证和前缀权限。

键空间：

```text
/go-scheduler/{environment}/services/api-server/{instance_id}
/go-scheduler/{environment}/services/scheduler-core/{instance_id}
```

权限原则：

- API Server 对 `api-server/` 前缀可写，对 `scheduler-core/` 前缀只读。
- Scheduler Core 对 `scheduler-core/` 前缀可写。
- Lease TTL 为 15 秒，实例使用 KeepAlive；优雅停机主动 Revoke。
- 应用通过 `ETCD_CA`、`ETCD_CERT`、`ETCD_KEY` 和用户名密码连接。

生产证书应由组织 CA 签发，etcd 服务端启用客户端证书认证；不要把证书、密码或服务 Token 写入 ConfigMap。
