# schedulerctl Python regression suite

This suite uses Python's standard library to test a released or locally supplied
`schedulerctl` binary against a running API server. It creates uniquely named
resources and removes them after each test.

```bash
SCHEDULERCTL=/path/to/schedulerctl \
SCHEDULER_URL=http://127.0.0.1:18080 \
SCHEDULER_EMAIL=admin@example.com \
SCHEDULER_PASSWORD='SchedulerDemo123!' \
SCHEDULERCTL_EXPECTED_VERSION=0.1.7 \
make integration-cli-python
```

`SCHEDULER_TENANT` is optional. When omitted, JWT authentication selects the
first accessible tenant. The configured account must be allowed to manage
executor groups, Kubernetes clusters, and notification channels.
