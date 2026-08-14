package migrations

import "embed"

//go:embed *.sql
var FS embed.FS

//go:embed 000001_init.up.sql
var Up string

//go:embed 000002_runtime_features.up.sql
var RuntimeFeaturesUp string

//go:embed 000003_console.up.sql
var ConsoleUp string

//go:embed 000004_block_policies.up.sql
var BlockPoliciesUp string

//go:embed 000005_job_dependencies.up.sql
var JobDependenciesUp string

//go:embed 000006_retry_lineage.up.sql
var RetryLineageUp string

//go:embed 000007_executor_routing.up.sql
var ExecutorRoutingUp string

//go:embed 000008_advanced_routing.up.sql
var AdvancedRoutingUp string

//go:embed 000009_active_routing.up.sql
var ActiveRoutingUp string

//go:embed 000010_sharding_broadcast.up.sql
var ShardingBroadcastUp string

//go:embed 000011_run_logs.up.sql
var RunLogsUp string

//go:embed 000012_fixed_delay.up.sql
var FixedDelayUp string

//go:embed 000013_reliable_notifications.up.sql
var ReliableNotificationsUp string

//go:embed 000014_script_jobs.up.sql
var ScriptJobsUp string

//go:embed 000015_manual_executor_groups.up.sql
var ManualExecutorGroupsUp string

//go:embed 000016_trigger_address_overrides.up.sql
var TriggerAddressOverridesUp string

//go:embed 000017_script_versions.up.sql
var ScriptVersionsUp string

//go:embed 000018_nodejs_php_scripts.up.sql
var NodeJSPHPScriptsUp string

//go:embed 000019_powershell_scripts.up.sql
var PowerShellScriptsUp string

//go:embed 000020_docker_image_jobs.up.sql
var DockerImageJobsUp string

//go:embed 000021_executor_labels.up.sql
var ExecutorLabelsUp string

//go:embed 000022_kubernetes_jobs.up.sql
var KubernetesJobsUp string

//go:embed 000023_hot_path_indexes.up.sql
var HotPathIndexesUp string

//go:embed 000024_webhook_subscriptions.up.sql
var WebhookSubscriptionsUp string

//go:embed 000025_notification_history_indexes.up.sql
var NotificationHistoryIndexesUp string

//go:embed 000026_notification_channel_lifecycle.up.sql
var NotificationChannelLifecycleUp string

//go:embed 000027_run_lease_fencing.up.sql
var RunLeaseFencingUp string

//go:embed 000028_docker_registry_auth.up.sql
var DockerRegistryAuthUp string

//go:embed 000029_notification_queue_metrics_index.up.sql
var NotificationQueueMetricsIndexUp string

//go:embed 000030_external_execution_identity.up.sql
var ExternalExecutionIdentityUp string

//go:embed 000031_executor_commands.up.sql
var ExecutorCommandsUp string

//go:embed 000032_executor_command_metrics_index.up.sql
var ExecutorCommandMetricsIndexUp string

//go:embed 000033_kubernetes_cluster_capacity.up.sql
var KubernetesClusterCapacityUp string

type Migration struct {
	Version int64
	SQL     string
}

var All = []Migration{{Version: 1, SQL: Up}, {Version: 2, SQL: RuntimeFeaturesUp}, {Version: 3, SQL: ConsoleUp}, {Version: 4, SQL: BlockPoliciesUp}, {Version: 5, SQL: JobDependenciesUp}, {Version: 6, SQL: RetryLineageUp}, {Version: 7, SQL: ExecutorRoutingUp}, {Version: 8, SQL: AdvancedRoutingUp}, {Version: 9, SQL: ActiveRoutingUp}, {Version: 10, SQL: ShardingBroadcastUp}, {Version: 11, SQL: RunLogsUp}, {Version: 12, SQL: FixedDelayUp}, {Version: 13, SQL: ReliableNotificationsUp}, {Version: 14, SQL: ScriptJobsUp}, {Version: 15, SQL: ManualExecutorGroupsUp}, {Version: 16, SQL: TriggerAddressOverridesUp}, {Version: 17, SQL: ScriptVersionsUp}, {Version: 18, SQL: NodeJSPHPScriptsUp}, {Version: 19, SQL: PowerShellScriptsUp}, {Version: 20, SQL: DockerImageJobsUp}, {Version: 21, SQL: ExecutorLabelsUp}, {Version: 22, SQL: KubernetesJobsUp}, {Version: 23, SQL: HotPathIndexesUp}, {Version: 24, SQL: WebhookSubscriptionsUp}, {Version: 25, SQL: NotificationHistoryIndexesUp}, {Version: 26, SQL: NotificationChannelLifecycleUp}, {Version: 27, SQL: RunLeaseFencingUp}, {Version: 28, SQL: DockerRegistryAuthUp}, {Version: 29, SQL: NotificationQueueMetricsIndexUp}, {Version: 30, SQL: ExternalExecutionIdentityUp}, {Version: 31, SQL: ExecutorCommandsUp}, {Version: 32, SQL: ExecutorCommandMetricsIndexUp}, {Version: 33, SQL: KubernetesClusterCapacityUp}}
