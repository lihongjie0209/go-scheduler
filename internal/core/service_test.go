package core

import (
	"strings"
	"testing"
	"time"

	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestNormalizeRunReportRequest(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 13, 12, 0, 0, 0, time.UTC)
	tests := []struct {
		name           string
		req            *schedulerv1.GetRunReportRequest
		from, to, zone string
		wantErr        bool
	}{
		{name: "defaults", req: &schedulerv1.GetRunReportRequest{TenantId: "tenant"}, from: "2026-07-31", to: "2026-08-13", zone: "UTC"},
		{name: "explicit timezone", req: &schedulerv1.GetRunReportRequest{TenantId: "tenant", FromDate: "2026-08-01", ToDate: "2026-08-03", Timezone: "Asia/Shanghai"}, from: "2026-08-01", to: "2026-08-03", zone: "Asia/Shanghai"},
		{name: "missing tenant", req: &schedulerv1.GetRunReportRequest{}, wantErr: true},
		{name: "bad timezone", req: &schedulerv1.GetRunReportRequest{TenantId: "tenant", Timezone: "Mars/Base"}, wantErr: true},
		{name: "reversed", req: &schedulerv1.GetRunReportRequest{TenantId: "tenant", FromDate: "2026-08-03", ToDate: "2026-08-01"}, wantErr: true},
		{name: "over 90 days", req: &schedulerv1.GetRunReportRequest{TenantId: "tenant", FromDate: "2026-01-01", ToDate: "2026-08-01"}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			from, to, zone, err := normalizeRunReportRequest(tt.req, now)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil && (from.Format(time.DateOnly) != tt.from || to.Format(time.DateOnly) != tt.to || zone.String() != tt.zone) {
				t.Fatalf("got %s..%s %s", from.Format(time.DateOnly), to.Format(time.DateOnly), zone)
			}
		})
	}
}

func TestValidatePurgeRunHistoryRequest(t *testing.T) {
	t.Parallel()
	valid := func() *schedulerv1.PurgeRunHistoryRequest {
		return &schedulerv1.PurgeRunHistoryRequest{TenantId: "tenant", Before: timestamppb.New(time.Now().Add(-time.Hour)), Limit: 500}
	}
	tests := []struct {
		name    string
		mutate  func(*schedulerv1.PurgeRunHistoryRequest)
		wantErr bool
	}{
		{name: "valid"},
		{name: "default limit", mutate: func(r *schedulerv1.PurgeRunHistoryRequest) { r.Limit = 0 }},
		{name: "missing tenant", mutate: func(r *schedulerv1.PurgeRunHistoryRequest) { r.TenantId = "" }, wantErr: true},
		{name: "missing before", mutate: func(r *schedulerv1.PurgeRunHistoryRequest) { r.Before = nil }, wantErr: true},
		{name: "invalid timestamp", mutate: func(r *schedulerv1.PurgeRunHistoryRequest) { r.Before = &timestamppb.Timestamp{Seconds: 253402300800} }, wantErr: true},
		{name: "limit too large", mutate: func(r *schedulerv1.PurgeRunHistoryRequest) { r.Limit = 10001 }, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := valid()
			if tt.mutate != nil {
				tt.mutate(req)
			}
			if err := validatePurgeRunHistoryRequest(req); (err != nil) != tt.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func validJob() *schedulerv1.Job {
	return &schedulerv1.Job{TenantId: "tenant", Name: "job", ScheduleType: "cron", ScheduleExpression: "0 * * * * *", Timezone: "UTC", TargetUrl: "https://example.com/hook", HttpMethod: "POST", TimeoutSeconds: 30, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, CallbackTimeoutSeconds: 3600, MaxQueueSize: 1000}
}
func TestValidateJob(t *testing.T) {
	t.Parallel()
	if err := validateJob(validJob()); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*schedulerv1.Job)
	}{{"private URL syntax", func(j *schedulerv1.Job) { j.TargetUrl = "file:///etc/passwd" }}, {"userinfo", func(j *schedulerv1.Job) { j.TargetUrl = "https://user:pass@example.com" }}, {"method", func(j *schedulerv1.Job) { j.HttpMethod = "TRACE" }}, {"template", func(j *schedulerv1.Job) { j.BodyTemplate = "{{secret}}" }}, {"overlap", func(j *schedulerv1.Job) { j.OverlapPolicy = "overwrite" }}}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			job := validJob()
			tt.mutate(job)
			if err := validateJob(job); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidateJobBlockPolicies(t *testing.T) {
	t.Parallel()
	for _, policy := range []string{"serial", "discard_later", "cover_early", "parallel"} {
		policy := policy
		t.Run(policy, func(t *testing.T) {
			t.Parallel()
			job := validJob()
			job.OverlapPolicy = policy
			if err := validateJob(job); err != nil {
				t.Fatalf("policy %q rejected: %v", policy, err)
			}
		})
	}
}

func TestValidExecutorRouteStrategy(t *testing.T) {
	t.Parallel()
	for _, strategy := range []string{"first", "last", "round", "random", "hash", "lfu", "lru", "failover", "busyover", "sharding_broadcast"} {
		strategy := strategy
		t.Run(strategy, func(t *testing.T) {
			t.Parallel()
			if !validExecutorRouteStrategy(strategy) {
				t.Fatalf("strategy %q rejected", strategy)
			}
		})
	}
	if validExecutorRouteStrategy("sharding") {
		t.Fatal("unsupported strategy accepted")
	}
}

func TestNormalizeExecutorGroup(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		group     *schedulerv1.ExecutorGroup
		wantMode  string
		wantAddrs []string
		wantErr   bool
	}{
		{name: "automatic defaults", group: &schedulerv1.ExecutorGroup{TenantId: "tenant", Name: "workers", RouteStrategy: "round"}, wantMode: "automatic"},
		{name: "manual addresses normalized", group: &schedulerv1.ExecutorGroup{TenantId: "tenant", Name: "workers", RouteStrategy: "first", RegistrationMode: "manual", ManualAddresses: []string{" https://worker-b:9999/ ", "http://worker-a:9999", "http://worker-a:9999/"}}, wantMode: "manual", wantAddrs: []string{"http://worker-a:9999", "https://worker-b:9999"}},
		{name: "manual requires address", group: &schedulerv1.ExecutorGroup{TenantId: "tenant", Name: "workers", RouteStrategy: "round", RegistrationMode: "manual"}, wantErr: true},
		{name: "automatic rejects addresses", group: &schedulerv1.ExecutorGroup{TenantId: "tenant", Name: "workers", RouteStrategy: "round", RegistrationMode: "automatic", ManualAddresses: []string{"http://worker:9999"}}, wantErr: true},
		{name: "invalid mode", group: &schedulerv1.ExecutorGroup{TenantId: "tenant", Name: "workers", RouteStrategy: "round", RegistrationMode: "dns"}, wantErr: true},
		{name: "invalid address", group: &schedulerv1.ExecutorGroup{TenantId: "tenant", Name: "workers", RouteStrategy: "round", RegistrationMode: "manual", ManualAddresses: []string{"file:///tmp/socket"}}, wantErr: true},
		{name: "userinfo rejected", group: &schedulerv1.ExecutorGroup{TenantId: "tenant", Name: "workers", RouteStrategy: "round", RegistrationMode: "manual", ManualAddresses: []string{"https://user:pass@worker:9999"}}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := normalizeExecutorGroup(tt.group)
			if (err != nil) != tt.wantErr {
				t.Fatalf("error=%v wantErr=%v", err, tt.wantErr)
			}
			if err == nil && (got.RegistrationMode != tt.wantMode || strings.Join(got.ManualAddresses, ",") != strings.Join(tt.wantAddrs, ",")) {
				t.Fatalf("group=%+v want mode=%q addresses=%v", got, tt.wantMode, tt.wantAddrs)
			}
		})
	}
}

func TestValidateJobAcceptsFixedRateAndFixedDelay(t *testing.T) {
	t.Parallel()
	for _, scheduleType := range []string{"fixed_rate", "fixed_delay"} {
		t.Run(scheduleType, func(t *testing.T) {
			job := validJob()
			job.ScheduleType = scheduleType
			job.ScheduleExpression = "30"
			if err := validateJob(job); err != nil {
				t.Fatalf("validateJob(%s): %v", scheduleType, err)
			}
		})
	}
}

func TestValidateScriptJob(t *testing.T) {
	t.Parallel()
	valid := func() *schedulerv1.Job {
		job := validJob()
		job.TargetUrl = ""
		job.ExecutorGroupId = "group"
		job.ExecutorHandler = "__script__"
		job.ScriptLanguage = "shell"
		job.ScriptSource = "printf '%s' \"$SCHEDULER_INPUT\""
		return job
	}
	if err := validateJob(valid()); err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name   string
		mutate func(*schedulerv1.Job)
	}{
		{name: "language", mutate: func(j *schedulerv1.Job) { j.ScriptLanguage = "ruby" }},
		{name: "source", mutate: func(j *schedulerv1.Job) { j.ScriptSource = "" }},
		{name: "source too large", mutate: func(j *schedulerv1.Job) { j.ScriptSource = strings.Repeat("x", (1<<20)+1) }},
		{name: "handler", mutate: func(j *schedulerv1.Job) { j.ExecutorHandler = "custom" }},
		{name: "script without group", mutate: func(j *schedulerv1.Job) { j.ExecutorGroupId = "" }},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := valid()
			tt.mutate(job)
			if err := validateJob(job); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestValidateScriptJobAcceptsAdditionalLanguages(t *testing.T) {
	t.Parallel()
	for _, language := range []string{"nodejs", "php", "powershell"} {
		t.Run(language, func(t *testing.T) {
			job := validJob()
			job.TargetUrl = ""
			job.ExecutorGroupId = "group"
			job.ExecutorHandler = "__script__"
			job.ScriptLanguage = language
			job.ScriptSource = "source"
			if err := validateJob(job); err != nil {
				t.Fatalf("validate %s: %v", language, err)
			}
		})
	}
}

func TestValidateDockerImageJob(t *testing.T) {
	t.Parallel()
	job := validJob()
	job.TargetUrl = ""
	job.ExecutorGroupId = "docker-group"
	job.ExecutorHandler = "__docker__"
	job.ScriptLanguage = "docker"
	job.ScriptSource = `{"image":"alpine:3.22","command":["echo"],"args":["hello"]}`
	if err := validateJob(job); err != nil {
		t.Fatal(err)
	}
	job.ExecutorHandler = "__script__"
	if err := validateJob(job); err == nil {
		t.Fatal("Docker job accepted script handler")
	}
}

func TestValidateJobNormalizesExecutorLabels(t *testing.T) {
	t.Parallel()
	job := validJob()
	job.TargetUrl = ""
	job.ExecutorGroupId = "group"
	job.ExecutorHandler = "handler"
	job.RequiredExecutorLabels = []string{" Linux ", "gpu", "linux"}
	job.ExcludedExecutorLabels = []string{"spot"}
	if err := validateJob(job); err != nil {
		t.Fatal(err)
	}
	if strings.Join(job.RequiredExecutorLabels, ",") != "gpu,linux" {
		t.Fatalf("required labels = %v", job.RequiredExecutorLabels)
	}
	job.ExcludedExecutorLabels = []string{"linux"}
	if err := validateJob(job); err == nil {
		t.Fatal("overlapping labels accepted")
	}
}

func TestScriptVersionRequestsRejectInvalidInputBeforeStore(t *testing.T) {
	t.Parallel()
	service := NewService(nil)
	if _, err := service.ListJobScriptVersions(t.Context(), &schedulerv1.ListJobScriptVersionsRequest{TenantId: "tenant"}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("list code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
	if _, err := service.RollbackJobScriptVersion(t.Context(), &schedulerv1.RollbackJobScriptVersionRequest{TenantId: "tenant", JobId: "job", VersionId: "version", JobVersion: 0}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("rollback version code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
	if _, err := service.RollbackJobScriptVersion(t.Context(), &schedulerv1.RollbackJobScriptVersionRequest{TenantId: "tenant", JobId: "job", VersionId: "version", JobVersion: 1, Remark: strings.Repeat("x", 201)}); status.Code(err) != codes.InvalidArgument {
		t.Fatalf("rollback remark code = %v, want InvalidArgument: %v", status.Code(err), err)
	}
}

func TestValidateTriggerRequest(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		req  *schedulerv1.TriggerJobRequest
		want bool
	}{
		{name: "valid", req: &schedulerv1.TriggerJobRequest{TenantId: "tenant", JobId: "job", IdempotencyKey: "deploy-42", Input: "payload"}},
		{name: "missing tenant", req: &schedulerv1.TriggerJobRequest{JobId: "job"}, want: true},
		{name: "missing job", req: &schedulerv1.TriggerJobRequest{TenantId: "tenant"}, want: true},
		{name: "idempotency key too large", req: &schedulerv1.TriggerJobRequest{TenantId: "tenant", JobId: "job", IdempotencyKey: strings.Repeat("k", 201)}, want: true},
		{name: "input too large", req: &schedulerv1.TriggerJobRequest{TenantId: "tenant", JobId: "job", Input: strings.Repeat("x", (1<<20)+1)}, want: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := validateTriggerRequest(tt.req)
			if (err != nil) != tt.want {
				t.Fatalf("validateTriggerRequest() error = %v, want error %v", err, tt.want)
			}
		})
	}
}

func TestNormalizeTriggerOverrideAddresses(t *testing.T) {
	t.Parallel()
	got, err := normalizeTriggerOverrideAddresses([]string{" https://worker-b:9999/ ", "http://worker-a:9999", "http://worker-a:9999/"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Join(got, ",") != "http://worker-a:9999,https://worker-b:9999" {
		t.Fatalf("addresses=%v", got)
	}
	for _, addresses := range [][]string{{"file:///tmp/worker"}, {"https://user:pass@worker:9999"}, {"worker:9999"}, make([]string, 101)} {
		if _, err = normalizeTriggerOverrideAddresses(addresses); err == nil {
			t.Fatalf("addresses %v accepted", addresses)
		}
	}
}

func TestNormalizePreviewScheduleRequest(t *testing.T) {
	t.Parallel()
	valid := &schedulerv1.PreviewScheduleRequest{ScheduleType: "cron", ScheduleExpression: "0 0 9 L * ?", Timezone: "UTC", Count: 5, After: timestamppb.New(time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC))}
	after, count, err := normalizePreviewScheduleRequest(valid, time.Now())
	if err != nil || count != 5 || !after.Equal(valid.After.AsTime()) {
		t.Fatalf("after=%s count=%d err=%v", after, count, err)
	}
	for _, mutate := range []func(*schedulerv1.PreviewScheduleRequest){
		func(r *schedulerv1.PreviewScheduleRequest) { r.ScheduleType = "" },
		func(r *schedulerv1.PreviewScheduleRequest) { r.ScheduleExpression = "" },
		func(r *schedulerv1.PreviewScheduleRequest) { r.Timezone = "Mars/Base" },
		func(r *schedulerv1.PreviewScheduleRequest) { r.Count = 101 },
		func(r *schedulerv1.PreviewScheduleRequest) { r.After = &timestamppb.Timestamp{Seconds: 253402300800} },
	} {
		request := proto.Clone(valid).(*schedulerv1.PreviewScheduleRequest)
		mutate(request)
		if _, _, err = normalizePreviewScheduleRequest(request, time.Now()); err == nil {
			t.Fatalf("request accepted: %+v", request)
		}
	}
}
