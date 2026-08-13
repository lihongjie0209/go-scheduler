//go:build integration

package store

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/lihongjie0209/go-scheduler/internal/cryptox"
	"github.com/lihongjie0209/go-scheduler/internal/schedule"
	"github.com/lihongjie0209/go-scheduler/migrations"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgreSQLSchedulingStateMachine(t *testing.T) {
	ctx := context.Background()
	projectRoot, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	container, err := postgres.Run(ctx, "", testcontainers.WithDockerfile(testcontainers.FromDockerfile{Context: projectRoot, Dockerfile: "deploy/postgres/Dockerfile", Repo: "go-scheduler-postgres-test", Tag: "16-partman-5.5.0", KeepImage: true}), postgres.WithDatabase("scheduler"), postgres.WithUsername("scheduler"), postgres.WithPassword("scheduler"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = container.Terminate(ctx) })
	dsn, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	conn, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range migrations.All {
		if _, err = conn.Exec(ctx, migration.SQL); err != nil {
			t.Fatalf("migration %d: %v", migration.Version, err)
		}
	}
	var parentConfigCount int
	if err = conn.QueryRow(ctx, `SELECT count(*) FROM partman.part_config WHERE parent_table='public.job_runs' AND partition_interval='1 mon' AND retention='90 days'`).Scan(&parentConfigCount); err != nil {
		t.Fatal(err)
	}
	if parentConfigCount != 1 {
		t.Fatal("pg_partman did not configure monthly partitions and retention")
	}
	var tenantID string
	if err = conn.QueryRow(ctx, `INSERT INTO tenants(name) VALUES('integration') RETURNING id`).Scan(&tenantID); err != nil {
		t.Fatal(err)
	}
	conn.Close(ctx)
	one, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer one.Close()
	maintenance, err := one.MaintainRunPartitions(ctx, time.Now(), 90*24*time.Hour)
	if err != nil {
		t.Fatalf("pg_partman maintenance: %v", err)
	}
	if maintenance.Backend != "pg_partman" {
		t.Fatalf("partition backend = %q, want pg_partman", maintenance.Backend)
	}
	two, err := New(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer two.Close()
	limitedAPI, err := New(ctx, dsn, WithPoolSize(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer limitedAPI.Close()
	limitedCore, err := New(ctx, dsn, WithPoolSize(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	defer limitedCore.Close()
	ownerOne, err := one.CreateUser(ctx, "owner-one@example.com", "hash", false)
	if err != nil {
		t.Fatal(err)
	}
	ownerTwo, err := one.CreateUser(ctx, "owner-two@example.com", "hash", false)
	if err != nil {
		t.Fatal(err)
	}
	if err = one.AddMembership(ctx, tenantID, ownerOne.ID, "owner"); err != nil {
		t.Fatal(err)
	}
	if err = one.AddMembership(ctx, tenantID, ownerTwo.ID, "owner"); err != nil {
		t.Fatal(err)
	}
	deleteStart := make(chan struct{})
	deleteResults := make(chan error, 2)
	for _, candidate := range []struct {
		store  *Store
		userID string
	}{{one, ownerOne.ID}, {two, ownerTwo.ID}} {
		go func(candidate struct {
			store  *Store
			userID string
		}) {
			<-deleteStart
			deleteResults <- candidate.store.DeleteMembership(ctx, tenantID, candidate.userID)
		}(candidate)
	}
	close(deleteStart)
	var ownerDeleted, ownerRejected int
	for range 2 {
		if deleteErr := <-deleteResults; deleteErr == nil {
			ownerDeleted++
		} else if errors.Is(deleteErr, ErrConflict) {
			ownerRejected++
		} else {
			t.Fatalf("concurrent owner deletion: %v", deleteErr)
		}
	}
	if ownerDeleted != 1 || ownerRejected != 1 {
		t.Fatalf("concurrent owner deletions: deleted=%d rejected=%d", ownerDeleted, ownerRejected)
	}
	var remainingOwners int
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM tenant_memberships WHERE tenant_id=$1 AND role='owner'`, tenantID).Scan(&remainingOwners); err != nil {
		t.Fatal(err)
	}
	if remainingOwners != 1 {
		t.Fatalf("remaining owners = %d, want 1", remainingOwners)
	}
	if stats := limitedAPI.PoolStats(); stats.MaxConnections != 1 {
		t.Fatalf("limited API max connections = %d, want 1", stats.MaxConnections)
	}
	heldAPIConnection, err := limitedAPI.pool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	apiWaitCtx, cancelAPIWait := context.WithTimeout(ctx, 50*time.Millisecond)
	if err = limitedAPI.Ping(apiWaitCtx); err == nil {
		cancelAPIWait()
		heldAPIConnection.Release()
		t.Fatal("saturated API pool unexpectedly acquired another connection")
	}
	cancelAPIWait()
	corePingCtx, cancelCorePing := context.WithTimeout(ctx, time.Second)
	if err = limitedCore.Ping(corePingCtx); err != nil {
		cancelCorePing()
		heldAPIConnection.Release()
		t.Fatalf("API pool saturation affected Core pool: %v", err)
	}
	cancelCorePing()
	heldAPIConnection.Release()
	routeGroup, err := one.CreateExecutorGroup(ctx, ExecutorGroup{TenantID: tenantID, Name: "workers", RouteStrategy: "round"})
	if err != nil {
		t.Fatal(err)
	}
	claimJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "concurrent-claim", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com/claim", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 5, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 2, MaxQueueSize: 10, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	for iteration := 0; iteration < 25; iteration++ {
		run, triggerErr := one.TriggerJob(ctx, tenantID, claimJob.ID, fmt.Sprintf("concurrent-claim-%d", iteration), "")
		if triggerErr != nil {
			t.Fatal(triggerErr)
		}
		start := make(chan struct{})
		results := make(chan []ClaimedRun, 2)
		errorsFound := make(chan error, 2)
		var claimers sync.WaitGroup
		for index, database := range []*Store{one, two} {
			claimers.Add(1)
			go func(index int, database *Store) {
				defer claimers.Done()
				<-start
				claims, claimErr := database.ClaimRuns(ctx, fmt.Sprintf("claim-owner-%d-%d", iteration, index), 1, time.Minute)
				results <- claims
				errorsFound <- claimErr
			}(index, database)
		}
		close(start)
		claimers.Wait()
		close(results)
		close(errorsFound)
		for claimErr := range errorsFound {
			if claimErr != nil {
				t.Fatal(claimErr)
			}
		}
		var claimed []ClaimedRun
		for claims := range results {
			for _, claim := range claims {
				if claim.Run.ID == run.ID {
					claimed = append(claimed, claim)
				}
			}
		}
		if len(claimed) != 1 {
			t.Fatalf("run %s concurrently claimed %d times", run.ID, len(claimed))
		}
		if err = one.CompleteRun(ctx, claimed[0].Run, true, http.StatusNoContent, "", ""); err != nil {
			t.Fatal(err)
		}
	}
	quartzJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "quartz-last-day", ScheduleType: "cron", ScheduleExpression: "0 0 9 L * ?", Timezone: "UTC", TargetURL: "https://example.com/quartz", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, Enabled: true})
	if err != nil || quartzJob.NextRunAt == nil {
		t.Fatalf("quartz job = %+v, %v", quartzJob, err)
	}
	quartzNext := quartzJob.NextRunAt.UTC()
	lastDay := time.Date(quartzNext.Year(), quartzNext.Month()+1, 0, 0, 0, 0, 0, time.UTC).Day()
	if quartzNext.Day() != lastDay || quartzNext.Hour() != 9 || quartzNext.Minute() != 0 || quartzNext.Second() != 0 {
		t.Fatalf("quartz next = %s, want last day at 09:00 UTC", quartzNext)
	}
	loadedQuartz, err := two.GetJob(ctx, tenantID, quartzJob.ID)
	if err != nil || loadedQuartz.NextRunAt == nil || !loadedQuartz.NextRunAt.Equal(*quartzJob.NextRunAt) || loadedQuartz.ScheduleExpression != "0 0 9 L * ?" {
		t.Fatalf("loaded quartz = %+v, %v", loadedQuartz, err)
	}
	manualGroup, err := one.CreateExecutorGroup(ctx, ExecutorGroup{TenantID: tenantID, Name: "manual-workers", RouteStrategy: "round", RegistrationMode: "manual", ManualAddresses: []string{"http://manual-a:9999", "http://manual-b:9999"}})
	if err != nil {
		t.Fatal(err)
	}
	manualGroups, err := two.ListExecutorGroups(ctx, tenantID)
	if err != nil {
		t.Fatal(err)
	}
	var listedManual ExecutorGroup
	for _, group := range manualGroups {
		if group.ID == manualGroup.ID {
			listedManual = group
		}
	}
	if listedManual.RegistrationMode != "manual" || len(listedManual.ManualAddresses) != 2 {
		t.Fatalf("manual group = %+v", listedManual)
	}
	manualNodes, err := two.ListExecutorNodes(ctx, tenantID, manualGroup.ID, true)
	if err != nil || len(manualNodes) != 2 || !manualNodes[0].Static || !manualNodes[1].Static {
		t.Fatalf("manual nodes = %+v, %v", manualNodes, err)
	}
	if _, err = one.RegisterExecutorNode(ctx, tenantID, manualGroup.ID, "dynamic", "http://dynamic:9999", 30*time.Second); err != ErrRegistrationMode {
		t.Fatalf("manual heartbeat error = %v, want ErrRegistrationMode", err)
	}
	manualJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "manual-route", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, ExecutorGroupID: manualGroup.ID, ExecutorHandler: "manual", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if err = two.DeleteExecutorGroup(ctx, tenantID, manualGroup.ID, manualGroup.Version); err != ErrExecutorGroupInUse {
		t.Fatalf("delete referenced group = %v, want ErrExecutorGroupInUse", err)
	}
	selectedManual, err := one.ReserveExecutorRoute(ctx, tenantID, manualGroup.ID, manualJob.ID, func(snapshot ExecutorRoutingSnapshot) (ExecutorNode, error) {
		if len(snapshot.Nodes) != 2 || !snapshot.Nodes[0].Static {
			return ExecutorNode{}, fmt.Errorf("manual routing snapshot = %+v", snapshot)
		}
		return snapshot.Nodes[0], nil
	})
	if err != nil || selectedManual.Address != "http://manual-a:9999" {
		t.Fatalf("selected manual = %+v, %v", selectedManual, err)
	}
	manualGroup.Name = "manual-workers-updated"
	manualGroup.RouteStrategy = "last"
	manualGroup.ManualAddresses = []string{"http://manual-c:9999"}
	updatedManual, err := two.UpdateExecutorGroup(ctx, manualGroup)
	if err != nil || updatedManual.Version != manualGroup.Version+1 {
		t.Fatalf("updated manual = %+v, %v", updatedManual, err)
	}
	if _, err = one.UpdateExecutorGroup(ctx, manualGroup); err != ErrConflict {
		t.Fatalf("stale manual update = %v, want ErrConflict", err)
	}
	manualNodes, err = one.ListExecutorNodes(ctx, tenantID, manualGroup.ID, true)
	if err != nil || len(manualNodes) != 1 || manualNodes[0].Address != "http://manual-c:9999" || !manualNodes[0].Static {
		t.Fatalf("updated manual nodes = %+v, %v", manualNodes, err)
	}
	deletableGroup, err := one.CreateExecutorGroup(ctx, ExecutorGroup{TenantID: tenantID, Name: "delete-manual", RouteStrategy: "first", RegistrationMode: "manual", ManualAddresses: []string{"http://delete:9999"}})
	if err != nil {
		t.Fatal(err)
	}
	if err = two.DeleteExecutorGroup(ctx, tenantID, deletableGroup.ID, deletableGroup.Version); err != nil {
		t.Fatal(err)
	}
	if err = one.DeleteExecutorGroup(ctx, tenantID, deletableGroup.ID, deletableGroup.Version); err != ErrConflict {
		t.Fatalf("repeated delete = %v, want ErrConflict", err)
	}
	overrideGroup, err := one.CreateExecutorGroup(ctx, ExecutorGroup{TenantID: tenantID, Name: "override-workers", RouteStrategy: "round"})
	if err != nil {
		t.Fatal(err)
	}
	overrideJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "override-route", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 2, MaxQueueSize: 10, ExecutorGroupID: overrideGroup.ID, ExecutorHandler: "override", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	overrideAddresses := []string{"http://override-a:9999", "http://override-b:9999"}
	firstOverrideRun, err := one.TriggerJobWithOptions(ctx, tenantID, overrideJob.ID, "override-1", "first", TriggerOptions{OverrideAddresses: overrideAddresses})
	if err != nil || strings.Join(firstOverrideRun.OverrideAddresses, ",") != strings.Join(overrideAddresses, ",") {
		t.Fatalf("first override run = %+v, %v", firstOverrideRun, err)
	}
	loadedOverrideRun, err := two.GetRun(ctx, tenantID, firstOverrideRun.ID)
	if err != nil || strings.Join(loadedOverrideRun.OverrideAddresses, ",") != strings.Join(overrideAddresses, ",") {
		t.Fatalf("loaded override run = %+v, %v", loadedOverrideRun, err)
	}
	selectRound := func(snapshot ExecutorRoutingSnapshot) (ExecutorNode, error) {
		if snapshot.Strategy != "round" || len(snapshot.Nodes) != 2 {
			return ExecutorNode{}, fmt.Errorf("override snapshot = %+v", snapshot)
		}
		return snapshot.Nodes[int(snapshot.Cursor%int64(len(snapshot.Nodes)))], nil
	}
	firstOverrideNode, err := one.ReserveExecutorOverrideRoute(ctx, tenantID, overrideGroup.ID, overrideJob.ID, overrideAddresses, selectRound)
	if err != nil || firstOverrideNode.Address != overrideAddresses[0] {
		t.Fatalf("first override node = %+v, %v", firstOverrideNode, err)
	}
	secondOverrideNode, err := two.ReserveExecutorOverrideRoute(ctx, tenantID, overrideGroup.ID, overrideJob.ID, overrideAddresses, selectRound)
	if err != nil || secondOverrideNode.Address != overrideAddresses[1] {
		t.Fatalf("second override node = %+v, %v", secondOverrideNode, err)
	}
	if _, err = one.CancelRun(ctx, tenantID, firstOverrideRun.ID, "module test cleanup"); err != nil {
		t.Fatal(err)
	}
	directJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "direct-no-override", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = one.TriggerJobWithOptions(ctx, tenantID, directJob.ID, "", "", TriggerOptions{OverrideAddresses: overrideAddresses}); err != ErrOverrideRequiresExecutorGroup {
		t.Fatalf("direct override = %v, want ErrOverrideRequiresExecutorGroup", err)
	}
	var jobsBefore, runsBefore int
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE tenant_id=$1`, tenantID).Scan(&jobsBefore); err != nil {
		t.Fatal(err)
	}
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE tenant_id=$1`, tenantID).Scan(&runsBefore); err != nil {
		t.Fatal(err)
	}
	preview, err := schedule.Preview("cron", "0 0 9 L * ?", "UTC", time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC), 5)
	if err != nil || len(preview) != 5 || !preview[0].Equal(time.Date(2026, 8, 31, 9, 0, 0, 0, time.UTC)) {
		t.Fatalf("schedule preview = %v, %v", preview, err)
	}
	var jobsAfter, runsAfter int
	if err = two.pool.QueryRow(ctx, `SELECT count(*) FROM jobs WHERE tenant_id=$1`, tenantID).Scan(&jobsAfter); err != nil {
		t.Fatal(err)
	}
	if err = two.pool.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE tenant_id=$1`, tenantID).Scan(&runsAfter); err != nil {
		t.Fatal(err)
	}
	if jobsAfter != jobsBefore || runsAfter != runsBefore {
		t.Fatalf("preview mutated PG: jobs %d→%d runs %d→%d", jobsBefore, jobsAfter, runsBefore, runsAfter)
	}
	scriptJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "script-persistence", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, ExecutorGroupID: routeGroup.ID, ExecutorHandler: "__script__", ScriptLanguage: "shell", ScriptSource: `printf '%s' "$SCHEDULER_INPUT"`, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	loadedScript, err := two.GetJob(ctx, tenantID, scriptJob.ID)
	if err != nil || loadedScript.ScriptLanguage != "shell" || loadedScript.ScriptSource != scriptJob.ScriptSource || loadedScript.ExecutorHandler != "__script__" {
		t.Fatalf("script job = %+v, %v", loadedScript, err)
	}
	for _, language := range []string{"nodejs", "php", "powershell", "docker"} {
		handler := "__script__"
		if language == "docker" {
			handler = "__docker__"
		}
		languageJob, createErr := one.CreateJob(ctx, Job{TenantID: tenantID, Name: language + "-persistence", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, ExecutorGroupID: routeGroup.ID, ExecutorHandler: handler, ScriptLanguage: language, ScriptSource: "source", Enabled: false})
		if createErr != nil {
			t.Fatalf("create %s script: %v", language, createErr)
		}
		loadedLanguageJob, getErr := two.GetJob(ctx, tenantID, languageJob.ID)
		languageVersions, versionsErr := two.ListJobScriptVersions(ctx, tenantID, languageJob.ID)
		if getErr != nil || versionsErr != nil || loadedLanguageJob.ScriptLanguage != language || len(languageVersions) != 1 || languageVersions[0].ScriptLanguage != language {
			t.Fatalf("%s job=%+v versions=%+v errors=%v/%v", language, loadedLanguageJob, languageVersions, getErr, versionsErr)
		}
	}
	versions, err := two.ListJobScriptVersions(ctx, tenantID, scriptJob.ID)
	if err != nil || len(versions) != 1 || versions[0].Revision != 1 || versions[0].ScriptSource != scriptJob.ScriptSource || versions[0].Remark != "initial version" {
		t.Fatalf("initial script versions = %+v, %v", versions, err)
	}
	loadedScript.Description = "configuration only"
	loadedScript, err = one.UpdateJob(ctx, loadedScript)
	if err != nil {
		t.Fatal(err)
	}
	versions, err = two.ListJobScriptVersions(ctx, tenantID, scriptJob.ID)
	if err != nil || len(versions) != 1 {
		t.Fatalf("configuration-only script versions = %+v, %v", versions, err)
	}
	loadedScript.ScriptSource = `printf 'v2:%s' "$SCHEDULER_INPUT"`
	loadedScript, err = two.UpdateJob(ctx, loadedScript)
	if err != nil {
		t.Fatal(err)
	}
	versions, err = one.ListJobScriptVersions(ctx, tenantID, scriptJob.ID)
	if err != nil || len(versions) != 2 || versions[0].Revision != 2 || versions[0].ScriptSource != loadedScript.ScriptSource {
		t.Fatalf("updated script versions = %+v, %v", versions, err)
	}
	staleVersion := loadedScript.Version
	rolledBack, err := one.RollbackJobScriptVersion(ctx, tenantID, scriptJob.ID, versions[1].ID, staleVersion, "restore stable")
	if err != nil || rolledBack.ScriptSource != scriptJob.ScriptSource || rolledBack.Version != staleVersion+1 {
		t.Fatalf("rolled back script = %+v, %v", rolledBack, err)
	}
	if _, err = two.RollbackJobScriptVersion(ctx, tenantID, scriptJob.ID, versions[0].ID, staleVersion, "stale"); err != ErrConflict {
		t.Fatalf("stale rollback = %v, want ErrConflict", err)
	}
	versions, err = two.ListJobScriptVersions(ctx, tenantID, scriptJob.ID)
	if err != nil || len(versions) != 3 || versions[0].Revision != 3 || versions[0].Remark != "restore stable" || versions[0].ScriptSource != scriptJob.ScriptSource {
		t.Fatalf("rollback audit versions = %+v, %v", versions, err)
	}
	if _, err = one.RegisterExecutorNode(ctx, tenantID, routeGroup.ID, "node-b", "http://worker-b:9999", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if _, err = two.RegisterExecutorNode(ctx, tenantID, routeGroup.ID, "node-a", "http://worker-a:9999", 30*time.Second, []string{"gpu", "linux"}); err != nil {
		t.Fatal(err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE executor_nodes SET expires_at=now()-interval '1 second' WHERE group_id=$1 AND node_id='node-b'`, routeGroup.ID); err != nil {
		t.Fatal(err)
	}
	liveNodes, err := two.ListExecutorNodes(ctx, tenantID, routeGroup.ID, true)
	if err != nil || len(liveNodes) != 1 || liveNodes[0].NodeID != "node-a" {
		t.Fatalf("live nodes = %+v, %v", liveNodes, err)
	}
	if strings.Join(liveNodes[0].Labels, ",") != "gpu,linux" {
		t.Fatalf("node labels = %v", liveNodes[0].Labels)
	}
	unregisterGroup, err := one.CreateExecutorGroup(ctx, ExecutorGroup{TenantID: tenantID, Name: "unregister-workers", RouteStrategy: "first"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = one.RegisterExecutorNode(ctx, tenantID, unregisterGroup.ID, "node-remove", "http://remove:9999", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	if err = two.UnregisterExecutorNode(ctx, tenantID, unregisterGroup.ID, "node-remove"); err != nil {
		t.Fatal(err)
	}
	if err = one.UnregisterExecutorNode(ctx, tenantID, unregisterGroup.ID, "node-remove"); err != nil {
		t.Fatalf("repeated unregister = %v", err)
	}
	removedNodes, err := one.ListExecutorNodes(ctx, tenantID, unregisterGroup.ID, false)
	if err != nil || len(removedNodes) != 0 {
		t.Fatalf("unregistered nodes = %+v, %v", removedNodes, err)
	}
	if err = one.UnregisterExecutorNode(ctx, tenantID, manualGroup.ID, manualNodes[0].NodeID); err != ErrRegistrationMode {
		t.Fatalf("manual unregister = %v, want ErrRegistrationMode", err)
	}
	broadcastGroup, err := one.CreateExecutorGroup(ctx, ExecutorGroup{TenantID: tenantID, Name: "broadcast-workers", RouteStrategy: "sharding_broadcast"})
	if err != nil {
		t.Fatal(err)
	}
	for _, node := range []struct{ id, address string }{{"node-b", "http://worker-b:9999"}, {"node-a", "http://worker-a:9999"}} {
		if _, err = one.RegisterExecutorNode(ctx, tenantID, broadcastGroup.ID, node.id, node.address, 30*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	broadcastJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "broadcast-manual", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, MaxRetries: 1, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, ExecutorGroupID: broadcastGroup.ID, ExecutorHandler: "broadcastHandler", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	primary, err := one.TriggerJob(ctx, tenantID, broadcastJob.ID, "broadcast-key", "payload")
	if err != nil {
		t.Fatal(err)
	}
	shards, err := two.ListRunsFiltered(ctx, tenantID, broadcastJob.ID, primary.BroadcastGroupID, 10)
	if err != nil || len(shards) != 2 || shards[0].ShardIndex != 0 || shards[0].ExecutorNodeID != "node-a" || shards[1].ShardIndex != 1 || shards[1].ExecutorNodeID != "node-b" {
		t.Fatalf("broadcast shards = %+v, %v", shards, err)
	}
	repeated, err := two.TriggerJob(ctx, tenantID, broadcastJob.ID, "broadcast-key", "ignored")
	if err != nil || repeated.ID != primary.ID || repeated.BroadcastGroupID != primary.BroadcastGroupID || repeated.ShardTotal != 2 {
		t.Fatalf("idempotent broadcast = %+v, %v", repeated, err)
	}
	broadcastClaims, err := one.ClaimRuns(ctx, "broadcast-owner", 2, time.Minute)
	if err != nil || len(broadcastClaims) != 2 {
		t.Fatalf("broadcast claims = %+v, %v", broadcastClaims, err)
	}
	var firstShard *Run
	for i := range broadcastClaims {
		if broadcastClaims[i].Run.BroadcastGroupID == primary.BroadcastGroupID && broadcastClaims[i].Run.ShardIndex == 0 {
			copy := broadcastClaims[i].Run
			firstShard = &copy
		}
	}
	if firstShard == nil {
		t.Fatalf("missing first shard in claims: %+v", broadcastClaims)
	}
	logToken := []byte("test-run-token-hash")
	if err = one.ActivateRunToken(ctx, firstShard.ID, logToken, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err = two.AppendRunLogs(ctx, firstShard.ID, []byte("wrong-token"), []RunLogInput{{EntryID: "line-1", Stream: "stdout", Content: "started"}}); err != ErrNotFound {
		t.Fatalf("wrong log token = %v, want ErrNotFound", err)
	}
	firstCursor, err := two.AppendRunLogs(ctx, firstShard.ID, logToken, []RunLogInput{{EntryID: "line-1", Stream: "stdout", Content: "started"}, {EntryID: "line-2", Stream: "stderr", Content: "warning"}})
	if err != nil {
		t.Fatal(err)
	}
	duplicateCursor, err := one.AppendRunLogs(ctx, firstShard.ID, logToken, []RunLogInput{{EntryID: "line-1", Stream: "stdout", Content: "changed must be ignored"}})
	if err != nil || duplicateCursor >= firstCursor {
		t.Fatalf("duplicate cursor = %d, first = %d, err = %v", duplicateCursor, firstCursor, err)
	}
	firstPage, cursor, err := one.ListRunLogs(ctx, tenantID, firstShard.ID, 0, 1)
	if err != nil || len(firstPage) != 1 || firstPage[0].Content != "started" {
		t.Fatalf("first log page = %+v, cursor=%d, err=%v", firstPage, cursor, err)
	}
	secondPage, nextCursor, err := two.ListRunLogs(ctx, tenantID, firstShard.ID, cursor, 10)
	if err != nil || len(secondPage) != 1 || secondPage[0].Content != "warning" || nextCursor != firstCursor {
		t.Fatalf("second log page = %+v, cursor=%d, err=%v", secondPage, nextCursor, err)
	}
	var otherTenantID string
	if err = one.pool.QueryRow(ctx, `INSERT INTO tenants(name) VALUES('log-isolation') RETURNING id`).Scan(&otherTenantID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = one.ListRunLogs(ctx, otherTenantID, firstShard.ID, 0, 10); err != ErrNotFound {
		t.Fatalf("cross-tenant logs = %v, want ErrNotFound", err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE job_run_logs SET created_at=now()-interval '2 hours' WHERE run_id=$1`, firstShard.ID); err != nil {
		t.Fatal(err)
	}
	if err = one.CleanupAuxiliaryHistory(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	cleanedLogs, _, err := one.ListRunLogs(ctx, tenantID, firstShard.ID, 0, 10)
	if err != nil || len(cleanedLogs) != 0 {
		t.Fatalf("cleaned logs = %+v, %v", cleanedLogs, err)
	}
	if _, err = one.pool.Exec(ctx, `INSERT INTO job_run_logs(tenant_id,run_id,entry_id,stream,content,created_at)
		SELECT $1,$2,'cleanup-'||n,'stdout','old',now()-interval '2 hours'
		FROM generate_series(1,$3) AS n`, tenantID, firstShard.ID, historyCleanupBatchSize+1); err != nil {
		t.Fatal(err)
	}
	if err = one.CleanupAuxiliaryHistory(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	var cleanupCount int
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM job_run_logs WHERE run_id=$1 AND entry_id LIKE 'cleanup-%'`, firstShard.ID).Scan(&cleanupCount); err != nil || cleanupCount != 1 {
		t.Fatalf("bounded cleanup remaining logs = %d, want 1: %v", cleanupCount, err)
	}
	if err = one.CleanupAuxiliaryHistory(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM job_run_logs WHERE run_id=$1 AND entry_id LIKE 'cleanup-%'`, firstShard.ID).Scan(&cleanupCount); err != nil || cleanupCount != 0 {
		t.Fatalf("repeated cleanup remaining logs = %d, want 0: %v", cleanupCount, err)
	}
	delay := time.Second
	broadcastRetry, err := one.FailRun(ctx, *firstShard, "failed", 500, "retry shard", &delay)
	if err != nil || broadcastRetry.BroadcastGroupID != primary.BroadcastGroupID || broadcastRetry.ShardIndex != 0 || broadcastRetry.ExecutorNodeID != "node-a" {
		t.Fatalf("broadcast retry = %+v, %v", broadcastRetry, err)
	}
	scheduledJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "broadcast-scheduled", ScheduleType: "fixed_delay", ScheduleExpression: "2", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, ExecutorGroupID: broadcastGroup.ID, ExecutorHandler: "broadcastHandler", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE jobs SET next_run_at=now()-interval '1 second' WHERE id=$1`, scheduledJob.ID); err != nil {
		t.Fatal(err)
	}
	if err = one.EnqueueDue(ctx, 10); err != nil {
		t.Fatal(err)
	}
	scheduledRuns, err := one.ListRuns(ctx, tenantID, scheduledJob.ID, 10)
	if err != nil || len(scheduledRuns) != 2 || scheduledRuns[0].BroadcastGroupID == "" || scheduledRuns[0].BroadcastGroupID != scheduledRuns[1].BroadcastGroupID {
		t.Fatalf("scheduled broadcast runs = %+v, %v", scheduledRuns, err)
	}
	fixedDelayJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "fixed-delay", ScheduleType: "fixed_delay", ScheduleExpression: "2", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, MaxRetries: 1, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE jobs SET next_run_at=now()-interval '1 second' WHERE id=$1`, fixedDelayJob.ID); err != nil {
		t.Fatal(err)
	}
	if err = one.EnqueueDue(ctx, 10); err != nil {
		t.Fatal(err)
	}
	fixedDelayState, err := two.GetJob(ctx, tenantID, fixedDelayJob.ID)
	if err != nil || !fixedDelayState.Enabled || fixedDelayState.NextRunAt != nil {
		t.Fatalf("fixed delay state while pending = %+v, %v", fixedDelayState, err)
	}
	if err = two.EnqueueDue(ctx, 10); err != nil {
		t.Fatal(err)
	}
	fixedDelayRuns, err := one.ListRuns(ctx, tenantID, fixedDelayJob.ID, 10)
	if err != nil || len(fixedDelayRuns) != 1 || !fixedDelayRuns[0].RescheduleOnTerminal {
		t.Fatalf("fixed delay runs = %+v, %v", fixedDelayRuns, err)
	}
	fixedClaims, err := one.ClaimRuns(ctx, "fixed-delay-owner", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var broadcastFixedClaims []Run
	for _, claim := range fixedClaims {
		if claim.Job.ID == scheduledJob.ID {
			broadcastFixedClaims = append(broadcastFixedClaims, claim.Run)
		}
	}
	if len(broadcastFixedClaims) != 2 {
		t.Fatalf("fixed-delay broadcast claims = %+v", broadcastFixedClaims)
	}
	if err = one.CompleteRun(ctx, broadcastFixedClaims[0], true, http.StatusOK, "done", ""); err != nil {
		t.Fatal(err)
	}
	broadcastState, err := one.GetJob(ctx, tenantID, scheduledJob.ID)
	if err != nil || broadcastState.NextRunAt != nil {
		t.Fatalf("broadcast fixed delay rearmed before all shards: %+v, %v", broadcastState, err)
	}
	broadcastTerminalBefore := time.Now().UTC()
	if err = two.CompleteRun(ctx, broadcastFixedClaims[1], true, http.StatusOK, "done", ""); err != nil {
		t.Fatal(err)
	}
	broadcastState, err = one.GetJob(ctx, tenantID, scheduledJob.ID)
	if err != nil || broadcastState.NextRunAt == nil || broadcastState.NextRunAt.Before(broadcastTerminalBefore.Add(1900*time.Millisecond)) {
		t.Fatalf("broadcast fixed delay did not rearm after all shards: %+v, %v", broadcastState, err)
	}
	var fixedClaim Run
	for _, claim := range fixedClaims {
		if claim.Job.ID == fixedDelayJob.ID {
			fixedClaim = claim.Run
		}
	}
	if fixedClaim.ID == "" {
		t.Fatalf("fixed delay run not claimed: %+v", fixedClaims)
	}
	zeroRetryDelay := time.Duration(0)
	fixedRetry, err := one.FailRun(ctx, fixedClaim, "failed", 500, "retry fixed delay", &zeroRetryDelay)
	if err != nil || fixedRetry == nil || !fixedRetry.RescheduleOnTerminal {
		t.Fatalf("fixed delay retry = %+v, %v", fixedRetry, err)
	}
	fixedDelayState, err = one.GetJob(ctx, tenantID, fixedDelayJob.ID)
	if err != nil || fixedDelayState.NextRunAt != nil {
		t.Fatalf("fixed delay rearmed before final retry = %+v, %v", fixedDelayState, err)
	}
	fixedClaims, err = two.ClaimRuns(ctx, "fixed-delay-retry-owner", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	fixedClaim = Run{}
	for _, claim := range fixedClaims {
		if claim.Run.ID == fixedRetry.ID {
			fixedClaim = claim.Run
		}
	}
	terminalBefore := time.Now().UTC()
	if fixedClaim.ID == "" {
		t.Fatalf("fixed delay retry not claimed: %+v", fixedClaims)
	}
	if _, err = two.FailRun(ctx, fixedClaim, "failed", 500, "final fixed delay failure", nil); err != nil {
		t.Fatal(err)
	}
	fixedDelayState, err = one.GetJob(ctx, tenantID, fixedDelayJob.ID)
	if err != nil || fixedDelayState.NextRunAt == nil || fixedDelayState.NextRunAt.Before(terminalBefore.Add(1900*time.Millisecond)) || fixedDelayState.NextRunAt.After(time.Now().Add(2200*time.Millisecond)) {
		t.Fatalf("fixed delay final next = %+v, %v", fixedDelayState.NextRunAt, err)
	}
	restartJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "fixed-delay-restart", ScheduleType: "fixed_delay", ScheduleExpression: "2", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 10, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE jobs SET next_run_at=now()-interval '1 second' WHERE id=$1`, restartJob.ID); err != nil {
		t.Fatal(err)
	}
	if err = one.EnqueueDue(ctx, 10); err != nil {
		t.Fatal(err)
	}
	restartClaims, err := one.ClaimRuns(ctx, "fixed-delay-restart-owner", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var staleClaim Run
	for _, claim := range restartClaims {
		if claim.Job.ID == restartJob.ID {
			staleClaim = claim.Run
		}
	}
	if staleClaim.ID == "" {
		t.Fatal("fixed delay restart run was not claimed")
	}
	stopped, err := one.SetJobEnabled(ctx, tenantID, restartJob.ID, false, restartJob.Version)
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := one.SetJobEnabled(ctx, tenantID, restartJob.ID, true, stopped.Version)
	if err != nil || restarted.NextRunAt == nil {
		t.Fatalf("restarted fixed delay = %+v, %v", restarted, err)
	}
	restartedNext := *restarted.NextRunAt
	if err = two.CompleteRun(ctx, staleClaim, true, http.StatusOK, "stale completion", ""); err != nil {
		t.Fatal(err)
	}
	afterStaleCompletion, err := two.GetJob(ctx, tenantID, restartJob.ID)
	if err != nil || afterStaleCompletion.NextRunAt == nil || !afterStaleCompletion.NextRunAt.Equal(restartedNext) {
		t.Fatalf("stale completion overwrote restart schedule: before=%s after=%+v err=%v", restartedNext, afterStaleCompletion.NextRunAt, err)
	}
	routedJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "routed-job", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, ExecutorGroupID: routeGroup.ID, ExecutorHandler: "demoHandler", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	loadedRoutedJob, err := two.GetJob(ctx, tenantID, routedJob.ID)
	if err != nil || loadedRoutedJob.ExecutorGroupID != routeGroup.ID || loadedRoutedJob.ExecutorHandler != "demoHandler" {
		t.Fatalf("routed job = %+v, %v", loadedRoutedJob, err)
	}
	if _, err = one.RegisterExecutorNode(ctx, tenantID, routeGroup.ID, "node-b", "http://worker-b:9999", 30*time.Second); err != nil {
		t.Fatal(err)
	}
	parallelJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "parallel-routed-job", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, ExecutorGroupID: routeGroup.ID, ExecutorHandler: "demoHandler", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	firstEntered, releaseFirst := make(chan struct{}), make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, routeErr := one.ReserveExecutorRoute(ctx, tenantID, routeGroup.ID, routedJob.ID, func(snapshot ExecutorRoutingSnapshot) (ExecutorNode, error) {
			close(firstEntered)
			<-releaseFirst
			return snapshot.Nodes[0], nil
		})
		firstDone <- routeErr
	}()
	<-firstEntered
	secondEntered := make(chan struct{})
	secondDone := make(chan error, 1)
	go func() {
		_, routeErr := two.ReserveExecutorRoute(ctx, tenantID, routeGroup.ID, parallelJob.ID, func(snapshot ExecutorRoutingSnapshot) (ExecutorNode, error) {
			close(secondEntered)
			return snapshot.Nodes[0], nil
		})
		secondDone <- routeErr
	}()
	select {
	case <-secondEntered:
	case <-time.After(time.Second):
		close(releaseFirst)
		t.Fatal("different jobs were serialized by the shared executor group row")
	}
	close(releaseFirst)
	if err = <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err = <-secondDone; err != nil {
		t.Fatal(err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE executor_groups SET route_strategy='lfu' WHERE id=$1`, routeGroup.ID); err != nil {
		t.Fatal(err)
	}
	selectLeastUsed := func(snapshot ExecutorRoutingSnapshot) (ExecutorNode, error) {
		if len(snapshot.Nodes) != 2 {
			return ExecutorNode{}, fmt.Errorf("live nodes = %d", len(snapshot.Nodes))
		}
		selected := snapshot.Nodes[0]
		for _, node := range snapshot.Nodes[1:] {
			if node.UseCount < selected.UseCount || node.UseCount == selected.UseCount && node.NodeID < selected.NodeID {
				selected = node
			}
		}
		return selected, nil
	}
	var routeWG sync.WaitGroup
	routedNodes := make(chan string, 2)
	routeErrors := make(chan error, 2)
	for _, database := range []*Store{one, two} {
		database := database
		routeWG.Add(1)
		go func() {
			defer routeWG.Done()
			node, routeErr := database.ReserveExecutorRoute(ctx, tenantID, routeGroup.ID, routedJob.ID, selectLeastUsed)
			if routeErr != nil {
				routeErrors <- routeErr
				return
			}
			routedNodes <- node.NodeID
		}()
	}
	routeWG.Wait()
	close(routeErrors)
	for routeErr := range routeErrors {
		t.Fatal(routeErr)
	}
	close(routedNodes)
	selectedNodes := map[string]int{}
	for nodeID := range routedNodes {
		selectedNodes[nodeID]++
	}
	if selectedNodes["node-a"] != 1 || selectedNodes["node-b"] != 1 {
		t.Fatalf("atomic LFU selections = %v", selectedNodes)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE executor_groups SET route_strategy='failover' WHERE id=$1`, routeGroup.ID); err != nil {
		t.Fatal(err)
	}
	activeStrategy, activeCandidates, err := two.ExecutorRouteCandidates(ctx, tenantID, routeGroup.ID, routedJob.ID)
	if err != nil || activeStrategy != "failover" || len(activeCandidates) != 2 || activeCandidates[0].NodeID != "node-a" {
		t.Fatalf("active candidates = %q %+v, %v", activeStrategy, activeCandidates, err)
	}
	newPolicyJob := func(name, policy string) Job {
		t.Helper()
		created, createErr := one.CreateJob(ctx, Job{TenantID: tenantID, Name: name, ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: policy, MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false})
		if createErr != nil {
			t.Fatal(createErr)
		}
		return created
	}
	retryJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "retry-state-machine", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, MaxRetries: 1, OverlapPolicy: "serial", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	firstAttempt, err := one.TriggerJob(ctx, tenantID, retryJob.ID, "retry-state", "payload")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := one.ClaimRuns(ctx, "retry-core", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	firstClaim, ok := claimForRun(claims, firstAttempt.ID)
	if !ok {
		t.Fatal("first retry attempt was not claimed")
	}
	zeroDelay := time.Duration(0)
	retry, err := one.FailRun(ctx, firstClaim.Run, "timed_out", 0, "deadline exceeded", &zeroDelay)
	if err != nil {
		t.Fatal(err)
	}
	if retry == nil || retry.Attempt != 2 || retry.TriggerType != "retry" || retry.RetryOfRunID != firstAttempt.ID || retry.RuntimeInput != "payload" {
		t.Fatalf("retry run = %+v", retry)
	}
	storedFirst, err := two.GetRun(ctx, tenantID, firstAttempt.ID)
	if err != nil || storedFirst.Status != "timed_out" {
		t.Fatalf("first attempt = %+v, %v", storedFirst, err)
	}
	var alarmCount int
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload->>'run_id'=$1`, firstAttempt.ID).Scan(&alarmCount); err != nil || alarmCount != 0 {
		t.Fatalf("intermediate alarm count = %d, %v", alarmCount, err)
	}
	claims, err = two.ClaimRuns(ctx, "retry-core-2", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	retryClaim, ok := claimForRun(claims, retry.ID)
	if !ok {
		t.Fatal("retry attempt was not claimed")
	}
	if _, err = two.FailRun(ctx, retryClaim.Run, "failed", http.StatusInternalServerError, "still failing", nil); err != nil {
		t.Fatal(err)
	}
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload->>'run_id'=$1`, retry.ID).Scan(&alarmCount); err != nil || alarmCount != 1 {
		t.Fatalf("final alarm count = %d, %v", alarmCount, err)
	}
	parentJob := newPolicyJob("dependency-parent", "serial")
	childJob := newPolicyJob("dependency-child", "serial")
	if err = one.SetJobDependencies(ctx, tenantID, parentJob.ID, []string{childJob.ID}); err != nil {
		t.Fatal(err)
	}
	if err = two.SetJobDependencies(ctx, tenantID, childJob.ID, []string{parentJob.ID}); err != ErrDependencyCycle {
		t.Fatalf("dependency cycle = %v, want ErrDependencyCycle", err)
	}
	parentRun, err := one.TriggerJob(ctx, tenantID, parentJob.ID, "dependency-success", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err = one.ClaimRuns(ctx, "dependency-core", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	parentClaim, ok := claimForRun(claims, parentRun.ID)
	if !ok {
		t.Fatal("parent run was not claimed")
	}
	if err = one.CompleteRun(ctx, parentClaim.Run, true, http.StatusOK, "done", ""); err != nil {
		t.Fatal(err)
	}
	childRuns, err := two.ListRuns(ctx, tenantID, childJob.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(childRuns) != 1 || childRuns[0].TriggerType != "dependency" || childRuns[0].ParentRunID != parentRun.ID {
		t.Fatalf("child runs = %+v", childRuns)
	}
	if err = one.CompleteRun(ctx, parentClaim.Run, true, http.StatusOK, "again", ""); err != ErrConflict {
		t.Fatalf("duplicate parent completion = %v", err)
	}
	childRuns, err = two.ListRuns(ctx, tenantID, childJob.ID, 10)
	if err != nil || len(childRuns) != 1 {
		t.Fatalf("dependency dispatched more than once: %+v %v", childRuns, err)
	}
	failedParent, err := one.TriggerJob(ctx, tenantID, parentJob.ID, "dependency-failure", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err = one.ClaimRuns(ctx, "dependency-core", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	failedClaim, ok := claimForRun(claims, failedParent.ID)
	if !ok {
		t.Fatal("failed parent not claimed")
	}
	if err = one.CompleteRun(ctx, failedClaim.Run, false, http.StatusInternalServerError, "", "failed"); err != nil {
		t.Fatal(err)
	}
	childRuns, err = two.ListRuns(ctx, tenantID, childJob.ID, 10)
	if err != nil || len(childRuns) != 1 {
		t.Fatalf("failure triggered child: %+v %v", childRuns, err)
	}
	callbackParent, err := one.TriggerJob(ctx, tenantID, parentJob.ID, "dependency-callback", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err = one.ClaimRuns(ctx, "dependency-core", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	callbackClaim, ok := claimForRun(claims, callbackParent.ID)
	if !ok {
		t.Fatal("callback parent not claimed")
	}
	callbackTokenHash := sha256.Sum256([]byte("dependency-callback-token"))
	if err = one.MarkWaitingCallback(ctx, callbackClaim.Run.ID, http.StatusAccepted, callbackTokenHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = two.CompleteCallback(ctx, callbackClaim.Run.ID, callbackTokenHash[:], true, "done"); err != nil {
		t.Fatal(err)
	}
	childRuns, err = one.ListRuns(ctx, tenantID, childJob.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	foundCallbackChild := false
	for _, run := range childRuns {
		if run.ParentRunID == callbackParent.ID && run.TriggerType == "dependency" {
			foundCallbackChild = true
		}
	}
	if !foundCallbackChild {
		t.Fatalf("callback success did not trigger child: %+v", childRuns)
	}
	serialJob := newPolicyJob("serial-policy", "serial")
	if _, err = one.TriggerJob(ctx, tenantID, serialJob.ID, "serial-1", ""); err != nil {
		t.Fatal(err)
	}
	claims, err = one.ClaimRuns(ctx, "policy-core", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !hasClaimForJob(claims, serialJob.ID) {
		t.Fatal("serial first run was not claimed")
	}
	serialSecond, err := two.TriggerJob(ctx, tenantID, serialJob.ID, "serial-2", "")
	if err != nil || serialSecond.Status != "pending" {
		t.Fatalf("serial second trigger = %+v, %v", serialSecond, err)
	}

	discardJob := newPolicyJob("discard-policy", "discard_later")
	if _, err = one.TriggerJob(ctx, tenantID, discardJob.ID, "discard-1", ""); err != nil {
		t.Fatal(err)
	}
	claims, err = one.ClaimRuns(ctx, "policy-core", 10, time.Minute)
	if err != nil || !hasClaimForJob(claims, discardJob.ID) {
		t.Fatalf("discard first claim missing: %v", err)
	}
	discardSecond, err := two.TriggerJob(ctx, tenantID, discardJob.ID, "discard-2", "")
	if err != nil || discardSecond.Status != "skipped" {
		t.Fatalf("discard second trigger = %+v, %v", discardSecond, err)
	}

	coverJob := newPolicyJob("cover-policy", "cover_early")
	coverFirst, err := one.TriggerJob(ctx, tenantID, coverJob.ID, "cover-1", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err = one.ClaimRuns(ctx, "policy-core", 10, time.Minute)
	if err != nil || !hasClaimForJob(claims, coverJob.ID) {
		t.Fatalf("cover first claim missing: %v", err)
	}
	coverSecond, err := two.TriggerJob(ctx, tenantID, coverJob.ID, "cover-2", "")
	if err != nil || coverSecond.Status != "pending" {
		t.Fatalf("cover second trigger = %+v, %v", coverSecond, err)
	}
	coverRuns, err := one.ListRuns(ctx, tenantID, coverJob.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if runStatus(coverRuns, coverFirst.ID) != "cancelled" || runStatus(coverRuns, coverSecond.ID) != "pending" {
		t.Fatalf("cover statuses = %+v", coverRuns)
	}
	cancelJob := newPolicyJob("cancel-policy", "serial")
	cancelRun, err := one.TriggerJob(ctx, tenantID, cancelJob.ID, "cancel-1", "")
	if err != nil {
		t.Fatal(err)
	}
	cancelledRun, err := two.CancelRun(ctx, tenantID, cancelRun.ID, "operator maintenance")
	if err != nil {
		t.Fatal(err)
	}
	if cancelledRun.Status != "cancelled" || cancelledRun.ErrorMessage != "operator maintenance" {
		t.Fatalf("cancelled run = %+v", cancelledRun)
	}
	if repeated, repeatErr := one.CancelRun(ctx, tenantID, cancelRun.ID, "different reason"); repeatErr != nil || repeated.Status != "cancelled" || repeated.ErrorMessage != "operator maintenance" {
		t.Fatalf("repeated cancellation = %+v, %v", repeated, repeatErr)
	}
	if claims, err = one.ClaimRuns(ctx, "cancel-core", 10, time.Minute); err != nil || hasClaimForJob(claims, cancelJob.ID) {
		t.Fatalf("cancelled pending run was claimed: %+v, %v", claims, err)
	}
	waitingRun, err := one.TriggerJob(ctx, tenantID, cancelJob.ID, "cancel-waiting", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err = one.ClaimRuns(ctx, "cancel-core", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if !hasClaimForJob(claims, cancelJob.ID) {
		t.Fatal("callback run was not claimed")
	}
	callbackHash := sha256.Sum256([]byte("cancelled-callback-token"))
	if err = one.MarkWaitingCallback(ctx, waitingRun.ID, http.StatusAccepted, callbackHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if _, err = two.CancelRun(ctx, tenantID, waitingRun.ID, "cancel callback wait"); err != nil {
		t.Fatal(err)
	}
	if err = one.CompleteCallback(ctx, waitingRun.ID, callbackHash[:], true, "late callback"); err != ErrNotFound {
		t.Fatalf("callback completed cancelled run: %v", err)
	}
	terminalRun, err := one.TriggerJob(ctx, tenantID, cancelJob.ID, "cancel-terminal", "")
	if err != nil {
		t.Fatal(err)
	}
	claims, err = one.ClaimRuns(ctx, "cancel-core", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	var terminalClaim ClaimedRun
	for _, claim := range claims {
		if claim.Run.ID == terminalRun.ID {
			terminalClaim = claim
			break
		}
	}
	if terminalClaim.Run.ID == "" {
		t.Fatal("terminal test run was not claimed")
	}
	if err = one.CompleteRun(ctx, terminalClaim.Run, true, http.StatusOK, "done", ""); err != nil {
		t.Fatal(err)
	}
	if _, err = two.CancelRun(ctx, tenantID, terminalRun.ID, "too late"); err != ErrNotCancellable {
		t.Fatalf("terminal cancellation = %v, want ErrNotCancellable", err)
	}
	job, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "job", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "skip", MisfirePolicy: "fire_once", Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	lifecycleJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "lifecycle", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "skip", MisfirePolicy: "fire_once", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	startedJob, err := one.SetJobEnabled(ctx, tenantID, lifecycleJob.ID, true, lifecycleJob.Version)
	if err != nil {
		t.Fatal(err)
	}
	if !startedJob.Enabled || startedJob.NextRunAt == nil || startedJob.Version != lifecycleJob.Version+1 {
		t.Fatalf("started job has invalid state: %+v", startedJob)
	}
	if _, err = two.SetJobEnabled(ctx, tenantID, lifecycleJob.ID, false, lifecycleJob.Version); err != ErrConflict {
		t.Fatalf("stale lifecycle update = %v, want ErrConflict", err)
	}
	stoppedJob, err := two.SetJobEnabled(ctx, tenantID, lifecycleJob.ID, false, startedJob.Version)
	if err != nil {
		t.Fatal(err)
	}
	if stoppedJob.Enabled || stoppedJob.NextRunAt != nil {
		t.Fatalf("stopped job remains schedulable: %+v", stoppedJob)
	}
	updatedJob := stoppedJob
	updatedJob.Name = "lifecycle-updated"
	updatedJob.Description = "updated through optimistic locking"
	updatedJob, err = one.UpdateJob(ctx, updatedJob)
	if err != nil {
		t.Fatal(err)
	}
	if updatedJob.Version != stoppedJob.Version+1 || updatedJob.Name != "lifecycle-updated" {
		t.Fatalf("updated job has invalid state: %+v", updatedJob)
	}
	if _, err = two.UpdateJob(ctx, stoppedJob); err != ErrConflict {
		t.Fatalf("stale job update = %v, want ErrConflict", err)
	}
	loadedUpdatedJob, err := two.GetJob(ctx, tenantID, updatedJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loadedUpdatedJob.Description != updatedJob.Description {
		t.Fatalf("loaded updated job = %+v", loadedUpdatedJob)
	}
	if err = two.DeleteJob(ctx, tenantID, updatedJob.ID, stoppedJob.Version); err != ErrConflict {
		t.Fatalf("stale job delete = %v, want ErrConflict", err)
	}
	if err = two.DeleteJob(ctx, tenantID, updatedJob.ID, updatedJob.Version); err != nil {
		t.Fatal(err)
	}
	if _, err = one.GetJob(ctx, tenantID, updatedJob.ID); err != ErrNotFound {
		t.Fatalf("get deleted job = %v, want ErrNotFound", err)
	}
	direct, err := pgx.Connect(ctx, dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer direct.Close(ctx)
	cronJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "cron-seconds", ScheduleType: "cron", ScheduleExpression: "0/1 * * * * ?", Timezone: "Asia/Shanghai", TargetURL: "https://example.com/cron", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, Enabled: true})
	if err != nil {
		t.Fatal(err)
	}
	if cronJob.NextRunAt == nil {
		t.Fatal("enabled cron job has no next_run_at")
	}
	if _, err = direct.Exec(ctx, `UPDATE jobs SET next_run_at=date_trunc('second',now()) WHERE id=$1`, cronJob.ID); err != nil {
		t.Fatal(err)
	}
	misfireJobs := make(map[string]Job, 3)
	for _, policy := range []string{"skip", "fire_once", "catch_up"} {
		misfireJob, createErr := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "misfire-" + policy, ScheduleType: "fixed_rate", ScheduleExpression: "1", Timezone: "UTC", TargetURL: "https://example.com/misfire", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: policy, MaxConcurrentRuns: 1, MaxCatchUp: 3, MaxQueueSize: 10, Enabled: true})
		if createErr != nil {
			t.Fatal(createErr)
		}
		misfireJobs[policy] = misfireJob
		if _, err = direct.Exec(ctx, `UPDATE jobs SET next_run_at=date_trunc('second',now())-interval '10 seconds' WHERE id=$1`, misfireJob.ID); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = direct.Exec(ctx, `UPDATE jobs SET next_run_at=now()-interval '1 second' WHERE id=$1`, job.ID); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for _, s := range []*Store{one, two} {
		s := s
		wg.Add(1)
		go func() { defer wg.Done(); errs <- s.EnqueueDue(ctx, 10) }()
	}
	wg.Wait()
	close(errs)
	for err = range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var count int
	if err = direct.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE job_id=$1`, job.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected one scheduled run, got %d", count)
	}
	var cronScheduledAt time.Time
	if err = direct.QueryRow(ctx, `SELECT count(*),min(scheduled_at) FROM job_runs WHERE job_id=$1`, cronJob.ID).Scan(&count, &cronScheduledAt); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("expected one cron run across stores, got %d", count)
	}
	cronState, err := two.GetJob(ctx, tenantID, cronJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cronState.NextRunAt == nil || !cronState.NextRunAt.After(cronScheduledAt) || cronState.Timezone != "Asia/Shanghai" {
		t.Fatalf("cron job did not advance in configured timezone: %+v", cronState)
	}
	for policy, want := range map[string]int{"skip": 0, "fire_once": 1, "catch_up": 3} {
		misfireJob := misfireJobs[policy]
		if err = direct.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE job_id=$1`, misfireJob.ID).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != want {
			t.Fatalf("misfire %s produced %d runs, want %d", policy, count, want)
		}
		state, stateErr := two.GetJob(ctx, tenantID, misfireJob.ID)
		if stateErr != nil {
			t.Fatal(stateErr)
		}
		if state.NextRunAt == nil || !state.NextRunAt.After(time.Now().Add(-time.Second)) {
			t.Fatalf("misfire %s did not advance next_run_at: %+v", policy, state)
		}
	}
	key := base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{4}, 32))
	ring, err := cryptox.NewKeyring(1, key)
	if err != nil {
		t.Fatal(err)
	}
	encryptedStore, err := New(ctx, dsn, WithHeaderCipher(ring))
	if err != nil {
		t.Fatal(err)
	}
	defer encryptedStore.Close()
	secretJob, err := encryptedStore.CreateJob(ctx, Job{TenantID: tenantID, Name: "secret-job", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{"Authorization": "Bearer top-secret"}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	var rawHeaders []byte
	if err = direct.QueryRow(ctx, `SELECT encrypted_headers FROM jobs WHERE id=$1`, secretJob.ID).Scan(&rawHeaders); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(rawHeaders, []byte("top-secret")) {
		t.Fatal("stored headers were not encrypted")
	}
	loaded, err := encryptedStore.GetJob(ctx, tenantID, secretJob.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Headers["Authorization"] != "Bearer top-secret" {
		t.Fatal("encrypted headers did not round-trip")
	}
	kubernetesCluster, err := encryptedStore.CreateKubernetesCluster(ctx, KubernetesCluster{TenantID: tenantID, Name: "integration-k8s", AuthMode: "service_account", APIServer: "https://k8s.example", Namespace: "jobs", Credentials: KubernetesCredentials{Token: "service-account-secret", CAData: "test-ca"}})
	if err != nil {
		t.Fatal(err)
	}
	var rawCredentials []byte
	if err = direct.QueryRow(ctx, `SELECT encrypted_credentials FROM kubernetes_clusters WHERE id=$1`, kubernetesCluster.ID).Scan(&rawCredentials); err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(rawCredentials, []byte("service-account-secret")) {
		t.Fatal("kubernetes credentials were stored in plaintext")
	}
	loadedCluster, err := encryptedStore.GetKubernetesCluster(ctx, tenantID, kubernetesCluster.ID)
	if err != nil || loadedCluster.Credentials.Token != "service-account-secret" {
		t.Fatalf("kubernetes cluster = %+v, %v", loadedCluster, err)
	}
	kubernetesJob, err := encryptedStore.CreateJob(ctx, Job{TenantID: tenantID, Name: "kubernetes-persistence", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 60, CallbackTimeoutSeconds: 60, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, ExecutorGroupID: routeGroup.ID, ExecutorHandler: "__kubernetes__", ScriptLanguage: "kubernetes", ScriptSource: `{"image":"alpine:3.22"}`, KubernetesClusterID: kubernetesCluster.ID})
	if err != nil {
		t.Fatal(err)
	}
	loadedKubernetesJob, err := encryptedStore.GetJob(ctx, tenantID, kubernetesJob.ID)
	if err != nil || loadedKubernetesJob.KubernetesClusterID != kubernetesCluster.ID {
		t.Fatalf("kubernetes job binding = %+v, %v", loadedKubernetesJob, err)
	}
	if err = encryptedStore.DeleteKubernetesCluster(ctx, tenantID, kubernetesCluster.ID, kubernetesCluster.Version); err != ErrKubernetesClusterInUse {
		t.Fatalf("delete referenced kubernetes cluster = %v", err)
	}
	if _, err = one.TriggerJob(ctx, tenantID, "00000000-0000-0000-0000-000000000099", "missing-job", ""); err != ErrNotFound {
		t.Fatalf("trigger missing job = %v, want ErrNotFound", err)
	}
	run, err := one.TriggerJob(ctx, tenantID, job.ID, "same-key", "")
	if err != nil {
		t.Fatal(err)
	}
	again, err := two.TriggerJob(ctx, tenantID, job.ID, "same-key", "")
	if err != nil {
		t.Fatal(err)
	}
	if run.ID != again.ID {
		t.Fatal("idempotency key created multiple runs")
	}
	type triggerResult struct {
		run Run
		err error
	}
	concurrent := make(chan triggerResult, 2)
	startTriggers := make(chan struct{})
	for index, schedulerStore := range []*Store{one, two} {
		index, schedulerStore := index, schedulerStore
		go func() {
			<-startTriggers
			triggered, triggerErr := schedulerStore.TriggerJob(ctx, tenantID, job.ID, "concurrent-key", fmt.Sprintf("payload-%d", index))
			concurrent <- triggerResult{run: triggered, err: triggerErr}
		}()
	}
	close(startTriggers)
	firstConcurrent := <-concurrent
	secondConcurrent := <-concurrent
	if firstConcurrent.err != nil || secondConcurrent.err != nil {
		t.Fatalf("concurrent triggers failed: %v, %v", firstConcurrent.err, secondConcurrent.err)
	}
	if firstConcurrent.run.ID != secondConcurrent.run.ID || firstConcurrent.run.RuntimeInput != secondConcurrent.run.RuntimeInput {
		t.Fatalf("concurrent idempotency mismatch: %+v, %+v", firstConcurrent.run, secondConcurrent.run)
	}
	if err = direct.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE job_id=$1 AND idempotency_key='concurrent-key'`, job.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("concurrent idempotency created %d runs", count)
	}
	claimed, err := one.ClaimRuns(ctx, "core-a", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) == 0 {
		t.Fatal("expected claimed runs")
	}
	target := claimed[0]
	token := "one-time-token"
	hash := sha256.Sum256([]byte(token))
	if err = one.PrepareExecutorDispatch(ctx, target.Run.ID, "executor-fast-path", "127.0.0.1:19090", hash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	prepared, err := two.GetRun(ctx, tenantID, target.Run.ID)
	if err != nil || prepared.Status != "waiting_callback" || prepared.ExecutorNodeID != "executor-fast-path" || prepared.ExecutorAddress != "127.0.0.1:19090" || prepared.ResponseStatus != 202 {
		t.Fatalf("prepared executor dispatch = %+v, %v", prepared, err)
	}
	if err = one.PrepareExecutorDispatch(ctx, target.Run.ID, "executor-fast-path", "127.0.0.1:19090", hash[:], time.Now().Add(time.Minute)); err != ErrConflict {
		t.Fatalf("repeated executor dispatch preparation = %v, want ErrConflict", err)
	}
	if err = two.CompleteCallback(ctx, target.Run.ID, hash[:], true, "done"); err != nil {
		t.Fatal(err)
	}
	if err = one.CompleteCallback(ctx, target.Run.ID, hash[:], true, "again"); err != ErrNotFound {
		t.Fatalf("reused callback token: %v", err)
	}
	callbackRetryJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "callback-retry", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com/callback", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, MaxRetries: 1, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	callbackAttempt, err := one.TriggerJob(ctx, tenantID, callbackRetryJob.ID, "callback-retry", "payload")
	if err != nil {
		t.Fatal(err)
	}
	callbackClaims, err := one.ClaimRuns(ctx, "callback-core", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	failureCallbackClaim, ok := claimForRun(callbackClaims, callbackAttempt.ID)
	if !ok {
		t.Fatal("callback retry attempt was not claimed")
	}
	failureHash := sha256.Sum256([]byte("callback-failure-token"))
	if err = one.MarkWaitingCallback(ctx, failureCallbackClaim.Run.ID, http.StatusAccepted, failureHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = two.CompleteCallback(ctx, failureCallbackClaim.Run.ID, failureHash[:], false, "async failure"); err != nil {
		t.Fatal(err)
	}
	failedCallback, err := one.GetRun(ctx, tenantID, failureCallbackClaim.Run.ID)
	if err != nil || failedCallback.Status != "failed" {
		t.Fatalf("failed callback = %+v, %v", failedCallback, err)
	}
	callbackRuns, err := one.ListRuns(ctx, tenantID, callbackRetryJob.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	var callbackRetry *Run
	for index := range callbackRuns {
		if callbackRuns[index].RetryOfRunID == failureCallbackClaim.Run.ID {
			callbackRetry = &callbackRuns[index]
		}
	}
	if callbackRetry == nil || callbackRetry.Attempt != 2 || callbackRetry.RuntimeInput != "payload" {
		t.Fatalf("callback retry = %+v; runs=%+v", callbackRetry, callbackRuns)
	}
	if err = direct.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload->>'run_id'=$1`, failureCallbackClaim.Run.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("intermediate callback failure emitted %d alerts", count)
	}
	if _, err = direct.Exec(ctx, `UPDATE job_runs SET available_at=now() WHERE id=$1`, callbackRetry.ID); err != nil {
		t.Fatal(err)
	}
	finalCallbackClaims, err := two.ClaimRuns(ctx, "callback-final-core", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	finalCallbackClaim, ok := claimForRun(finalCallbackClaims, callbackRetry.ID)
	if !ok {
		t.Fatal("final callback attempt was not claimed")
	}
	finalFailureHash := sha256.Sum256([]byte("callback-final-failure-token"))
	if err = two.MarkWaitingCallback(ctx, finalCallbackClaim.Run.ID, http.StatusAccepted, finalFailureHash[:], time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	if err = one.CompleteCallback(ctx, finalCallbackClaim.Run.ID, finalFailureHash[:], false, "final async failure"); err != nil {
		t.Fatal(err)
	}
	callbackRuns, err = one.ListRuns(ctx, tenantID, callbackRetryJob.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(callbackRuns) != 2 || callbackRuns[0].Status != "failed" {
		t.Fatalf("final callback runs = %+v", callbackRuns)
	}
	if err = direct.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload->>'run_id'=$1`, finalCallbackClaim.Run.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("final callback failure emitted %d alerts", count)
	}
	callbackTimeoutJob, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "callback-timeout-retry", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com/callback-timeout", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, MaxRetries: 1, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxCatchUp: 10, MaxQueueSize: 10, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	timeoutAttempt, err := one.TriggerJob(ctx, tenantID, callbackTimeoutJob.ID, "callback-timeout", "timeout-payload")
	if err != nil {
		t.Fatal(err)
	}
	timeoutClaims, err := one.ClaimRuns(ctx, "callback-timeout-core", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	timeoutClaim, ok := claimForRun(timeoutClaims, timeoutAttempt.ID)
	if !ok {
		t.Fatal("callback timeout attempt was not claimed")
	}
	timeoutHash := sha256.Sum256([]byte("callback-timeout-token"))
	if err = one.MarkWaitingCallback(ctx, timeoutClaim.Run.ID, http.StatusAccepted, timeoutHash[:], time.Now().Add(-time.Second)); err != nil {
		t.Fatal(err)
	}
	if err = two.ExpireCallbacks(ctx); err != nil {
		t.Fatal(err)
	}
	timedOutCallback, err := one.GetRun(ctx, tenantID, timeoutClaim.Run.ID)
	if err != nil || timedOutCallback.Status != "timed_out" {
		t.Fatalf("timed out callback = %+v, %v", timedOutCallback, err)
	}
	timeoutRuns, err := one.ListRuns(ctx, tenantID, callbackTimeoutJob.ID, 10)
	if err != nil {
		t.Fatal(err)
	}
	var timeoutRetry *Run
	for index := range timeoutRuns {
		if timeoutRuns[index].RetryOfRunID == timeoutClaim.Run.ID {
			timeoutRetry = &timeoutRuns[index]
		}
	}
	if timeoutRetry == nil || timeoutRetry.Attempt != 2 || timeoutRetry.RuntimeInput != "timeout-payload" {
		t.Fatalf("callback timeout retry = %+v; runs=%+v", timeoutRetry, timeoutRuns)
	}
	if err = direct.QueryRow(ctx, `SELECT count(*) FROM outbox_events WHERE payload->>'run_id'=$1`, timeoutClaim.Run.ID).Scan(&count); err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("intermediate callback timeout emitted %d alerts", count)
	}
	limited, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "limited", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "queue", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if _, err = one.TriggerJob(ctx, tenantID, limited.ID, fmt.Sprintf("limit-%d", i), ""); err != nil {
			t.Fatal(err)
		}
	}
	firstClaims, err := one.ClaimRuns(ctx, "core-limit", 10, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	limitedCount := 0
	for _, claim := range firstClaims {
		if claim.Job.ID == limited.ID {
			limitedCount++
		}
	}
	if limitedCount != 1 {
		t.Fatalf("job concurrency limit claimed %d runs", limitedCount)
	}
	queueLimited, err := one.CreateJob(ctx, Job{TenantID: tenantID, Name: "queue-limited", ScheduleType: "fixed_interval", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 600, OverlapPolicy: "queue", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, MaxQueueSize: 1, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = one.TriggerJob(ctx, tenantID, queueLimited.ID, "queue-1", ""); err != nil {
		t.Fatal(err)
	}
	if _, err = two.TriggerJob(ctx, tenantID, queueLimited.ID, "queue-2", ""); err != ErrQueueFull {
		t.Fatalf("queue limit was not enforced: %v", err)
	}
	leaseClaims, err := one.ClaimRuns(ctx, "core-lease", 20, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	for _, claim := range leaseClaims {
		if claim.Job.ID == queueLimited.ID {
			var leaseSeconds float64
			if err = direct.QueryRow(ctx, `SELECT extract(epoch FROM lease_until-now()) FROM job_runs WHERE id=$1`, claim.Run.ID).Scan(&leaseSeconds); err != nil {
				t.Fatal(err)
			}
			if leaseSeconds < 620 {
				t.Fatalf("lease %.0fs does not cover task timeout", leaseSeconds)
			}
		}
	}
	apiKey, rawToken, err := one.CreateAPIKey(ctx, tenantID, "integration-key", "developer")
	if err != nil {
		t.Fatal(err)
	}
	authenticatedTenant, role, err := two.AuthenticateAPIKey(ctx, rawToken)
	if err != nil {
		t.Fatal(err)
	}
	if authenticatedTenant != tenantID || role != "developer" {
		t.Fatal("API key authentication returned wrong principal")
	}
	if err = one.RevokeAPIKey(ctx, tenantID, apiKey.ID); err != nil {
		t.Fatal(err)
	}
	if _, _, err = two.AuthenticateAPIKey(ctx, rawToken); err != ErrNotFound {
		t.Fatalf("revoked API key authenticated: %v", err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE outbox_events SET published_at=now() WHERE published_at IS NULL`); err != nil {
		t.Fatal(err)
	}
	if _, err = one.CreateNotificationChannel(ctx, NotificationChannel{TenantID: tenantID, Kind: "webhook", Name: "primary-alerts", Config: json.RawMessage(`{"url":"https://alerts.example.com/primary"}`), Events: []string{"job.run.exhausted"}, AllJobs: true, MaxAttempts: 8, BackoffInitialSeconds: 2, BackoffMaxSeconds: 300}); err != nil {
		t.Fatal(err)
	}
	if _, err = one.CreateNotificationChannel(ctx, NotificationChannel{TenantID: tenantID, Kind: "webhook", Name: "secondary-alerts", Config: json.RawMessage(`{"url":"https://alerts.example.com/secondary"}`), Events: []string{"job.run.exhausted"}, AllJobs: true, MaxAttempts: 8, BackoffInitialSeconds: 2, BackoffMaxSeconds: 300}); err != nil {
		t.Fatal(err)
	}
	var notificationEventID string
	if err = one.pool.QueryRow(ctx, `INSERT INTO outbox_events(tenant_id,topic,payload) VALUES($1,'job.run.exhausted','{"run_id":"notification-test"}') RETURNING id`, tenantID).Scan(&notificationEventID); err != nil {
		t.Fatal(err)
	}
	if err = one.PrepareNotificationDeliveries(ctx, 10); err != nil {
		t.Fatal(err)
	}
	firstDeliveries, err := one.ClaimNotificationDeliveries(ctx, "notifier-a", 1)
	if err != nil || len(firstDeliveries) != 1 {
		t.Fatalf("first deliveries = %+v, %v", firstDeliveries, err)
	}
	secondDeliveries, err := two.ClaimNotificationDeliveries(ctx, "notifier-b", 10)
	if err != nil || len(secondDeliveries) != 1 || secondDeliveries[0].ID == firstDeliveries[0].ID {
		t.Fatalf("second deliveries = %+v, %v", secondDeliveries, err)
	}
	if err = one.CompleteNotificationDelivery(ctx, firstDeliveries[0].ID, notificationEventID); err != nil {
		t.Fatal(err)
	}
	if err = two.RetryNotificationDelivery(ctx, secondDeliveries[0].ID, "temporary failure", 0); err != nil {
		t.Fatal(err)
	}
	var published bool
	if err = one.pool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox_events WHERE id=$1`, notificationEventID).Scan(&published); err != nil || published {
		t.Fatalf("event published before all channels: %v, %v", published, err)
	}
	retryDeliveries, err := one.ClaimNotificationDeliveries(ctx, "notifier-a", 10)
	if err != nil || len(retryDeliveries) != 1 || retryDeliveries[0].ID != secondDeliveries[0].ID || retryDeliveries[0].Attempts != 2 {
		t.Fatalf("retry deliveries = %+v, %v", retryDeliveries, err)
	}
	if err = one.CompleteNotificationDelivery(ctx, retryDeliveries[0].ID, notificationEventID); err != nil {
		t.Fatal(err)
	}
	if err = one.pool.QueryRow(ctx, `SELECT published_at IS NOT NULL FROM outbox_events WHERE id=$1`, notificationEventID).Scan(&published); err != nil || !published {
		t.Fatalf("event not published after all channels: %v, %v", published, err)
	}
	remainingDeliveries, err := two.ClaimNotificationDeliveries(ctx, "notifier-b", 10)
	if err != nil || len(remainingDeliveries) != 0 {
		t.Fatalf("completed delivery reclaimed = %+v, %v", remainingDeliveries, err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE notification_channels SET enabled=false WHERE tenant_id=$1`, tenantID); err != nil {
		t.Fatal(err)
	}
	scoped, err := one.CreateNotificationChannel(ctx, NotificationChannel{TenantID: tenantID, Kind: "dingtalk", Name: "queue-job-only", Config: json.RawMessage(`{"url":"https://oapi.dingtalk.com/robot/send","auth_type":"none"}`), Events: []string{"job.run.succeeded"}, JobIDs: []string{queueLimited.ID}, MaxAttempts: 1, BackoffInitialSeconds: 2, BackoffMaxSeconds: 2})
	if err != nil {
		t.Fatal(err)
	}
	var matchingEventID, ignoredEventID string
	if err = one.pool.QueryRow(ctx, `INSERT INTO outbox_events(tenant_id,topic,payload) VALUES($1,'job.run.succeeded',jsonb_build_object('run_id','matching','job_id',$2::text)) RETURNING id`, tenantID, queueLimited.ID).Scan(&matchingEventID); err != nil {
		t.Fatal(err)
	}
	if err = one.pool.QueryRow(ctx, `INSERT INTO outbox_events(tenant_id,topic,payload) VALUES($1,'job.run.succeeded',jsonb_build_object('run_id','ignored','job_id',$2::text)) RETURNING id`, tenantID, firstShard.JobID).Scan(&ignoredEventID); err != nil {
		t.Fatal(err)
	}
	if err = one.PrepareNotificationDeliveries(ctx, 10); err != nil {
		t.Fatal(err)
	}
	var matchingDeliveries, ignoredDeliveries int
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FILTER(WHERE event_id=$1),count(*) FILTER(WHERE event_id=$2) FROM notification_deliveries WHERE channel_id=$3`, matchingEventID, ignoredEventID, scoped.ID).Scan(&matchingDeliveries, &ignoredDeliveries); err != nil {
		t.Fatal(err)
	}
	if matchingDeliveries != 1 || ignoredDeliveries != 0 {
		t.Fatalf("scoped deliveries matching=%d ignored=%d", matchingDeliveries, ignoredDeliveries)
	}
	deadDeliveries, err := one.ClaimNotificationDeliveries(ctx, "notifier-dead", 10)
	if err != nil || len(deadDeliveries) != 1 {
		t.Fatalf("dead-letter claim = %+v, %v", deadDeliveries, err)
	}
	if err = one.DeadLetterNotificationDelivery(ctx, deadDeliveries[0].ID, matchingEventID, "attempts exhausted"); err != nil {
		t.Fatal(err)
	}
	history, err := one.NotificationHistory(ctx, tenantID, scoped.ID, queueLimited.ID, "dead", 10)
	if err != nil || len(history) != 1 || history[0].RunID != "matching" || history[0].Attempts != 1 || history[0].LastError != "attempts exhausted" || history[0].DeadAt == nil {
		t.Fatalf("notification history = %+v, %v", history, err)
	}
	var reportTenantID string
	if err = one.pool.QueryRow(ctx, `INSERT INTO tenants(name) VALUES('report-isolation') RETURNING id`).Scan(&reportTenantID); err != nil {
		t.Fatal(err)
	}
	reportJob, err := one.CreateJob(ctx, Job{TenantID: reportTenantID, Name: "report", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range []struct{ status, at string }{{"succeeded", "2026-08-01 15:30:00+00"}, {"failed", "2026-08-01 16:30:00+00"}, {"timed_out", "2026-08-01 17:30:00+00"}, {"running", "2026-08-03 02:00:00+00"}} {
		if _, err = one.pool.Exec(ctx, `INSERT INTO job_runs(tenant_id,job_id,trigger_type,status,scheduled_at) VALUES($1,$2,'manual',$3,$4)`, reportTenantID, reportJob.ID, row.status, row.at); err != nil {
			t.Fatal(err)
		}
	}
	from, _ := time.Parse(time.DateOnly, "2026-08-01")
	to, _ := time.Parse(time.DateOnly, "2026-08-03")
	report, err := two.RunReport(ctx, reportTenantID, from, to, "Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	if len(report) != 3 || report[0].Total != 1 || report[0].Succeeded != 1 || report[1].Total != 2 || report[1].Failed != 2 || report[2].Active != 1 {
		t.Fatalf("timezone report = %+v", report)
	}
	purgeOtherJob, err := one.CreateJob(ctx, Job{TenantID: reportTenantID, Name: "purge-other", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	purgeFilterJob, err := one.CreateJob(ctx, Job{TenantID: reportTenantID, Name: "purge-filter", ScheduleType: "fixed_rate", ScheduleExpression: "60", Timezone: "UTC", TargetURL: "https://example.com", HTTPMethod: "POST", Headers: map[string]string{}, TimeoutSeconds: 10, OverlapPolicy: "parallel", MisfirePolicy: "fire_once", MaxConcurrentRuns: 1, Enabled: false})
	if err != nil {
		t.Fatal(err)
	}
	var oldestID, newerID, activeID, otherJobRunID string
	for _, row := range []struct {
		id            *string
		jobID, status string
		age           time.Duration
	}{{&oldestID, purgeOtherJob.ID, "succeeded", 3 * time.Hour}, {&newerID, purgeOtherJob.ID, "failed", 2 * time.Hour}, {&activeID, purgeOtherJob.ID, "pending", time.Hour}, {&otherJobRunID, purgeFilterJob.ID, "succeeded", 3 * time.Hour}} {
		if err = one.pool.QueryRow(ctx, `INSERT INTO job_runs(tenant_id,job_id,trigger_type,status,scheduled_at) VALUES($1,$2,'manual',$3,$4) RETURNING id`, reportTenantID, row.jobID, row.status, time.Now().Add(-row.age)).Scan(row.id); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = one.pool.Exec(ctx, `INSERT INTO job_run_logs(tenant_id,run_id,entry_id,stream,content) VALUES($1,$2,'purge-log','stdout','old')`, reportTenantID, oldestID); err != nil {
		t.Fatal(err)
	}
	deleted, err := one.PurgeRunHistory(ctx, reportTenantID, purgeOtherJob.ID, time.Now(), 1)
	if err != nil || deleted != 1 {
		t.Fatalf("first purge deleted=%d err=%v", deleted, err)
	}
	count = 0
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM job_run_logs WHERE run_id=$1`, oldestID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("old run logs remain: %d %v", count, err)
	}
	deleted, err = two.PurgeRunHistory(ctx, reportTenantID, purgeOtherJob.ID, time.Now(), 10)
	if err != nil || deleted != 1 {
		t.Fatalf("second purge deleted=%d err=%v", deleted, err)
	}
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE id=ANY($1::uuid[])`, []string{activeID, otherJobRunID}).Scan(&count); err != nil || count != 2 {
		t.Fatalf("active or other-job run removed: %d %v", count, err)
	}
	if _, err = one.pool.Exec(ctx, `UPDATE job_runs SET scheduled_at=now()-interval '2 hours' WHERE id=$1`, otherJobRunID); err != nil {
		t.Fatal(err)
	}
	if err = one.CleanupRunHistory(ctx, time.Hour); err != nil {
		t.Fatal(err)
	}
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE id=$1`, otherJobRunID).Scan(&count); err != nil || count != 0 {
		t.Fatalf("automatic retention kept terminal run: %d %v", count, err)
	}
	if err = one.pool.QueryRow(ctx, `SELECT count(*) FROM job_runs WHERE id=$1`, activeID).Scan(&count); err != nil || count != 1 {
		t.Fatalf("automatic retention removed active run: %d %v", count, err)
	}
	var userID string
	if err = direct.QueryRow(ctx, `INSERT INTO users(email,password_hash) VALUES('session@example.com','test') RETURNING id`).Scan(&userID); err != nil {
		t.Fatal(err)
	}
	refreshToken, firstSession, err := one.CreateRefreshSession(ctx, userID, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	nextToken, nextSession, err := one.RotateRefreshSession(ctx, refreshToken, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if nextToken == refreshToken || nextSession.FamilyID != firstSession.FamilyID {
		t.Fatal("refresh token was not rotated in the same family")
	}
	if _, _, err = one.RotateRefreshSession(ctx, refreshToken, time.Hour); err != ErrConflict {
		t.Fatalf("expected replay conflict, got %v", err)
	}
	if _, _, err = one.RotateRefreshSession(ctx, nextToken, time.Hour); err != ErrConflict {
		t.Fatalf("replay did not revoke session family: %v", err)
	}
}

func hasClaimForJob(claims []ClaimedRun, jobID string) bool {
	for _, claim := range claims {
		if claim.Job.ID == jobID {
			return true
		}
	}
	return false
}

func runStatus(runs []Run, runID string) string {
	for _, run := range runs {
		if run.ID == runID {
			return run.Status
		}
	}
	return ""
}

func claimForRun(claims []ClaimedRun, runID string) (ClaimedRun, bool) {
	for _, claim := range claims {
		if claim.Run.ID == runID {
			return claim, true
		}
	}
	return ClaimedRun{}, false
}
