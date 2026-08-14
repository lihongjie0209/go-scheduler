package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/spf13/cobra"
)

type cliConfig struct {
	baseURL, token, email, password, tenant string
	passwordStdin                           bool
	client                                  *http.Client
}

type apiError struct {
	Status  int
	Message string
}

func (e *apiError) Error() string {
	return fmt.Sprintf("api returned HTTP %d: %s", e.Status, e.Message)
}

func newRootCommand(buildVersion string) *cobra.Command {
	c := &cliConfig{baseURL: envOr("SCHEDULER_URL", "http://127.0.0.1:8080"), token: os.Getenv("SCHEDULER_TOKEN"), email: os.Getenv("SCHEDULER_EMAIL"), password: os.Getenv("SCHEDULER_PASSWORD"), tenant: os.Getenv("SCHEDULER_TENANT"), client: &http.Client{Timeout: 20 * time.Second}}
	root := &cobra.Command{Use: "schedulerctl", Short: "Go Scheduler API command line client", SilenceUsage: true, SilenceErrors: true}
	f := root.PersistentFlags()
	f.StringVar(&c.baseURL, "server", c.baseURL, "API Server URL (SCHEDULER_URL)")
	f.StringVar(&c.token, "token", c.token, "JWT or gsk_ API key (SCHEDULER_TOKEN)")
	f.StringVar(&c.email, "email", c.email, "login email (SCHEDULER_EMAIL)")
	f.StringVar(&c.password, "password", c.password, "login password (SCHEDULER_PASSWORD)")
	f.BoolVar(&c.passwordStdin, "password-stdin", false, "read login password from stdin")
	f.StringVar(&c.tenant, "tenant", c.tenant, "tenant UUID (SCHEDULER_TENANT)")
	root.AddCommand(loginCommand(c), healthCommand(c), dashboardCommand(c), reportsCommand(c), jobsCommand(c), runsCommand(c), executorsCommand(c), kubernetesClustersCommand(c), notificationsCommand(c), versionCommand(buildVersion), completionCommand(root))
	return root
}

func envOr(key, fallback string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return fallback
}
func (c *cliConfig) endpoint(path string) string { return strings.TrimRight(c.baseURL, "/") + path }
func (c *cliConfig) login(ctx context.Context, stdin io.Reader) (string, error) {
	password := c.password
	if c.passwordStdin {
		raw, err := io.ReadAll(io.LimitReader(stdin, 4096))
		if err != nil {
			return "", err
		}
		password = strings.TrimSpace(string(raw))
	}
	if c.email == "" || password == "" {
		return "", errors.New("provide --token or both --email and --password")
	}
	payload, err := json.Marshal(map[string]string{"email": c.email, "password": password})
	if err != nil {
		return "", fmt.Errorf("encode login request: %w", err)
	}
	body, err := c.do(ctx, http.MethodPost, "/api/v1/auth/login", payload, false)
	if err != nil {
		return "", err
	}
	var response struct {
		AccessToken string `json:"access_token"`
	}
	if err = json.Unmarshal(body, &response); err != nil {
		return "", fmt.Errorf("decode login response: %w", err)
	}
	if response.AccessToken == "" {
		return "", errors.New("login response did not contain an access token")
	}
	return response.AccessToken, nil
}
func (c *cliConfig) authenticate(ctx context.Context, stdin io.Reader) error {
	if c.token == "" {
		token, err := c.login(ctx, stdin)
		if err != nil {
			return err
		}
		c.token = token
	}
	if c.tenant == "" && !strings.HasPrefix(c.token, "gsk_") {
		body, err := c.do(ctx, http.MethodGet, "/api/v1/auth/me", nil, true)
		if err != nil {
			return err
		}
		var me struct {
			Tenants []struct {
				ID string `json:"ID"`
			} `json:"tenants"`
		}
		if err = json.Unmarshal(body, &me); err != nil {
			return fmt.Errorf("decode current user: %w", err)
		}
		if len(me.Tenants) == 0 {
			return errors.New("account has no accessible tenant; provide --tenant after membership is assigned")
		}
		c.tenant = me.Tenants[0].ID
	}
	return nil
}
func (c *cliConfig) do(ctx context.Context, method, path string, payload []byte, authenticated bool) ([]byte, error) {
	var reader io.Reader
	if payload != nil {
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path), reader)
	if err != nil {
		return nil, err
	}
	if payload != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if authenticated {
		req.Header.Set("Authorization", "Bearer "+c.token)
		if c.tenant != "" {
			req.Header.Set("X-Tenant-ID", c.tenant)
		}
	}
	response, err := c.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request API: %w", err)
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4<<20))
	if err != nil {
		return nil, fmt.Errorf("read API response: %w", err)
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		var detail struct {
			Error string `json:"error"`
		}
		_ = json.Unmarshal(body, &detail)
		if detail.Error == "" {
			detail.Error = strings.TrimSpace(string(body))
		}
		return nil, &apiError{Status: response.StatusCode, Message: detail.Error}
	}
	return body, nil
}
func writeJSON(w io.Writer, raw []byte) error {
	if len(raw) == 0 {
		return nil
	}
	var value any
	if err := json.Unmarshal(raw, &value); err != nil {
		return err
	}
	encoder := json.NewEncoder(w)
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}
func runAuthenticated(c *cliConfig, method, path string, payload func() ([]byte, error)) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		if err := c.authenticate(cmd.Context(), cmd.InOrStdin()); err != nil {
			return err
		}
		var raw []byte
		var err error
		if payload != nil {
			raw, err = payload()
			if err != nil {
				return err
			}
		}
		body, err := c.do(cmd.Context(), method, path, raw, true)
		if err != nil {
			return err
		}
		return writeJSON(cmd.OutOrStdout(), body)
	}
}
func loginCommand(c *cliConfig) *cobra.Command {
	return &cobra.Command{Use: "login", Short: "Log in and print an access token", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		token, err := c.login(cmd.Context(), cmd.InOrStdin())
		if err != nil {
			return err
		}
		_, err = fmt.Fprintln(cmd.OutOrStdout(), token)
		return err
	}}
}
func healthCommand(c *cliConfig) *cobra.Command {
	return &cobra.Command{Use: "health", Short: "Check API Server readiness", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		body, err := c.do(cmd.Context(), http.MethodGet, "/health/ready", nil, false)
		if err != nil {
			return err
		}
		return writeJSON(cmd.OutOrStdout(), body)
	}}
}
func dashboardCommand(c *cliConfig) *cobra.Command {
	return &cobra.Command{Use: "dashboard", Short: "Show tenant scheduling summary", Args: cobra.NoArgs, RunE: runAuthenticated(c, http.MethodGet, "/api/v1/dashboard", nil)}
}
func reportsCommand(c *cliConfig) *cobra.Command {
	reports := &cobra.Command{Use: "reports", Short: "Inspect scheduling reports"}
	var from, to, timezone string
	runs := &cobra.Command{Use: "runs", Short: "Show daily run outcome trend", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		query := url.Values{}
		if from != "" {
			query.Set("from", from)
		}
		if to != "" {
			query.Set("to", to)
		}
		if timezone != "" {
			query.Set("timezone", timezone)
		}
		path := "/api/v1/reports/runs"
		if encoded := query.Encode(); encoded != "" {
			path += "?" + encoded
		}
		return runAuthenticated(c, http.MethodGet, path, nil)(cmd, nil)
	}}
	runs.Flags().StringVar(&from, "from", "", "inclusive start date (YYYY-MM-DD)")
	runs.Flags().StringVar(&to, "to", "", "inclusive end date (YYYY-MM-DD)")
	runs.Flags().StringVar(&timezone, "timezone", "", "IANA timezone (default UTC)")
	reports.AddCommand(runs)
	return reports
}
func jobsCommand(c *cliConfig) *cobra.Command {
	jobs := &cobra.Command{Use: "jobs", Short: "Manage scheduled jobs"}
	jobs.AddCommand(&cobra.Command{Use: "list", Short: "List jobs", Args: cobra.NoArgs, RunE: runAuthenticated(c, http.MethodGet, "/api/v1/jobs", nil)}, &cobra.Command{Use: "get ID", Short: "Get a job", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodGet, "/api/v1/jobs/"+args[0], nil)(cmd, nil)
	}}, createJobCommand(c), updateJobCommand(c), lifecycleJobCommand(c, true), lifecycleJobCommand(c, false), deleteJobCommand(c), triggerJobCommand(c), previewScheduleCommand(c))
	jobs.AddCommand(jobDependenciesCommand(c))
	jobs.AddCommand(jobScriptVersionsCommand(c))
	return jobs
}
func jobScriptVersionsCommand(c *cliConfig) *cobra.Command {
	versions := &cobra.Command{Use: "script-versions", Short: "Inspect and roll back immutable script versions"}
	versions.AddCommand(&cobra.Command{Use: "list JOB_ID", Short: "List script versions newest first", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodGet, "/api/v1/jobs/"+args[0]+"/script-versions", nil)(cmd, nil)
	}})
	var jobVersion int64
	var remark string
	rollback := &cobra.Command{Use: "rollback JOB_ID VERSION_ID", Short: "Atomically restore a script version", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		payload := func() ([]byte, error) { return json.Marshal(map[string]any{"version": jobVersion, "remark": remark}) }
		return runAuthenticated(c, http.MethodPost, "/api/v1/jobs/"+args[0]+"/script-versions/"+args[1]+"/rollback", payload)(cmd, nil)
	}}
	rollback.Flags().Int64Var(&jobVersion, "version", 0, "expected current job version")
	rollback.Flags().StringVar(&remark, "remark", "", "rollback audit remark (max 200 characters)")
	_ = rollback.MarkFlagRequired("version")
	versions.AddCommand(rollback)
	return versions
}
func previewScheduleCommand(c *cliConfig) *cobra.Command {
	var scheduleType, expression, timezone, after string
	var count int32
	cmd := &cobra.Command{Use: "preview", Short: "Preview future trigger times without saving a job", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if after != "" {
			if _, err := time.Parse(time.RFC3339, after); err != nil {
				return fmt.Errorf("after must be RFC3339: %w", err)
			}
		}
		payload := func() ([]byte, error) {
			return json.Marshal(map[string]any{"schedule_type": scheduleType, "schedule_expression": expression, "timezone": timezone, "after": after, "count": count})
		}
		return runAuthenticated(c, http.MethodPost, "/api/v1/schedules/preview", payload)(cmd, nil)
	}}
	cmd.Flags().StringVar(&scheduleType, "type", "", "schedule type: cron, once, fixed_rate, or fixed_delay")
	cmd.Flags().StringVar(&expression, "expression", "", "schedule expression")
	cmd.Flags().StringVar(&timezone, "timezone", "UTC", "IANA timezone")
	cmd.Flags().StringVar(&after, "after", "", "preview strictly after this RFC3339 timestamp")
	cmd.Flags().Int32Var(&count, "count", 5, "number of trigger times (1-100)")
	_ = cmd.MarkFlagRequired("type")
	_ = cmd.MarkFlagRequired("expression")
	return cmd
}
func jobDependenciesCommand(c *cliConfig) *cobra.Command {
	dependencies := &cobra.Command{Use: "dependencies", Short: "Manage child jobs triggered after success"}
	dependencies.AddCommand(&cobra.Command{Use: "get PARENT_ID", Short: "List child job dependencies", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodGet, "/api/v1/jobs/"+args[0]+"/dependencies", nil)(cmd, nil)
	}})
	var children []string
	set := &cobra.Command{Use: "set PARENT_ID", Short: "Replace child job dependencies", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		payload := func() ([]byte, error) { return json.Marshal(map[string][]string{"child_job_ids": children}) }
		return runAuthenticated(c, http.MethodPut, "/api/v1/jobs/"+args[0]+"/dependencies", payload)(cmd, nil)
	}}
	set.Flags().StringSliceVar(&children, "child", nil, "child job UUID (repeat flag or use comma-separated values)")
	dependencies.AddCommand(set)
	return dependencies
}
func lifecycleJobCommand(c *cliConfig, enabled bool) *cobra.Command {
	name := "stop"
	if enabled {
		name = "start"
	}
	var version int64
	cmd := &cobra.Command{Use: name + " ID", Short: name + " a scheduled job", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		payload := func() ([]byte, error) { return json.Marshal(map[string]int64{"version": version}) }
		return runAuthenticated(c, http.MethodPost, "/api/v1/jobs/"+args[0]+"/"+name, payload)(cmd, nil)
	}}
	cmd.Flags().Int64Var(&version, "version", 0, "optimistic-lock version")
	_ = cmd.MarkFlagRequired("version")
	return cmd
}
func createJobCommand(c *cliConfig) *cobra.Command {
	var file string
	cmd := &cobra.Command{Use: "create", Short: "Create a job from a JSON file or stdin", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		read := func() ([]byte, error) {
			if file == "-" {
				return io.ReadAll(io.LimitReader(cmd.InOrStdin(), 1<<20))
			}
			// #nosec G304 -- the operator explicitly supplies the job definition path.
			return os.ReadFile(file)
		}
		return runAuthenticated(c, http.MethodPost, "/api/v1/jobs", read)(cmd, nil)
	}}
	cmd.Flags().StringVarP(&file, "file", "f", "-", "job JSON file, or - for stdin")
	return cmd
}

func updateJobCommand(c *cliConfig) *cobra.Command {
	var file string
	cmd := &cobra.Command{Use: "update ID", Short: "Update a job from a JSON file or stdin", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		read := func() ([]byte, error) {
			if file == "-" {
				return io.ReadAll(io.LimitReader(cmd.InOrStdin(), 1<<20))
			}
			// #nosec G304 -- the operator explicitly supplies the job definition path.
			return os.ReadFile(file)
		}
		return runAuthenticated(c, http.MethodPut, "/api/v1/jobs/"+args[0], read)(cmd, nil)
	}}
	cmd.Flags().StringVarP(&file, "file", "f", "-", "job JSON file, or - for stdin")
	return cmd
}

func deleteJobCommand(c *cliConfig) *cobra.Command {
	var version int64
	cmd := &cobra.Command{Use: "delete ID", Short: "Delete a job", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		payload := func() ([]byte, error) { return json.Marshal(map[string]int64{"version": version}) }
		return runAuthenticated(c, http.MethodDelete, "/api/v1/jobs/"+args[0], payload)(cmd, nil)
	}}
	cmd.Flags().Int64Var(&version, "version", 0, "optimistic-lock version")
	_ = cmd.MarkFlagRequired("version")
	return cmd
}
func triggerJobCommand(c *cliConfig) *cobra.Command {
	var input, key string
	var addresses []string
	cmd := &cobra.Command{Use: "trigger ID", Short: "Trigger a job manually", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		payload := func() ([]byte, error) {
			return json.Marshal(map[string]any{"input": input, "idempotency_key": key, "override_addresses": addresses})
		}
		return runAuthenticated(c, http.MethodPost, "/api/v1/jobs/"+args[0]+"/trigger", payload)(cmd, nil)
	}}
	cmd.Flags().StringVar(&input, "input", "", "runtime input")
	cmd.Flags().StringVar(&key, "idempotency-key", "", "idempotency key")
	cmd.Flags().StringArrayVar(&addresses, "address", nil, "temporary HTTP(S) or gRPC executor URL; repeat to override the group for this run")
	return cmd
}
func runsCommand(c *cliConfig) *cobra.Command {
	var job, broadcastGroup string
	cmd := &cobra.Command{Use: "runs", Short: "List job runs", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		path := "/api/v1/runs"
		query := url.Values{}
		if job != "" {
			query.Set("job_id", job)
		}
		if broadcastGroup != "" {
			query.Set("broadcast_group_id", broadcastGroup)
		}
		if len(query) > 0 {
			path += "?" + query.Encode()
		}
		return runAuthenticated(c, http.MethodGet, path, nil)(cmd, nil)
	}}
	cmd.Flags().StringVar(&job, "job", "", "filter by job UUID")
	cmd.Flags().StringVar(&broadcastGroup, "broadcast-group", "", "filter by broadcast group UUID")
	cmd.AddCommand(getRunCommand(c))
	cmd.AddCommand(cancelRunCommand(c))
	cmd.AddCommand(runLogsCommand(c))
	cmd.AddCommand(purgeRunsCommand(c))
	return cmd
}
func purgeRunsCommand(c *cliConfig) *cobra.Command {
	var before, jobID string
	var limit int32
	cmd := &cobra.Command{Use: "purge", Short: "Delete terminal run history before a timestamp", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if _, err := time.Parse(time.RFC3339, before); err != nil {
			return errors.New("before must be an RFC3339 timestamp")
		}
		if limit < 1 || limit > 10000 {
			return errors.New("limit must be between 1 and 10000")
		}
		payload := func() ([]byte, error) {
			return json.Marshal(map[string]any{"before": before, "job_id": jobID, "limit": limit})
		}
		return runAuthenticated(c, http.MethodPost, "/api/v1/runs/purge", payload)(cmd, nil)
	}}
	cmd.Flags().StringVar(&before, "before", "", "delete terminal runs scheduled before RFC3339 timestamp")
	cmd.Flags().StringVar(&jobID, "job", "", "limit purge to one job UUID")
	cmd.Flags().Int32Var(&limit, "limit", 1000, "maximum runs to delete (1-10000)")
	_ = cmd.MarkFlagRequired("before")
	return cmd
}
func runLogsCommand(c *cliConfig) *cobra.Command {
	var after int64
	var limit int
	var follow bool
	var poll time.Duration
	cmd := &cobra.Command{Use: "logs ID", Short: "Read or follow rolling execution logs", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		if after < 0 || limit < 1 || limit > 500 || poll <= 0 {
			return errors.New("after must be non-negative, limit must be 1..500, and poll must be positive")
		}
		if err := c.authenticate(cmd.Context(), cmd.InOrStdin()); err != nil {
			return err
		}
		cursor := after
		for {
			query := url.Values{"after": {strconv.FormatInt(cursor, 10)}, "limit": {strconv.Itoa(limit)}}
			body, err := c.do(cmd.Context(), http.MethodGet, "/api/v1/runs/"+args[0]+"/logs?"+query.Encode(), nil, true)
			if err != nil {
				return err
			}
			if !follow {
				return writeJSON(cmd.OutOrStdout(), body)
			}
			var page struct {
				Entries []struct {
					Stream  string `json:"stream"`
					Content string `json:"content"`
				} `json:"entries"`
				NextCursor int64 `json:"next_cursor"`
			}
			if err = json.Unmarshal(body, &page); err != nil {
				return fmt.Errorf("decode log page: %w", err)
			}
			for _, entry := range page.Entries {
				if _, err = fmt.Fprintf(cmd.OutOrStdout(), "[%s] %s\n", entry.Stream, entry.Content); err != nil {
					return err
				}
			}
			cursor = page.NextCursor
			timer := time.NewTimer(poll)
			select {
			case <-cmd.Context().Done():
				timer.Stop()
				return cmd.Context().Err()
			case <-timer.C:
			}
		}
	}}
	cmd.Flags().Int64Var(&after, "after", 0, "read entries after this cursor")
	cmd.Flags().IntVar(&limit, "limit", 100, "maximum entries per request (1-500)")
	cmd.Flags().BoolVarP(&follow, "follow", "f", false, "poll and print new log entries")
	cmd.Flags().DurationVar(&poll, "poll", time.Second, "follow polling interval")
	return cmd
}
func getRunCommand(c *cliConfig) *cobra.Command {
	return &cobra.Command{Use: "get ID", Short: "Get a job run and its retry lineage", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodGet, "/api/v1/runs/"+args[0], nil)(cmd, nil)
	}}
}
func cancelRunCommand(c *cliConfig) *cobra.Command {
	var reason string
	cmd := &cobra.Command{Use: "cancel ID", Short: "Cancel a pending or running job run", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		payload := func() ([]byte, error) { return json.Marshal(map[string]string{"reason": reason}) }
		return runAuthenticated(c, http.MethodPost, "/api/v1/runs/"+args[0]+"/cancel", payload)(cmd, nil)
	}}
	cmd.Flags().StringVar(&reason, "reason", "", "operator cancellation reason")
	return cmd
}
func executorsCommand(c *cliConfig) *cobra.Command {
	cmd := &cobra.Command{Use: "executors", Short: "Manage executor groups and heartbeats"}
	groups := &cobra.Command{Use: "groups", Short: "Manage executor groups"}
	groups.AddCommand(&cobra.Command{Use: "list", Short: "List executor groups", Args: cobra.NoArgs, RunE: runAuthenticated(c, http.MethodGet, "/api/v1/executor-groups", nil)})
	var name, strategy, mode string
	var addresses []string
	create := &cobra.Command{Use: "create", Short: "Create an executor group", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		payload := func() ([]byte, error) {
			return json.Marshal(map[string]any{"name": name, "route_strategy": strategy, "registration_mode": mode, "manual_addresses": addresses})
		}
		return runAuthenticated(c, http.MethodPost, "/api/v1/executor-groups", payload)(cmd, nil)
	}}
	create.Flags().StringVar(&name, "name", "", "executor group name")
	create.Flags().StringVar(&strategy, "strategy", "round", "route strategy: first, last, round, random, hash, lfu, lru, failover, busyover")
	create.Flags().StringVar(&mode, "mode", "automatic", "registration mode: automatic or manual")
	create.Flags().StringArrayVar(&addresses, "address", nil, "manual HTTP(S) or gRPC executor URL; repeat for multiple nodes")
	_ = create.MarkFlagRequired("name")
	groups.AddCommand(create)
	var updateName, updateStrategy, updateMode string
	var updateAddresses []string
	var updateVersion int64
	update := &cobra.Command{Use: "update ID", Short: "Update an executor group", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		payload := func() ([]byte, error) {
			return json.Marshal(map[string]any{"name": updateName, "route_strategy": updateStrategy, "registration_mode": updateMode, "manual_addresses": updateAddresses, "version": updateVersion})
		}
		return runAuthenticated(c, http.MethodPut, "/api/v1/executor-groups/"+args[0], payload)(cmd, nil)
	}}
	update.Flags().StringVar(&updateName, "name", "", "executor group name")
	update.Flags().StringVar(&updateStrategy, "strategy", "round", "route strategy")
	update.Flags().StringVar(&updateMode, "mode", "automatic", "registration mode: automatic or manual")
	update.Flags().StringArrayVar(&updateAddresses, "address", nil, "manual HTTP(S) or gRPC executor URL; repeat for multiple nodes")
	update.Flags().Int64Var(&updateVersion, "version", 0, "expected executor group version")
	_ = update.MarkFlagRequired("name")
	_ = update.MarkFlagRequired("version")
	groups.AddCommand(update)
	var deleteVersion int64
	deleteGroup := &cobra.Command{Use: "delete ID", Short: "Delete an unused executor group", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodDelete, "/api/v1/executor-groups/"+args[0]+"?version="+strconv.FormatInt(deleteVersion, 10), nil)(cmd, nil)
	}}
	deleteGroup.Flags().Int64Var(&deleteVersion, "version", 0, "expected executor group version")
	_ = deleteGroup.MarkFlagRequired("version")
	groups.AddCommand(deleteGroup)
	cmd.AddCommand(groups)
	var address string
	var ttl int32
	var labels []string
	register := &cobra.Command{Use: "register GROUP_ID NODE_ID", Short: "Register or heartbeat an executor node", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		payload := func() ([]byte, error) {
			return json.Marshal(map[string]any{"address": address, "ttl_seconds": ttl, "labels": labels})
		}
		return runAuthenticated(c, http.MethodPut, "/api/v1/executor-groups/"+args[0]+"/nodes/"+args[1], payload)(cmd, nil)
	}}
	register.Flags().StringVar(&address, "address", "", "executor base URL")
	register.Flags().Int32Var(&ttl, "ttl", 30, "heartbeat TTL in seconds")
	register.Flags().StringSliceVar(&labels, "label", nil, "executor label (repeatable or comma-separated)")
	_ = register.MarkFlagRequired("address")
	cmd.AddCommand(register)
	cmd.AddCommand(&cobra.Command{Use: "unregister GROUP_ID NODE_ID", Short: "Remove an executor node immediately", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodDelete, "/api/v1/executor-groups/"+args[0]+"/nodes/"+args[1], nil)(cmd, nil)
	}})
	cmd.AddCommand(&cobra.Command{Use: "list GROUP_ID", Short: "List live executor nodes", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodGet, "/api/v1/executor-groups/"+args[0]+"/nodes", nil)(cmd, nil)
	}})
	return cmd
}

func kubernetesClustersCommand(c *cliConfig) *cobra.Command {
	cmd := &cobra.Command{Use: "kubernetes-clusters", Aliases: []string{"k8s-clusters"}, Short: "Manage Kubernetes execution clusters"}
	cmd.AddCommand(&cobra.Command{Use: "list", Args: cobra.NoArgs, RunE: runAuthenticated(c, http.MethodGet, "/api/v1/kubernetes-clusters", nil)})
	cmd.AddCommand(&cobra.Command{Use: "get ID", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodGet, "/api/v1/kubernetes-clusters/"+args[0], nil)(cmd, nil)
	}})
	addWrite := func(update bool) *cobra.Command {
		var name, authMode, apiServer, namespace, kubeconfigFile, token, caFile string
		var insecure bool
		var maxConcurrentJobs int32
		var version int64
		use := "create"
		argsValidator := cobra.NoArgs
		method := http.MethodPost
		if update {
			use, argsValidator, method = "update ID", cobra.ExactArgs(1), http.MethodPut
		}
		write := &cobra.Command{Use: use, Args: argsValidator, RunE: func(command *cobra.Command, args []string) error {
			var kubeconfig, caData []byte
			var err error
			if kubeconfigFile != "" {
				// #nosec G304 -- the operator explicitly supplies this CLI input path.
				kubeconfig, err = os.ReadFile(kubeconfigFile)
				if err != nil {
					return fmt.Errorf("read kubeconfig: %w", err)
				}
			}
			if caFile != "" {
				// #nosec G304 -- the operator explicitly supplies this CLI input path.
				caData, err = os.ReadFile(caFile)
				if err != nil {
					return fmt.Errorf("read CA file: %w", err)
				}
			}
			payload := func() ([]byte, error) {
				return json.Marshal(map[string]any{"name": name, "auth_mode": authMode, "api_server": apiServer, "namespace": namespace, "kubeconfig": string(kubeconfig), "token": token, "ca_data": string(caData), "insecure_skip_tls_verify": insecure, "max_concurrent_jobs": maxConcurrentJobs, "version": version})
			}
			path := "/api/v1/kubernetes-clusters"
			if update {
				path += "/" + args[0]
			}
			return runAuthenticated(c, method, path, payload)(command, nil)
		}}
		write.Flags().StringVar(&name, "name", "", "cluster name")
		write.Flags().StringVar(&authMode, "auth-mode", "kubeconfig", "kubeconfig or service_account")
		write.Flags().StringVar(&apiServer, "api-server", "", "Kubernetes API Server URL")
		write.Flags().StringVar(&namespace, "namespace", "default", "default Job namespace")
		write.Flags().StringVar(&kubeconfigFile, "kubeconfig", "", "path to kubeconfig (omit on update to preserve credentials)")
		write.Flags().StringVar(&token, "service-account-token", "", "ServiceAccount bearer token (omit on update to preserve credentials)")
		write.Flags().StringVar(&caFile, "ca-file", "", "path to API Server CA certificate (omit on update to preserve credentials)")
		write.Flags().BoolVar(&insecure, "insecure-skip-tls-verify", false, "skip API Server TLS verification")
		write.Flags().Int32Var(&maxConcurrentJobs, "max-concurrent-jobs", 100, "maximum active jobs across this Kubernetes cluster")
		write.Flags().Int64Var(&version, "version", 0, "expected cluster version (update only)")
		_ = write.MarkFlagRequired("name")
		if update {
			_ = write.MarkFlagRequired("version")
		}
		return write
	}
	cmd.AddCommand(addWrite(false), addWrite(true))
	var deleteVersion int64
	remove := &cobra.Command{Use: "delete ID", Args: cobra.ExactArgs(1), RunE: func(command *cobra.Command, args []string) error {
		return runAuthenticated(c, http.MethodDelete, "/api/v1/kubernetes-clusters/"+args[0]+"?version="+strconv.FormatInt(deleteVersion, 10), nil)(command, nil)
	}}
	remove.Flags().Int64Var(&deleteVersion, "version", 0, "expected cluster version")
	_ = remove.MarkFlagRequired("version")
	cmd.AddCommand(remove)
	return cmd
}

func notificationsCommand(c *cliConfig) *cobra.Command {
	cmd := &cobra.Command{Use: "notifications", Short: "Manage notification subscriptions and delivery history"}
	cmd.AddCommand(&cobra.Command{Use: "list", Short: "List active and disabled notification channels", Args: cobra.NoArgs, RunE: runAuthenticated(c, http.MethodGet, "/api/v1/notification-channels", nil)})
	var kind, name, config string
	var events, jobIDs []string
	var allJobs bool
	var maxAttempts, initialBackoff, maxBackoff int
	create := &cobra.Command{Use: "create", Short: "Create a webhook or email channel", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		if !json.Valid([]byte(config)) {
			return errors.New("config must be valid JSON")
		}
		payload := func() ([]byte, error) {
			return json.Marshal(map[string]any{"kind": kind, "name": name, "config": json.RawMessage(config), "events": events, "all_jobs": allJobs, "job_ids": jobIDs, "max_attempts": maxAttempts, "backoff_initial_seconds": initialBackoff, "backoff_max_seconds": maxBackoff})
		}
		return runAuthenticated(c, http.MethodPost, "/api/v1/notification-channels", payload)(cmd, nil)
	}}
	create.Flags().StringVar(&kind, "kind", "", "channel kind: webhook, email, or dingtalk")
	create.Flags().StringVar(&name, "name", "", "channel name")
	create.Flags().StringVar(&config, "config", "", "channel configuration JSON")
	create.Flags().StringSliceVar(&events, "events", []string{"exhausted"}, "run lifecycle events")
	create.Flags().BoolVar(&allJobs, "all-jobs", true, "subscribe to all jobs")
	create.Flags().StringSliceVar(&jobIDs, "job-ids", nil, "specific job IDs when --all-jobs=false")
	create.Flags().IntVar(&maxAttempts, "max-attempts", 8, "maximum delivery attempts")
	create.Flags().IntVar(&initialBackoff, "backoff-initial-seconds", 2, "initial retry backoff")
	create.Flags().IntVar(&maxBackoff, "backoff-max-seconds", 300, "maximum retry backoff")
	_ = create.MarkFlagRequired("kind")
	_ = create.MarkFlagRequired("name")
	_ = create.MarkFlagRequired("config")
	cmd.AddCommand(create)
	var updateKind, updateName, updateConfig string
	var updateEvents, updateJobIDs []string
	var updateAllJobs bool
	var updateMaxAttempts, updateInitialBackoff, updateMaxBackoff int
	var updateVersion int64
	update := &cobra.Command{Use: "update ID", Short: "Update a notification channel", Args: cobra.ExactArgs(1), RunE: func(command *cobra.Command, args []string) error {
		configChanged := command.Flags().Changed("config")
		if configChanged && !json.Valid([]byte(updateConfig)) {
			return errors.New("config must be valid JSON")
		}
		payload := func() ([]byte, error) {
			body := map[string]any{"kind": updateKind, "name": updateName, "events": updateEvents, "all_jobs": updateAllJobs, "job_ids": updateJobIDs, "max_attempts": updateMaxAttempts, "backoff_initial_seconds": updateInitialBackoff, "backoff_max_seconds": updateMaxBackoff, "version": updateVersion}
			if configChanged {
				body["config"] = json.RawMessage(updateConfig)
			}
			return json.Marshal(body)
		}
		return runAuthenticated(c, http.MethodPut, "/api/v1/notification-channels/"+url.PathEscape(args[0]), payload)(command, nil)
	}}
	update.Flags().StringVar(&updateKind, "kind", "", "channel kind: webhook, email, or dingtalk")
	update.Flags().StringVar(&updateName, "name", "", "channel name")
	update.Flags().StringVar(&updateConfig, "config", "", "replacement channel configuration JSON (omit to preserve)")
	update.Flags().StringSliceVar(&updateEvents, "events", []string{"exhausted"}, "run lifecycle events")
	update.Flags().BoolVar(&updateAllJobs, "all-jobs", true, "subscribe to all jobs")
	update.Flags().StringSliceVar(&updateJobIDs, "job-ids", nil, "specific job IDs when --all-jobs=false")
	update.Flags().IntVar(&updateMaxAttempts, "max-attempts", 8, "maximum delivery attempts")
	update.Flags().IntVar(&updateInitialBackoff, "backoff-initial-seconds", 2, "initial retry backoff")
	update.Flags().IntVar(&updateMaxBackoff, "backoff-max-seconds", 300, "maximum retry backoff")
	update.Flags().Int64Var(&updateVersion, "version", 0, "expected channel version")
	for _, flag := range []string{"kind", "name", "version"} {
		_ = update.MarkFlagRequired(flag)
	}
	cmd.AddCommand(update)
	var enabledVersion int64
	setEnabled := func(enabled bool) *cobra.Command {
		name := "disable ID"
		short := "Disable a notification channel"
		if enabled {
			name = "enable ID"
			short = "Enable a notification channel"
		}
		command := &cobra.Command{Use: name, Short: short, Args: cobra.ExactArgs(1), RunE: func(command *cobra.Command, args []string) error {
			payload := func() ([]byte, error) {
				return json.Marshal(map[string]any{"enabled": enabled, "version": enabledVersion})
			}
			return runAuthenticated(c, http.MethodPut, "/api/v1/notification-channels/"+url.PathEscape(args[0])+"/enabled", payload)(command, nil)
		}}
		command.Flags().Int64Var(&enabledVersion, "version", 0, "expected channel version")
		_ = command.MarkFlagRequired("version")
		return command
	}
	cmd.AddCommand(setEnabled(true), setEnabled(false))
	var deleteVersion int64
	remove := &cobra.Command{Use: "delete ID", Short: "Delete a notification channel while preserving its history", Args: cobra.ExactArgs(1), RunE: func(command *cobra.Command, args []string) error {
		path := "/api/v1/notification-channels/" + url.PathEscape(args[0]) + "?version=" + strconv.FormatInt(deleteVersion, 10)
		return runAuthenticated(c, http.MethodDelete, path, nil)(command, nil)
	}}
	remove.Flags().Int64Var(&deleteVersion, "version", 0, "expected channel version")
	_ = remove.MarkFlagRequired("version")
	cmd.AddCommand(remove)
	var historyChannelID, historyJobID, historyStatus, historyCursor string
	var historyLimit int
	history := &cobra.Command{Use: "history", Short: "Query notification delivery history", Args: cobra.NoArgs, RunE: func(command *cobra.Command, _ []string) error {
		if err := validateNotificationHistoryFilters(historyChannelID, historyJobID, historyStatus, historyLimit); err != nil {
			return err
		}
		query := url.Values{}
		query.Set("limit", strconv.Itoa(historyLimit))
		if historyChannelID != "" {
			query.Set("channel_id", historyChannelID)
		}
		if historyJobID != "" {
			query.Set("job_id", historyJobID)
		}
		if historyStatus != "" {
			query.Set("status", historyStatus)
		}
		if historyCursor != "" {
			query.Set("cursor", historyCursor)
		}
		return runAuthenticated(c, http.MethodGet, "/api/v1/notification-history?"+query.Encode(), nil)(command, nil)
	}}
	history.Flags().StringVar(&historyChannelID, "channel-id", "", "filter by channel ID")
	history.Flags().StringVar(&historyJobID, "job-id", "", "filter by job ID")
	history.Flags().StringVar(&historyStatus, "status", "", "filter by pending, delivered, or dead")
	history.Flags().StringVar(&historyCursor, "cursor", "", "continue from a previous next_cursor")
	history.Flags().IntVar(&historyLimit, "limit", 100, "maximum rows (1-500)")
	cmd.AddCommand(history)
	return cmd
}

func validateNotificationHistoryFilters(channelID, jobID, deliveryStatus string, limit int) error {
	if channelID != "" && uuid.Validate(channelID) != nil {
		return errors.New("channel-id must be a UUID")
	}
	if jobID != "" && uuid.Validate(jobID) != nil {
		return errors.New("job-id must be a UUID")
	}
	switch deliveryStatus {
	case "", "pending", "delivered", "dead":
	default:
		return errors.New("status must be pending, delivered, or dead")
	}
	if limit < 1 || limit > 500 {
		return errors.New("limit must be between 1 and 500")
	}
	return nil
}

func versionCommand(value string) *cobra.Command {
	return &cobra.Command{Use: "version", Short: "Print version", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, _ []string) error {
		_, err := fmt.Fprintln(cmd.OutOrStdout(), value)
		return err
	}}
}
func completionCommand(root *cobra.Command) *cobra.Command {
	return &cobra.Command{Use: "completion [bash|zsh|fish|powershell]", Short: "Generate shell completion", Args: cobra.MatchAll(cobra.ExactArgs(1), cobra.OnlyValidArgs), ValidArgs: []string{"bash", "zsh", "fish", "powershell"}, RunE: func(cmd *cobra.Command, args []string) error {
		switch args[0] {
		case "bash":
			return root.GenBashCompletion(cmd.OutOrStdout())
		case "zsh":
			return root.GenZshCompletion(cmd.OutOrStdout())
		case "fish":
			return root.GenFishCompletion(cmd.OutOrStdout(), true)
		default:
			return root.GenPowerShellCompletion(cmd.OutOrStdout())
		}
	}}
}
