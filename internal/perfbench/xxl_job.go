package perfbench

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"strconv"
	"strings"
	"sync"
)

type XXLJobLoader struct {
	BaseURL         string
	Username        string
	Password        string
	ExecutorGroupID int
	ExecutorHandler string
	Client          *http.Client
	mu              sync.Mutex
}

type xxlResponse struct {
	Code int             `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

func (l *XXLJobLoader) Login(ctx context.Context) error {
	if l.Username == "" || l.Password == "" {
		return fmt.Errorf("XXL-JOB username and password are required")
	}
	client, err := l.sessionClient()
	if err != nil {
		return err
	}
	response, err := l.postForm(ctx, client, "/auth/doLogin", url.Values{"userName": {l.Username}, "password": {l.Password}})
	if err != nil {
		return err
	}
	if response.Code != http.StatusOK {
		return fmt.Errorf("XXL-JOB login failed: code=%d message=%s", response.Code, response.Msg)
	}
	return nil
}

func (l *XXLJobLoader) CreateScheduledJob(ctx context.Context, job ScheduledJob) (string, error) {
	if err := ValidateScheduledJob(job); err != nil {
		return "", err
	}
	if l.ExecutorGroupID < 1 || l.ExecutorHandler == "" {
		return "", fmt.Errorf("XXL-JOB executor group and handler are required")
	}
	client, err := l.sessionClient()
	if err != nil {
		return "", err
	}
	form := url.Values{
		"jobGroup":               {strconv.Itoa(l.ExecutorGroupID)},
		"name":                   {job.Name},
		"author":                 {"scheduler-bench"},
		"scheduleType":           {"CRON"},
		"scheduleConf":           {QuartzCron(job.ScheduledAt)},
		"misfireStrategy":        {"DO_NOTHING"},
		"executorRouteStrategy":  {"FIRST"},
		"executorHandler":        {l.ExecutorHandler},
		"executorParam":          {job.EventID},
		"executorBlockStrategy":  {"SERIAL_EXECUTION"},
		"executorTimeout":        {"10"},
		"executorFailRetryCount": {"0"},
		"glueType":               {"BEAN"},
	}
	created, err := l.postForm(ctx, client, "/jobinfo/insert", form)
	if err != nil {
		return "", err
	}
	if created.Code != http.StatusOK {
		return "", fmt.Errorf("XXL-JOB create job failed: code=%d message=%s", created.Code, created.Msg)
	}
	var jobID string
	if err = json.Unmarshal(created.Data, &jobID); err != nil {
		return "", fmt.Errorf("decode XXL-JOB job ID: %w", err)
	}
	if jobID == "" {
		return "", fmt.Errorf("XXL-JOB create response has no job ID")
	}
	started, err := l.postForm(ctx, client, "/jobinfo/start", url.Values{"ids[]": {jobID}})
	if err != nil {
		return "", err
	}
	if started.Code != http.StatusOK {
		return "", fmt.Errorf("XXL-JOB start job %s failed: code=%d message=%s", jobID, started.Code, started.Msg)
	}
	return jobID, nil
}

func (l *XXLJobLoader) sessionClient() (*http.Client, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.Client != nil {
		return l.Client, nil
	}
	jar, err := cookiejar.New(nil)
	if err != nil {
		return nil, err
	}
	l.Client = &http.Client{Jar: jar}
	return l.Client, nil
}

func (l *XXLJobLoader) postForm(ctx context.Context, client *http.Client, path string, form url.Values) (xxlResponse, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(l.BaseURL, "/")+path, strings.NewReader(form.Encode()))
	if err != nil {
		return xxlResponse{}, err
	}
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := client.Do(request)
	if err != nil {
		return xxlResponse{}, err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxRequestBody))
	if err != nil {
		return xxlResponse{}, err
	}
	if response.StatusCode != http.StatusOK {
		return xxlResponse{}, fmt.Errorf("XXL-JOB %s returned HTTP %d: %s", path, response.StatusCode, strings.TrimSpace(string(body)))
	}
	var result xxlResponse
	if err = json.Unmarshal(body, &result); err != nil {
		return xxlResponse{}, fmt.Errorf("decode XXL-JOB %s response: %w", path, err)
	}
	return result, nil
}
