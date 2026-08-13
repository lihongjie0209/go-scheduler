package perfbench

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type XXLRegistrarOptions struct {
	AdminURL, AccessToken, AppName, Address string
	Interval                                time.Duration
	HTTPClient                              *http.Client
}

type XXLRegistrar struct {
	options XXLRegistrarOptions
	client  *http.Client
}

func NewXXLRegistrar(options XXLRegistrarOptions) (*XXLRegistrar, error) {
	if options.AdminURL == "" || options.AccessToken == "" || options.AppName == "" || options.Address == "" {
		return nil, fmt.Errorf("xxl-job admin URL, access token, app name, and address are required")
	}
	if options.Interval < 5*time.Second || options.Interval > 60*time.Second {
		return nil, fmt.Errorf("xxl-job heartbeat interval must be between 5 and 60 seconds")
	}
	for name, raw := range map[string]string{"admin URL": options.AdminURL, "executor address": options.Address} {
		parsed, err := url.Parse(raw)
		if err != nil || (parsed.Scheme != "http" && parsed.Scheme != "https") || parsed.Host == "" {
			return nil, fmt.Errorf("xxl-job %s must be absolute HTTP or HTTPS", name)
		}
	}
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	return &XXLRegistrar{options: options, client: client}, nil
}

func (r *XXLRegistrar) Run(ctx context.Context) error {
	ticker := time.NewTicker(r.options.Interval)
	defer ticker.Stop()
	for {
		if err := r.send(ctx, "registry"); err != nil && ctx.Err() == nil {
			return err
		}
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
			defer cancel()
			_ = r.send(shutdownCtx, "registryRemove")
			return nil
		case <-ticker.C:
		}
	}
}

func (r *XXLRegistrar) send(ctx context.Context, operation string) error {
	payload, err := json.Marshal(map[string]string{"registryGroup": "EXECUTOR", "registryKey": r.options.AppName, "registryValue": strings.TrimRight(r.options.Address, "/")})
	if err != nil {
		return err
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(r.options.AdminURL, "/")+"/api/"+operation, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("XXL-JOB-ACCESS-TOKEN", r.options.AccessToken)
	request.Header.Set("XXL-JOB-APPNAME", r.options.AppName)
	response, err := r.client.Do(request)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 4096))
	if err != nil {
		return err
	}
	if response.StatusCode != http.StatusOK {
		return fmt.Errorf("xxl-job %s returned HTTP %d", operation, response.StatusCode)
	}
	var result xxlResponse
	if err = json.Unmarshal(body, &result); err != nil {
		return err
	}
	if result.Code != http.StatusOK {
		return fmt.Errorf("xxl-job %s failed: code=%d message=%s", operation, result.Code, result.Msg)
	}
	return nil
}
