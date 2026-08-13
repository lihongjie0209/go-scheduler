package executor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type RegistrarOptions struct {
	APIURL, Token, GroupID, NodeID, Address string
	TTL                                     time.Duration
	HTTPClient                              *http.Client
}
type Registrar struct {
	options  RegistrarOptions
	endpoint string
	client   *http.Client
}

func NewRegistrar(options RegistrarOptions) (*Registrar, error) {
	api, err := url.Parse(options.APIURL)
	if err != nil || api.Scheme == "" || api.Host == "" || (api.Scheme != "http" && api.Scheme != "https") {
		return nil, errors.New("API URL must be absolute HTTP(S)")
	}
	address, err := url.Parse(options.Address)
	if err != nil || address.Scheme == "" || address.Host == "" || (address.Scheme != "http" && address.Scheme != "https") {
		return nil, errors.New("executor address must be absolute HTTP(S)")
	}
	if strings.TrimSpace(options.Token) == "" || strings.TrimSpace(options.GroupID) == "" || strings.TrimSpace(options.NodeID) == "" {
		return nil, errors.New("token, group ID, and node ID are required")
	}
	if options.TTL < 5*time.Second || options.TTL > 300*time.Second {
		return nil, errors.New("TTL must be between 5 and 300 seconds")
	}
	client := options.HTTPClient
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}
	endpoint := strings.TrimRight(options.APIURL, "/") + "/api/v1/executor-groups/" + url.PathEscape(options.GroupID) + "/nodes/" + url.PathEscape(options.NodeID)
	return &Registrar{options: options, endpoint: endpoint, client: client}, nil
}

func (r *Registrar) Run(ctx context.Context) error {
	interval := r.options.TTL / 3
	ticker := time.NewTicker(interval)
	defer func() {
		ticker.Stop()
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 5*time.Second)
		defer cancel()
		_ = r.unregister(shutdownCtx)
	}()
	for {
		_ = r.heartbeat(ctx)
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}
func (r *Registrar) unregister(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, r.endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.options.Token)
	response, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("executor unregistration returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
func (r *Registrar) heartbeat(ctx context.Context) error {
	ttl := r.options.TTL / time.Second
	payload, err := json.Marshal(map[string]any{"address": strings.TrimRight(r.options.Address, "/"), "ttl_seconds": ttl})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, r.endpoint, bytes.NewReader(payload))
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", "Bearer "+r.options.Token)
	req.Header.Set("Content-Type", "application/json")
	response, err := r.client.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = response.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("executor registration returned HTTP %d: %s", response.StatusCode, strings.TrimSpace(string(body)))
	}
	return nil
}
