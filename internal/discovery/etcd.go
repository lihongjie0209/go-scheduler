package discovery

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path"
	"sync"
	"time"

	"go.etcd.io/etcd/api/v3/mvccpb"
	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc/resolver"
)

type Metadata struct {
	InstanceID  string    `json:"instance_id"`
	GRPCAddress string    `json:"grpc_address,omitempty"`
	HTTPAddress string    `json:"http_address,omitempty"`
	Version     string    `json:"version"`
	StartedAt   time.Time `json:"started_at"`
	Draining    bool      `json:"draining"`
}

func NewClient(endpoints []string, username, password, caFile, certFile, keyFile string) (*clientv3.Client, error) {
	tlsConfig, err := loadTLS(caFile, certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return clientv3.New(clientv3.Config{Endpoints: endpoints, Username: username, Password: password, TLS: tlsConfig, DialTimeout: 5 * time.Second})
}
func loadTLS(caFile, certFile, keyFile string) (*tls.Config, error) {
	if caFile == "" && certFile == "" && keyFile == "" {
		return nil, nil
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12}
	if caFile != "" {
		pem, err := os.ReadFile(caFile) // #nosec G304 -- path is trusted operator configuration, never request input.
		if err != nil {
			return nil, fmt.Errorf("read etcd CA: %w", err)
		}
		roots := x509.NewCertPool()
		if !roots.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("parse etcd CA")
		}
		config.RootCAs = roots
	}
	if certFile != "" || keyFile != "" {
		if certFile == "" || keyFile == "" {
			return nil, fmt.Errorf("etcd client certificate and key must be configured together")
		}
		certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
		if err != nil {
			return nil, fmt.Errorf("load etcd client certificate: %w", err)
		}
		config.Certificates = []tls.Certificate{certificate}
	}
	return config, nil
}

type Registrar struct {
	client *clientv3.Client
	key    string
	value  []byte
	mu     sync.Mutex
	lease  clientv3.LeaseID
}

func NewRegistrar(client *clientv3.Client, prefix, service string, m Metadata) (*Registrar, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, err
	}
	return &Registrar{client: client, key: path.Join(prefix, service, m.InstanceID), value: b}, nil
}
func (r *Registrar) Run(ctx context.Context) error {
	for {
		if err := r.register(ctx); err != nil {
			slog.Error("etcd registration failed", "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
				continue
			}
		}
		return nil
	}
}
func (r *Registrar) register(ctx context.Context) error {
	lease, err := r.client.Grant(ctx, 15)
	if err != nil {
		return err
	}
	r.mu.Lock()
	r.lease = lease.ID
	r.mu.Unlock()
	if _, err = r.client.Put(ctx, r.key, string(r.value), clientv3.WithLease(lease.ID)); err != nil {
		return err
	}
	ch, err := r.client.KeepAlive(ctx, lease.ID)
	if err != nil {
		return err
	}
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case resp, ok := <-ch:
			if !ok || resp == nil {
				return fmt.Errorf("keepalive channel closed")
			}
		}
	}
}
func (r *Registrar) Close(ctx context.Context) error {
	r.mu.Lock()
	lease := r.lease
	r.mu.Unlock()
	if lease != 0 {
		_, err := r.client.Revoke(ctx, lease)
		return err
	}
	return nil
}

type Builder struct {
	client *clientv3.Client
	prefix string
}

func NewBuilder(client *clientv3.Client, prefix string) *Builder {
	return &Builder{client: client, prefix: prefix}
}
func (b *Builder) Scheme() string { return "etcd" }
func (b *Builder) Build(target resolver.Target, cc resolver.ClientConn, _ resolver.BuildOptions) (resolver.Resolver, error) {
	r := &etcdResolver{client: b.client, prefix: path.Join(b.prefix, target.Endpoint()), cc: cc}
	ctx, cancel := context.WithCancel(context.Background())
	r.cancel = cancel
	go r.watch(ctx)
	return r, nil
}

type etcdResolver struct {
	client *clientv3.Client
	prefix string
	cc     resolver.ClientConn
	cancel context.CancelFunc
}

func (r *etcdResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (r *etcdResolver) Close()                                { r.cancel() }
func (r *etcdResolver) watch(ctx context.Context) {
	for ctx.Err() == nil {
		resp, err := r.client.Get(ctx, r.prefix, clientv3.WithPrefix())
		if err != nil {
			r.cc.ReportError(err)
			if !wait(ctx, time.Second) {
				return
			}
			continue
		}
		r.publish(resp.Kvs)
		revision := resp.Header.Revision + 1
		watch := r.client.Watch(ctx, r.prefix, clientv3.WithPrefix(), clientv3.WithRev(revision))
		restart := false
		for event := range watch {
			if event.Err() != nil {
				r.cc.ReportError(event.Err())
				restart = true
				break
			}
			latest, err := r.client.Get(ctx, r.prefix, clientv3.WithPrefix())
			if err != nil {
				r.cc.ReportError(err)
				continue
			}
			r.publish(latest.Kvs)
		}
		if !restart && !wait(ctx, time.Second) {
			return
		}
	}
}
func (r *etcdResolver) publish(kvs []*mvccpb.KeyValue) {
	addresses := make([]resolver.Address, 0, len(kvs))
	for _, kv := range kvs {
		var m Metadata
		if json.Unmarshal(kv.Value, &m) == nil && !m.Draining && m.GRPCAddress != "" {
			addresses = append(addresses, resolver.Address{Addr: m.GRPCAddress, ServerName: m.InstanceID})
		}
	}
	_ = r.cc.UpdateState(resolver.State{Addresses: addresses})
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
