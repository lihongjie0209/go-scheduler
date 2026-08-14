package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

var ErrKubernetesClusterInUse = errors.New("kubernetes cluster is referenced by a job")

type KubernetesCredentials struct {
	Kubeconfig string `json:"kubeconfig,omitempty"`
	Token      string `json:"token,omitempty"`
	CAData     string `json:"ca_data,omitempty"`
}

type KubernetesCluster struct {
	ID, TenantID, Name, AuthMode, APIServer, Namespace string
	Credentials                                        KubernetesCredentials `json:"-"`
	InsecureSkipTLSVerify                              bool
	MaxConcurrentJobs                                  int32
	Version                                            int64
	CreatedAt, UpdatedAt                               time.Time
}

func validateKubernetesCluster(cluster KubernetesCluster) error {
	cluster.Name = strings.TrimSpace(cluster.Name)
	cluster.Namespace = strings.TrimSpace(cluster.Namespace)
	if cluster.Name == "" || len(cluster.Name) > 128 {
		return errors.New("cluster name must be between 1 and 128 characters")
	}
	if cluster.Namespace == "" || len(cluster.Namespace) > 253 {
		return errors.New("namespace must be between 1 and 253 characters")
	}
	if cluster.MaxConcurrentJobs < 1 || cluster.MaxConcurrentJobs > 1_000_000 {
		return errors.New("max_concurrent_jobs must be between 1 and 1000000")
	}
	switch cluster.AuthMode {
	case "kubeconfig":
		if strings.TrimSpace(cluster.Credentials.Kubeconfig) == "" {
			return errors.New("kubeconfig is required")
		}
	case "service_account":
		if strings.TrimSpace(cluster.APIServer) == "" || strings.TrimSpace(cluster.Credentials.Token) == "" {
			return errors.New("api_server and token are required")
		}
		parsed, err := url.ParseRequestURI(cluster.APIServer)
		if err != nil || parsed.Host == "" || (parsed.Scheme != "https" && parsed.Scheme != "http") {
			return errors.New("api_server must be an absolute HTTP or HTTPS URL")
		}
	default:
		return errors.New("auth_mode must be kubeconfig or service_account")
	}
	return nil
}

func (s *Store) CreateKubernetesCluster(ctx context.Context, cluster KubernetesCluster) (KubernetesCluster, error) {
	if cluster.MaxConcurrentJobs == 0 {
		cluster.MaxConcurrentJobs = 100
	}
	if err := validateKubernetesCluster(cluster); err != nil {
		return KubernetesCluster{}, err
	}
	if s.headerCipher == nil {
		return KubernetesCluster{}, errors.New("kubernetes credentials require store cipher")
	}
	cluster.ID = uuid.NewString()
	cluster.Name, cluster.Namespace = strings.TrimSpace(cluster.Name), strings.TrimSpace(cluster.Namespace)
	plain, err := json.Marshal(cluster.Credentials)
	if err != nil {
		return KubernetesCluster{}, err
	}
	encrypted, keyVersion, err := s.headerCipher.Encrypt(plain)
	if err != nil {
		return KubernetesCluster{}, fmt.Errorf("encrypt kubernetes credentials: %w", err)
	}
	err = s.pool.QueryRow(ctx, `INSERT INTO kubernetes_clusters(id,tenant_id,name,auth_mode,api_server,namespace,encrypted_credentials,encryption_key_version,insecure_skip_tls_verify,max_concurrent_jobs)
		VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10) RETURNING version,created_at,updated_at`, cluster.ID, cluster.TenantID, cluster.Name, cluster.AuthMode, strings.TrimSpace(cluster.APIServer), cluster.Namespace, encrypted, keyVersion, cluster.InsecureSkipTLSVerify, cluster.MaxConcurrentJobs).Scan(&cluster.Version, &cluster.CreatedAt, &cluster.UpdatedAt)
	return cluster, err
}

func (s *Store) scanKubernetesCluster(row pgx.Row) (KubernetesCluster, error) {
	var cluster KubernetesCluster
	var encrypted []byte
	var keyVersion int
	err := row.Scan(&cluster.ID, &cluster.TenantID, &cluster.Name, &cluster.AuthMode, &cluster.APIServer, &cluster.Namespace, &encrypted, &keyVersion, &cluster.InsecureSkipTLSVerify, &cluster.MaxConcurrentJobs, &cluster.Version, &cluster.CreatedAt, &cluster.UpdatedAt)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return KubernetesCluster{}, ErrNotFound
		}
		return KubernetesCluster{}, err
	}
	if s.headerCipher == nil {
		return KubernetesCluster{}, errors.New("encrypted kubernetes credentials require store cipher")
	}
	plain, err := s.headerCipher.Decrypt(encrypted, keyVersion)
	if err != nil {
		return KubernetesCluster{}, fmt.Errorf("decrypt kubernetes credentials: %w", err)
	}
	if err = json.Unmarshal(plain, &cluster.Credentials); err != nil {
		return KubernetesCluster{}, fmt.Errorf("decode kubernetes credentials: %w", err)
	}
	return cluster, nil
}

const kubernetesClusterColumns = `id,tenant_id,name,auth_mode,api_server,namespace,encrypted_credentials,encryption_key_version,insecure_skip_tls_verify,max_concurrent_jobs,version,created_at,updated_at`

func (s *Store) GetKubernetesCluster(ctx context.Context, tenantID, id string) (KubernetesCluster, error) {
	return s.scanKubernetesCluster(s.pool.QueryRow(ctx, `SELECT `+kubernetesClusterColumns+` FROM kubernetes_clusters WHERE tenant_id=$1 AND id=$2`, tenantID, id))
}

func (s *Store) ListKubernetesClusters(ctx context.Context, tenantID string) ([]KubernetesCluster, error) {
	rows, err := s.pool.Query(ctx, `SELECT `+kubernetesClusterColumns+` FROM kubernetes_clusters WHERE tenant_id=$1 ORDER BY name,id`, tenantID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	clusters := make([]KubernetesCluster, 0)
	for rows.Next() {
		cluster, scanErr := s.scanKubernetesCluster(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		clusters = append(clusters, cluster)
	}
	return clusters, rows.Err()
}

func (s *Store) UpdateKubernetesCluster(ctx context.Context, cluster KubernetesCluster) (KubernetesCluster, error) {
	if cluster.MaxConcurrentJobs == 0 {
		cluster.MaxConcurrentJobs = 100
	}
	if err := validateKubernetesCluster(cluster); err != nil {
		return KubernetesCluster{}, err
	}
	if s.headerCipher == nil {
		return KubernetesCluster{}, errors.New("kubernetes credentials require store cipher")
	}
	plain, err := json.Marshal(cluster.Credentials)
	if err != nil {
		return KubernetesCluster{}, err
	}
	encrypted, keyVersion, err := s.headerCipher.Encrypt(plain)
	if err != nil {
		return KubernetesCluster{}, fmt.Errorf("encrypt kubernetes credentials: %w", err)
	}
	err = s.pool.QueryRow(ctx, `UPDATE kubernetes_clusters SET name=$3,auth_mode=$4,api_server=$5,namespace=$6,encrypted_credentials=$7,encryption_key_version=$8,insecure_skip_tls_verify=$9,max_concurrent_jobs=$10,version=version+1,updated_at=now()
		WHERE tenant_id=$1 AND id=$2 AND version=$11 RETURNING version,created_at,updated_at`, cluster.TenantID, cluster.ID, strings.TrimSpace(cluster.Name), cluster.AuthMode, strings.TrimSpace(cluster.APIServer), strings.TrimSpace(cluster.Namespace), encrypted, keyVersion, cluster.InsecureSkipTLSVerify, cluster.MaxConcurrentJobs, cluster.Version).Scan(&cluster.Version, &cluster.CreatedAt, &cluster.UpdatedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return KubernetesCluster{}, ErrConflict
	}
	return cluster, err
}

func (s *Store) DeleteKubernetesCluster(ctx context.Context, tenantID, id string, version int64) error {
	result, err := s.pool.Exec(ctx, `DELETE FROM kubernetes_clusters WHERE tenant_id=$1 AND id=$2 AND version=$3`, tenantID, id, version)
	if err != nil {
		if strings.Contains(err.Error(), "jobs_kubernetes_cluster_fk") {
			return ErrKubernetesClusterInUse
		}
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrConflict
	}
	return nil
}
