package core

import (
	"context"
	"fmt"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type KubernetesClusterReader interface {
	ListKubernetesClusters(context.Context, string) ([]store.KubernetesCluster, error)
	GetKubernetesCluster(context.Context, string, string) (store.KubernetesCluster, error)
}

type KubernetesClusterWriter interface {
	CreateKubernetesCluster(context.Context, store.KubernetesCluster) (store.KubernetesCluster, error)
	UpdateKubernetesCluster(context.Context, store.KubernetesCluster) (store.KubernetesCluster, error)
	DeleteKubernetesCluster(context.Context, string, string, int64) error
}

type KubernetesClusterService struct {
	reader KubernetesClusterReader
	writer KubernetesClusterWriter
}

type KubernetesClusterInput struct {
	Cluster  store.KubernetesCluster
	Provided bool
}

func NewKubernetesClusterService(reader KubernetesClusterReader, writer KubernetesClusterWriter) *KubernetesClusterService {
	return &KubernetesClusterService{reader: reader, writer: writer}
}

func (s *KubernetesClusterService) List(ctx context.Context, tenantID string) ([]store.KubernetesCluster, error) {
	return s.reader.ListKubernetesClusters(ctx, tenantID)
}

func (s *KubernetesClusterService) Get(ctx context.Context, tenantID, id string) (store.KubernetesCluster, error) {
	return s.reader.GetKubernetesCluster(ctx, tenantID, id)
}

func (s *KubernetesClusterService) Create(ctx context.Context, input KubernetesClusterInput) (store.KubernetesCluster, error) {
	if !input.Provided {
		return store.KubernetesCluster{}, &ValidationError{err: fmt.Errorf("cluster is required")}
	}
	return s.writer.CreateKubernetesCluster(ctx, input.Cluster)
}

func (s *KubernetesClusterService) Update(ctx context.Context, input KubernetesClusterInput) (store.KubernetesCluster, error) {
	if !input.Provided {
		return store.KubernetesCluster{}, &ValidationError{err: fmt.Errorf("cluster is required")}
	}
	cluster := input.Cluster
	if cluster.Credentials.Kubeconfig == "" && cluster.Credentials.Token == "" && cluster.Credentials.CAData == "" {
		current, err := s.reader.GetKubernetesCluster(ctx, cluster.TenantID, cluster.ID)
		if err != nil {
			return store.KubernetesCluster{}, err
		}
		if current.AuthMode != cluster.AuthMode {
			return store.KubernetesCluster{}, &ValidationError{err: fmt.Errorf("credentials are required when changing auth_mode")}
		}
		cluster.Credentials = current.Credentials
	}
	return s.writer.UpdateKubernetesCluster(ctx, cluster)
}

func (s *KubernetesClusterService) Delete(ctx context.Context, tenantID, id string, version int64) error {
	return s.writer.DeleteKubernetesCluster(ctx, tenantID, id, version)
}
