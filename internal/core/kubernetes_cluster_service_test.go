package core

import (
	"context"
	"errors"
	"testing"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type fakeKubernetesClusterRepository struct {
	current     store.KubernetesCluster
	updated     store.KubernetesCluster
	getCalls    int
	updateCalls int
}

func (r *fakeKubernetesClusterRepository) ListKubernetesClusters(context.Context, string) ([]store.KubernetesCluster, error) {
	return nil, nil
}

func (r *fakeKubernetesClusterRepository) GetKubernetesCluster(context.Context, string, string) (store.KubernetesCluster, error) {
	r.getCalls++
	return r.current, nil
}

func (r *fakeKubernetesClusterRepository) CreateKubernetesCluster(_ context.Context, cluster store.KubernetesCluster) (store.KubernetesCluster, error) {
	return cluster, nil
}

func (r *fakeKubernetesClusterRepository) UpdateKubernetesCluster(_ context.Context, cluster store.KubernetesCluster) (store.KubernetesCluster, error) {
	r.updateCalls++
	r.updated = cluster
	return cluster, nil
}

func (r *fakeKubernetesClusterRepository) DeleteKubernetesCluster(context.Context, string, string, int64) error {
	return nil
}

func TestKubernetesClusterService_UpdatePreservesCredentials(t *testing.T) {
	t.Parallel()
	credentials := store.KubernetesCredentials{Token: "secret"}
	repository := &fakeKubernetesClusterRepository{current: store.KubernetesCluster{AuthMode: "token", Credentials: credentials}}
	service := NewKubernetesClusterService(repository, repository)

	_, err := service.Update(t.Context(), KubernetesClusterInput{Provided: true, Cluster: store.KubernetesCluster{ID: "cluster", TenantID: "tenant", AuthMode: "token"}})
	if err != nil {
		t.Fatal(err)
	}
	if repository.getCalls != 1 || repository.updateCalls != 1 || repository.updated.Credentials != credentials {
		t.Fatalf("get calls = %d, update calls = %d, credentials = %+v", repository.getCalls, repository.updateCalls, repository.updated.Credentials)
	}
}

func TestKubernetesClusterService_RejectsAuthModeChangeWithoutCredentials(t *testing.T) {
	t.Parallel()
	repository := &fakeKubernetesClusterRepository{current: store.KubernetesCluster{AuthMode: "token", Credentials: store.KubernetesCredentials{Token: "secret"}}}
	service := NewKubernetesClusterService(repository, repository)

	_, err := service.Update(t.Context(), KubernetesClusterInput{Provided: true, Cluster: store.KubernetesCluster{ID: "cluster", TenantID: "tenant", AuthMode: "kubeconfig"}})
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) || repository.updateCalls != 0 {
		t.Fatalf("error = %v, update calls = %d", err, repository.updateCalls)
	}
}

var _ KubernetesClusterReader = (*fakeKubernetesClusterRepository)(nil)
var _ KubernetesClusterWriter = (*fakeKubernetesClusterRepository)(nil)
