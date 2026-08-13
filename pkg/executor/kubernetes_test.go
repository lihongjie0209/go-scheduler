package executor

import (
	"context"
	"strings"
	"testing"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/fake"
	"k8s.io/client-go/rest"
)

func TestKubernetesRESTConfigServiceAccount(t *testing.T) {
	config, err := kubernetesRESTConfig(KubernetesClusterConfig{AuthMode: "service_account", APIServer: "https://k8s.example", Token: "secret", CAData: "ca"})
	if err != nil {
		t.Fatal(err)
	}
	if config.Host != "https://k8s.example" || config.BearerToken != "secret" || string(config.CAData) != "ca" {
		t.Fatalf("unexpected config: %+v", config)
	}
}

func TestKubernetesHandlerResumesExistingJob(t *testing.T) {
	executionID := "11111111-2222-3333-4444-555555555555"
	name := "scheduler-" + strings.ReplaceAll(executionID, "-", "")
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "work", Labels: map[string]string{kubernetesManagedByLabel: kubernetesManagedByValue, kubernetesExecutionIDLabel: executionID}}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}}})
	handler := KubernetesHandler(KubernetesOptions{PollInterval: time.Millisecond, ClientFactory: func(*rest.Config) (kubernetes.Interface, error) { return client, nil }})
	err := handler(t.Context(), Task{RunID: "retry-run", ExternalExecutionID: executionID, ScriptLanguage: "kubernetes", ScriptSource: `{"image":"alpine:3.22"}`, KubernetesCluster: &KubernetesClusterConfig{AuthMode: "service_account", APIServer: "https://k8s.example", Token: "token", Namespace: "work"}})
	if err != nil {
		t.Fatal(err)
	}
	actions := client.Actions()
	for _, action := range actions {
		if action.GetVerb() == "create" {
			t.Fatalf("existing external execution was recreated: %+v", actions)
		}
	}
}

func TestKubernetesHandlerRejectsJobNameCollision(t *testing.T) {
	t.Parallel()
	executionID := "11111111-2222-3333-4444-555555555555"
	name := "scheduler-" + strings.ReplaceAll(executionID, "-", "")
	tests := []struct {
		name   string
		labels map[string]string
	}{
		{name: "unmanaged job", labels: nil},
		{name: "different execution", labels: map[string]string{kubernetesManagedByLabel: kubernetesManagedByValue, kubernetesExecutionIDLabel: "different"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "work", Labels: tt.labels}})
			handler := KubernetesHandler(KubernetesOptions{PollInterval: time.Millisecond, ClientFactory: func(*rest.Config) (kubernetes.Interface, error) { return client, nil }})
			err := handler(t.Context(), Task{RunID: executionID, ExternalExecutionID: executionID, ScriptLanguage: "kubernetes", ScriptSource: `{"image":"alpine:3.22"}`, KubernetesCluster: &KubernetesClusterConfig{AuthMode: "service_account", APIServer: "https://k8s.example", Token: "token", Namespace: "work"}})
			if err == nil || !strings.Contains(err.Error(), "name collision") {
				t.Fatalf("error = %v, want name collision", err)
			}
		})
	}
}

func TestKubernetesHandlerDeletesJobAfterContextCancellation(t *testing.T) {
	client := fake.NewSimpleClientset()
	handler := KubernetesHandler(KubernetesOptions{PollInterval: time.Millisecond, ClientFactory: func(*rest.Config) (kubernetes.Interface, error) { return client, nil }})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err := handler(ctx, Task{RunID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", ScriptLanguage: "kubernetes", ScriptSource: `{"image":"alpine:3.22","image_pull_secrets":["registry"]}`, KubernetesCluster: &KubernetesClusterConfig{AuthMode: "service_account", APIServer: "https://k8s.example", Token: "token", Namespace: "work"}})
	if err == nil {
		t.Fatal("expected context deadline while fake job remains active")
	}
	if _, getErr := client.BatchV1().Jobs("work").Get(t.Context(), "scheduler-aaaaaaaabbbbccccddddeeeeeeeeeeee", metav1.GetOptions{}); getErr == nil {
		t.Fatal("Kubernetes Job remained after execution context cancellation")
	}
}

func TestKubernetesCancellerRecoversAfterExecutorRestart(t *testing.T) {
	executionID := "11111111-2222-3333-4444-555555555555"
	jobID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	name := kubernetesJobName(executionID)
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "work", Labels: map[string]string{kubernetesManagedByLabel: kubernetesManagedByValue, kubernetesExecutionIDLabel: executionID, kubernetesJobIDLabel: jobID}}})
	canceller := KubernetesCanceller(KubernetesOptions{ClientFactory: func(*rest.Config) (kubernetes.Interface, error) { return client, nil }})
	cancellation := ExternalCancellation{RunID: "retry-run", ExternalExecutionID: executionID, JobID: jobID, ScriptLanguage: "kubernetes", KubernetesCluster: &KubernetesClusterConfig{AuthMode: "service_account", APIServer: "https://k8s.example", Token: "token", Namespace: "work"}}
	if err := canceller(t.Context(), cancellation); err != nil {
		t.Fatal(err)
	}
	if _, err := client.BatchV1().Jobs("work").Get(t.Context(), name, metav1.GetOptions{}); err == nil {
		t.Fatal("managed Kubernetes Job was not deleted")
	}
	if err := canceller(t.Context(), cancellation); err != nil {
		t.Fatalf("repeated cancellation was not idempotent: %v", err)
	}
}

func TestKubernetesCancellerRejectsDifferentJobIdentity(t *testing.T) {
	executionID := "11111111-2222-3333-4444-555555555555"
	name := kubernetesJobName(executionID)
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "work", Labels: map[string]string{kubernetesManagedByLabel: kubernetesManagedByValue, kubernetesExecutionIDLabel: executionID, kubernetesJobIDLabel: "different-job"}}})
	canceller := KubernetesCanceller(KubernetesOptions{ClientFactory: func(*rest.Config) (kubernetes.Interface, error) { return client, nil }})
	err := canceller(t.Context(), ExternalCancellation{RunID: "run", ExternalExecutionID: executionID, JobID: "expected-job", KubernetesCluster: &KubernetesClusterConfig{AuthMode: "service_account", APIServer: "https://k8s.example", Token: "token", Namespace: "work"}})
	if err == nil || !strings.Contains(err.Error(), "different scheduler job") {
		t.Fatalf("cancellation error = %v", err)
	}
	if _, getErr := client.BatchV1().Jobs("work").Get(t.Context(), name, metav1.GetOptions{}); getErr != nil {
		t.Fatalf("mismatched Kubernetes Job was deleted: %v", getErr)
	}
}
