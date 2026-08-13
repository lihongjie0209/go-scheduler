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
	client := fake.NewSimpleClientset(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "work"}, Status: batchv1.JobStatus{Succeeded: 1, Conditions: []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}}})
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

func TestKubernetesHandlerCreatesRecoverableJob(t *testing.T) {
	client := fake.NewSimpleClientset()
	handler := KubernetesHandler(KubernetesOptions{PollInterval: time.Millisecond, ClientFactory: func(*rest.Config) (kubernetes.Interface, error) { return client, nil }})
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	err := handler(ctx, Task{RunID: "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee", ScriptLanguage: "kubernetes", ScriptSource: `{"image":"alpine:3.22","image_pull_secrets":["registry"]}`, KubernetesCluster: &KubernetesClusterConfig{AuthMode: "service_account", APIServer: "https://k8s.example", Token: "token", Namespace: "work"}})
	if err == nil {
		t.Fatal("expected context deadline while fake job remains active")
	}
	job, getErr := client.BatchV1().Jobs("work").Get(t.Context(), "scheduler-aaaaaaaabbbbccccddddeeeeeeeeeeee", metav1.GetOptions{})
	if getErr != nil {
		t.Fatal(getErr)
	}
	if job.Spec.TTLSecondsAfterFinished == nil || *job.Spec.TTLSecondsAfterFinished != 86400 || len(job.Spec.Template.Spec.ImagePullSecrets) != 1 {
		t.Fatalf("job is not recoverable or pull secret missing: %+v", job.Spec)
	}
}
