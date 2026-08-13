package executor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

type KubernetesClusterConfig struct {
	AuthMode              string `json:"auth_mode"`
	APIServer             string `json:"api_server,omitempty"`
	Namespace             string `json:"namespace"`
	Kubeconfig            string `json:"kubeconfig,omitempty"`
	Token                 string `json:"token,omitempty"`
	CAData                string `json:"ca_data,omitempty"`
	InsecureSkipTLSVerify bool   `json:"insecure_skip_tls_verify,omitempty"`
}

type KubernetesJobSpec struct {
	Image                   string            `json:"image"`
	Command                 []string          `json:"command,omitempty"`
	Args                    []string          `json:"args,omitempty"`
	Env                     map[string]string `json:"env,omitempty"`
	ImagePullSecrets        []string          `json:"image_pull_secrets,omitempty"`
	ServiceAccount          string            `json:"service_account,omitempty"`
	BackoffLimit            int32             `json:"backoff_limit,omitempty"`
	TTLSecondsAfterFinished int32             `json:"ttl_seconds_after_finished,omitempty"`
}

type KubernetesOptions struct {
	ClientFactory func(*rest.Config) (kubernetes.Interface, error)
	PollInterval  time.Duration
}

const (
	kubernetesManagedByLabel   = "app.kubernetes.io/managed-by"
	kubernetesManagedByValue   = "go-scheduler"
	kubernetesExecutionIDLabel = "go-scheduler/execution-id"
)

func KubernetesHandler(options KubernetesOptions) Handler {
	if options.ClientFactory == nil {
		options.ClientFactory = func(config *rest.Config) (kubernetes.Interface, error) { return kubernetes.NewForConfig(config) }
	}
	if options.PollInterval <= 0 {
		options.PollInterval = time.Second
	}
	return func(ctx context.Context, task Task) error {
		if task.ScriptLanguage != "kubernetes" || task.KubernetesCluster == nil {
			return errors.New("kubernetes task and cluster configuration are required")
		}
		var definition KubernetesJobSpec
		if err := json.Unmarshal([]byte(task.ScriptSource), &definition); err != nil {
			return fmt.Errorf("decode kubernetes job definition: %w", err)
		}
		if strings.TrimSpace(definition.Image) == "" || len(definition.Image) > 512 {
			return errors.New("kubernetes job image is required and must not exceed 512 characters")
		}
		config, err := kubernetesRESTConfig(*task.KubernetesCluster)
		if err != nil {
			return err
		}
		client, err := options.ClientFactory(config)
		if err != nil {
			return fmt.Errorf("create kubernetes client: %w", err)
		}
		namespace := strings.TrimSpace(task.KubernetesCluster.Namespace)
		if namespace == "" {
			namespace = "default"
		}
		executionID := task.ExternalExecutionID
		if executionID == "" {
			executionID = task.RunID
		}
		name := "scheduler-" + strings.ToLower(strings.ReplaceAll(executionID, "-", ""))
		if len(name) > 63 {
			name = name[:63]
		}
		env := make([]corev1.EnvVar, 0, len(definition.Env)+1)
		for key, value := range definition.Env {
			env = append(env, corev1.EnvVar{Name: key, Value: value})
		}
		env = append(env, corev1.EnvVar{Name: "SCHEDULER_INPUT", Value: task.Input})
		pullSecrets := make([]corev1.LocalObjectReference, 0, len(definition.ImagePullSecrets))
		for _, secret := range definition.ImagePullSecrets {
			if secret = strings.TrimSpace(secret); secret != "" {
				pullSecrets = append(pullSecrets, corev1.LocalObjectReference{Name: secret})
			}
		}
		backoff := definition.BackoffLimit
		ttl := definition.TTLSecondsAfterFinished
		if ttl <= 0 {
			ttl = 86400
		}
		job := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Labels: map[string]string{kubernetesManagedByLabel: kubernetesManagedByValue, kubernetesExecutionIDLabel: executionID}}, Spec: batchv1.JobSpec{BackoffLimit: &backoff, TTLSecondsAfterFinished: &ttl, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"job-name": name, kubernetesExecutionIDLabel: executionID}}, Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, ServiceAccountName: definition.ServiceAccount, ImagePullSecrets: pullSecrets, Containers: []corev1.Container{{Name: "task", Image: definition.Image, Command: definition.Command, Args: definition.Args, Env: env}}}}}}
		existing, getErr := client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
		if apierrors.IsNotFound(getErr) {
			if _, err = client.BatchV1().Jobs(namespace).Create(ctx, job, metav1.CreateOptions{}); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					return fmt.Errorf("create kubernetes job: %w", err)
				}
				existing, getErr = client.BatchV1().Jobs(namespace).Get(ctx, name, metav1.GetOptions{})
				if getErr != nil {
					return fmt.Errorf("get concurrently created kubernetes job: %w", getErr)
				}
				if err = validateManagedKubernetesJob(existing, executionID); err != nil {
					return err
				}
			}
		} else if getErr != nil {
			return fmt.Errorf("get kubernetes job: %w", getErr)
		} else if err = validateManagedKubernetesJob(existing, executionID); err != nil {
			return err
		}
		err = wait.PollUntilContextCancel(ctx, options.PollInterval, true, func(pollCtx context.Context) (bool, error) {
			current, getErr := client.BatchV1().Jobs(namespace).Get(pollCtx, name, metav1.GetOptions{})
			if getErr != nil {
				return false, getErr
			}
			if validateErr := validateManagedKubernetesJob(current, executionID); validateErr != nil {
				return false, validateErr
			}
			for _, condition := range current.Status.Conditions {
				if condition.Status != corev1.ConditionTrue {
					continue
				}
				switch condition.Type {
				case batchv1.JobComplete:
					return true, nil
				case batchv1.JobFailed:
					return false, fmt.Errorf("kubernetes job failed: %s", condition.Message)
				}
			}
			return false, nil
		})
		logErr := writeKubernetesPodLogs(ctx, client, namespace, name, task.Logger)
		if err != nil {
			return err
		}
		return logErr
	}
}

func validateManagedKubernetesJob(job *batchv1.Job, executionID string) error {
	if job == nil || job.Labels[kubernetesManagedByLabel] != kubernetesManagedByValue || job.Labels[kubernetesExecutionIDLabel] != executionID {
		return fmt.Errorf("kubernetes job name collision for execution %q", executionID)
	}
	return nil
}

func kubernetesRESTConfig(cluster KubernetesClusterConfig) (*rest.Config, error) {
	if cluster.AuthMode == "kubeconfig" {
		config, err := clientcmd.RESTConfigFromKubeConfig([]byte(cluster.Kubeconfig))
		if err != nil {
			return nil, fmt.Errorf("parse kubeconfig: %w", err)
		}
		return config, nil
	}
	if cluster.AuthMode != "service_account" || strings.TrimSpace(cluster.APIServer) == "" || strings.TrimSpace(cluster.Token) == "" {
		return nil, errors.New("invalid service account cluster configuration")
	}
	return &rest.Config{Host: cluster.APIServer, BearerToken: cluster.Token, TLSClientConfig: rest.TLSClientConfig{CAData: []byte(cluster.CAData), Insecure: cluster.InsecureSkipTLSVerify}}, nil
}

func writeKubernetesPodLogs(ctx context.Context, client kubernetes.Interface, namespace, jobName string, logger TaskLogger) error {
	if logger == nil {
		return nil
	}
	pods, err := client.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{FieldSelector: fields.Everything().String(), LabelSelector: "job-name=" + jobName})
	if err != nil || len(pods.Items) == 0 {
		return err
	}
	stream, err := client.CoreV1().Pods(namespace).GetLogs(pods.Items[0].Name, &corev1.PodLogOptions{Container: "task"}).Stream(ctx)
	if err != nil {
		return fmt.Errorf("open kubernetes pod logs: %w", err)
	}
	defer func() { _ = stream.Close() }()
	raw, err := io.ReadAll(io.LimitReader(stream, 1<<20))
	if err != nil {
		return err
	}
	if len(raw) > 0 {
		return logger.Info(string(raw))
	}
	return nil
}
