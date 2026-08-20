package core

import (
	"context"
	"errors"
	"testing"

	"github.com/lihongjie0209/go-scheduler/internal/store"
)

type fakeJobRepository struct {
	current         store.Job
	created         store.Job
	updated         store.Job
	createErr       error
	getErr          error
	updateErr       error
	createCalls     int
	getCalls        int
	updateCalls     int
	deleteCalls     int
	enableCalls     int
	labelsCalls     int
	triggerCalls    int
	dependencyCalls int
	triggerOptions  store.TriggerOptions
	dependencyIDs   []string
}

func (r *fakeJobRepository) CreateJob(_ context.Context, job store.Job) (store.Job, error) {
	r.createCalls++
	r.created = job
	return job, r.createErr
}

func (r *fakeJobRepository) GetJob(_ context.Context, _, _ string) (store.Job, error) {
	r.getCalls++
	return r.current, r.getErr
}

func (r *fakeJobRepository) UpdateJob(_ context.Context, job store.Job) (store.Job, error) {
	r.updateCalls++
	r.updated = job
	return job, r.updateErr
}

func (r *fakeJobRepository) ListJobs(context.Context, string, int) ([]store.Job, error) {
	return nil, nil
}

func (r *fakeJobRepository) JobExecutorLabels(context.Context, string) ([]string, []string, error) {
	r.labelsCalls++
	return []string{"required"}, []string{"excluded"}, nil
}

func (r *fakeJobRepository) SetJobEnabled(_ context.Context, _, _ string, _ bool, _ int64) (store.Job, error) {
	r.enableCalls++
	return r.current, nil
}

func (r *fakeJobRepository) DeleteJob(context.Context, string, string, int64) error {
	r.deleteCalls++
	return nil
}

func (r *fakeJobRepository) ListJobScriptVersions(context.Context, string, string) ([]store.JobScriptVersion, error) {
	return nil, nil
}

func (r *fakeJobRepository) RollbackJobScriptVersion(context.Context, string, string, string, int64, string) (store.Job, error) {
	return r.current, nil
}

func (r *fakeJobRepository) TriggerJobWithOptions(_ context.Context, _, _, _, _ string, options store.TriggerOptions) (store.Run, error) {
	r.triggerCalls++
	r.triggerOptions = options
	return store.Run{}, nil
}

func (r *fakeJobRepository) SetJobDependencies(_ context.Context, _, _ string, ids []string) error {
	r.dependencyCalls++
	r.dependencyIDs = append([]string(nil), ids...)
	return nil
}

func (r *fakeJobRepository) JobDependencies(context.Context, string, string) ([]string, error) {
	return nil, nil
}

func TestJobService_Create(t *testing.T) {
	t.Parallel()

	t.Run("normalizes labels before persistence", func(t *testing.T) {
		t.Parallel()
		repository := &fakeJobRepository{}
		service := NewJobService(repository, repository, repository, repository, repository)
		job := fromProto(validJob())
		job.RequiredExecutorLabels = []string{" GPU ", "linux", "gpu"}

		if _, err := service.Create(t.Context(), CreateJobInput{Job: job}); err != nil {
			t.Fatal(err)
		}
		if repository.createCalls != 1 || len(repository.created.RequiredExecutorLabels) != 2 || repository.created.RequiredExecutorLabels[0] != "gpu" {
			t.Fatalf("persisted job = %+v", repository.created)
		}
	})

	t.Run("rejects invalid job before persistence", func(t *testing.T) {
		t.Parallel()
		repository := &fakeJobRepository{}
		service := NewJobService(repository, repository, repository, repository, repository)
		job := fromProto(validJob())
		job.Name = ""

		_, err := service.Create(t.Context(), CreateJobInput{Job: job})
		var validationErr *ValidationError
		if !errors.As(err, &validationErr) || repository.createCalls != 0 {
			t.Fatalf("error = %v, create calls = %d", err, repository.createCalls)
		}
	})

	t.Run("rejects unconfigured credentials", func(t *testing.T) {
		t.Parallel()
		repository := &fakeJobRepository{}
		service := NewJobService(repository, repository, repository, repository, repository)
		job := dockerJob()

		_, err := service.Create(t.Context(), CreateJobInput{Job: job, DockerRegistryAuthProvided: true})
		var validationErr *ValidationError
		if !errors.As(err, &validationErr) || repository.createCalls != 0 {
			t.Fatalf("error = %v, create calls = %d", err, repository.createCalls)
		}
	})
}

func TestJobService_UpdateDockerRegistryAuth(t *testing.T) {
	t.Parallel()
	existingAuth := store.DockerRegistryAuth{Server: "registry.example.com", Username: "robot", Password: "secret", Configured: true}

	tests := []struct {
		name           string
		input          UpdateJobInput
		expectedAuth   store.DockerRegistryAuth
		expectedGets   int
		wantValidation bool
	}{
		{
			name:         "preserves omitted credentials",
			input:        UpdateJobInput{Job: dockerJob()},
			expectedAuth: existingAuth,
			expectedGets: 1,
		},
		{
			name:         "clears credentials explicitly",
			input:        UpdateJobInput{Job: dockerJob(), ClearDockerRegistryAuth: true},
			expectedAuth: store.DockerRegistryAuth{},
		},
		{
			name: "rejects identity change without password",
			input: UpdateJobInput{Job: func() store.Job {
				job := dockerJob()
				job.DockerRegistryAuth = store.DockerRegistryAuth{Server: "other.example.com", Username: "robot", Configured: true}
				return job
			}(), DockerRegistryAuthProvided: true},
			expectedGets:   1,
			wantValidation: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			repository := &fakeJobRepository{current: store.Job{DockerRegistryAuth: existingAuth}}
			service := NewJobService(repository, repository, repository, repository, repository)

			_, err := service.Update(t.Context(), tt.input)
			var validationErr *ValidationError
			if errors.As(err, &validationErr) != tt.wantValidation {
				t.Fatalf("error = %v, want validation = %v", err, tt.wantValidation)
			}
			if repository.getCalls != tt.expectedGets {
				t.Fatalf("get calls = %d, want %d", repository.getCalls, tt.expectedGets)
			}
			if !tt.wantValidation && repository.updated.DockerRegistryAuth != tt.expectedAuth {
				t.Fatalf("auth = %+v, want %+v", repository.updated.DockerRegistryAuth, tt.expectedAuth)
			}
		})
	}
}

func TestJobService_GetAttachesExecutorLabels(t *testing.T) {
	t.Parallel()
	repository := &fakeJobRepository{current: store.Job{ID: "job-id", TenantID: "tenant"}}
	service := NewJobService(repository, repository, repository, repository, repository)

	job, err := service.Get(t.Context(), "tenant", "job-id")
	if err != nil {
		t.Fatal(err)
	}
	if repository.getCalls != 1 || repository.labelsCalls != 1 {
		t.Fatalf("get calls = %d, label calls = %d", repository.getCalls, repository.labelsCalls)
	}
	if len(job.RequiredExecutorLabels) != 1 || job.RequiredExecutorLabels[0] != "required" || len(job.ExcludedExecutorLabels) != 1 || job.ExcludedExecutorLabels[0] != "excluded" {
		t.Fatalf("job labels = required:%v excluded:%v", job.RequiredExecutorLabels, job.ExcludedExecutorLabels)
	}
}

func TestJobService_RejectsInvalidLifecycleInputs(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		call func(context.Context, *JobService) error
	}{
		{name: "set enabled", call: func(ctx context.Context, service *JobService) error {
			_, err := service.SetEnabled(ctx, "", "job", true, 1)
			return err
		}},
		{name: "list script versions", call: func(ctx context.Context, service *JobService) error {
			_, err := service.ListScriptVersions(ctx, "tenant", "")
			return err
		}},
		{name: "rollback script version", call: func(ctx context.Context, service *JobService) error {
			_, err := service.RollbackScriptVersion(ctx, "tenant", "job", "", 1, "")
			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			repository := &fakeJobRepository{}
			service := NewJobService(repository, repository, repository, repository, repository)
			var validationErr *ValidationError
			if err := tt.call(t.Context(), service); !errors.As(err, &validationErr) {
				t.Fatalf("error = %v", err)
			}
			if repository.enableCalls != 0 {
				t.Fatalf("enable calls = %d", repository.enableCalls)
			}
		})
	}
}

func TestJobService_TriggerNormalizesOverrides(t *testing.T) {
	t.Parallel()
	repository := &fakeJobRepository{}
	service := NewJobService(repository, repository, repository, repository, repository)

	_, err := service.Trigger(t.Context(), TriggerJobInput{
		TenantID:          "tenant",
		JobID:             "job",
		OverrideAddresses: []string{" https://worker-b:9999/ ", "http://worker-a:9999", "http://worker-a:9999/"},
	})
	if err != nil {
		t.Fatal(err)
	}
	addresses := repository.triggerOptions.OverrideAddresses
	if repository.triggerCalls != 1 || len(addresses) != 2 || addresses[0] != "http://worker-a:9999" || addresses[1] != "https://worker-b:9999" {
		t.Fatalf("trigger calls = %d, addresses = %v", repository.triggerCalls, addresses)
	}
}

func TestJobService_SetDependenciesNormalizesIDs(t *testing.T) {
	t.Parallel()
	repository := &fakeJobRepository{}
	service := NewJobService(repository, repository, repository, repository, repository)

	ids, err := service.SetDependencies(t.Context(), "tenant", "parent", []string{" child-b ", "child-a", "child-a"})
	if err != nil {
		t.Fatal(err)
	}
	if repository.dependencyCalls != 1 || len(ids) != 2 || ids[0] != "child-a" || ids[1] != "child-b" {
		t.Fatalf("dependency calls = %d, ids = %v", repository.dependencyCalls, ids)
	}
	if len(repository.dependencyIDs) != 2 || repository.dependencyIDs[0] != "child-a" {
		t.Fatalf("persisted ids = %v", repository.dependencyIDs)
	}
}

func TestJobService_RejectsSelfDependency(t *testing.T) {
	t.Parallel()
	repository := &fakeJobRepository{}
	service := NewJobService(repository, repository, repository, repository, repository)

	_, err := service.SetDependencies(t.Context(), "tenant", "job", []string{"job"})
	var validationErr *ValidationError
	if !errors.As(err, &validationErr) || repository.dependencyCalls != 0 {
		t.Fatalf("error = %v, dependency calls = %d", err, repository.dependencyCalls)
	}
}

func dockerJob() store.Job {
	job := fromProto(validJob())
	job.TargetURL = ""
	job.ExecutorHandler = "__docker__"
	job.ScriptLanguage = "docker"
	job.ScriptSource = `{"image":"alpine:3.22"}`
	return job
}

var _ JobReader = (*fakeJobRepository)(nil)
var _ JobWriter = (*fakeJobRepository)(nil)
var _ JobScriptRepository = (*fakeJobRepository)(nil)
var _ JobTriggerer = (*fakeJobRepository)(nil)
var _ JobDependencyRepository = (*fakeJobRepository)(nil)
