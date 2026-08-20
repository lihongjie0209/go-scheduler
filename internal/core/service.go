package core

import (
	schedulerv1 "github.com/lihongjie0209/go-scheduler/gen/scheduler/v1"
)

type ServiceRepository interface {
	JobReader
	JobWriter
	JobScriptRepository
	JobTriggerer
	JobDependencyRepository
	RunReader
	RunWriter
	ExecutorGroupRepository
	KubernetesClusterReader
	KubernetesClusterWriter
	HealthChecker
	UserReader
	UserWriter
	AccessReader
	RefreshSessionRepository
	APIKeyAuthenticator
	TenantReader
	TenantWriter
	APIKeyRepository
	RunLogRepository
	RunHistoryRepository
	RunReportRepository
	NotificationChannelReader
	NotificationChannelWriter
	NotificationHistoryReader
}

type Service struct {
	schedulerv1.UnimplementedSchedulerServiceServer
	jobs             *JobService
	runs             *RunService
	executors        *ExecutorService
	clusters         *KubernetesClusterService
	identity         *IdentityService
	tenancy          *TenancyService
	operations       *OperationsService
	notifications    *NotificationService
	executorRegistry ExecutorRegistry
	executorControl  *ExecutorController
	onRunTerminal    func()
}

func NewService(repository ServiceRepository, registries ...ExecutorRegistry) *Service {
	registry, _ := repository.(ExecutorRegistry)
	if len(registries) > 0 && registries[0] != nil {
		registry = registries[0]
	}
	service := &Service{jobs: NewJobService(repository, repository, repository, repository, repository), executorRegistry: registry}
	service.runs = NewRunService(repository, repository, nil, service.notifyRunTerminal)
	service.executors = NewExecutorService(repository, registry)
	service.clusters = NewKubernetesClusterService(repository, repository)
	service.identity = NewIdentityService(repository, repository, repository, repository, repository, repository)
	service.tenancy = NewTenancyService(repository, repository, repository)
	service.operations = NewOperationsService(repository, repository, repository)
	service.notifications = NewNotificationService(repository, repository, repository)
	return service
}

func NewServiceWithExecutorController(repository ServiceRepository, registry ExecutorRegistry, controller *ExecutorController) *Service {
	service := NewService(repository, registry)
	service.executorControl = controller
	service.runs.executor = controller
	return service
}

func (s *Service) SetOnRunTerminal(fn func()) {
	s.onRunTerminal = fn
}

func (s *Service) notifyRunTerminal() {
	if s.onRunTerminal != nil {
		s.onRunTerminal()
	}
}

var _ schedulerv1.SchedulerServiceServer = (*Service)(nil)
