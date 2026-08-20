package core

import (
	"errors"
	"fmt"

	"github.com/lihongjie0209/go-scheduler/internal/store"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func toStatus(err error) error {
	var validationErr *ValidationError
	switch {
	case errors.As(err, &validationErr):
		return status.Error(codes.InvalidArgument, validationErr.Error())
	case errors.Is(err, store.ErrNotFound):
		return status.Error(codes.NotFound, "resource not found")
	case errors.Is(err, store.ErrConflict):
		return status.Error(codes.Aborted, "resource version conflict")
	case errors.Is(err, store.ErrQueueFull):
		return status.Error(codes.ResourceExhausted, "job queue is full")
	case errors.Is(err, store.ErrNotCancellable):
		return status.Error(codes.FailedPrecondition, "run is already terminal")
	case errors.Is(err, store.ErrDependencyCycle):
		return status.Error(codes.FailedPrecondition, "job dependency would create a cycle")
	case errors.Is(err, store.ErrRegistrationMode):
		return status.Error(codes.FailedPrecondition, "executor group uses manual registration")
	case errors.Is(err, store.ErrExecutorGroupInUse):
		return status.Error(codes.FailedPrecondition, "executor group is still referenced by a job")
	case errors.Is(err, store.ErrOverrideRequiresExecutorGroup):
		return status.Error(codes.FailedPrecondition, "executor address override requires an executor group job")
	case errors.Is(err, store.ErrOverrideAddressNotRegistered):
		return status.Error(codes.InvalidArgument, err.Error())
	case errors.Is(err, store.ErrInvalidNotificationScope):
		return status.Error(codes.InvalidArgument, "notification channel must target all jobs or one or more specific jobs")
	case errors.Is(err, store.ErrKubernetesClusterInUse):
		return status.Error(codes.FailedPrecondition, "kubernetes cluster is referenced by a job")
	default:
		return status.Error(codes.Internal, fmt.Sprintf("operation failed: %v", err))
	}
}
