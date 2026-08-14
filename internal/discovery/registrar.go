package discovery

import "context"

// ServiceRegistrar owns the lifecycle of a service-discovery registration.
// Kubernetes does not need an application-level registration because the
// Service controller publishes ready Pods through EndpointSlices and DNS.
type ServiceRegistrar interface {
	Run(context.Context) error
	Close(context.Context) error
}

type NoopRegistrar struct{}

func NewNoopRegistrar() *NoopRegistrar { return &NoopRegistrar{} }

func (*NoopRegistrar) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

func (*NoopRegistrar) Close(context.Context) error { return nil }

var _ ServiceRegistrar = (*Registrar)(nil)
var _ ServiceRegistrar = (*NoopRegistrar)(nil)
