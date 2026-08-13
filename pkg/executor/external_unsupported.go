//go:build !unix

package executor

import (
	"context"
	"errors"
)

type ScriptOptions struct{ Languages []string }

func ScriptHandler(ScriptOptions) Handler {
	return func(context.Context, Task) error {
		return errors.New("script execution is not supported on this operating system")
	}
}

type DockerOptions struct {
	Binary string
}

func DockerHandler(DockerOptions) Handler {
	return func(context.Context, Task) error {
		return errors.New("Docker execution is not supported on this operating system")
	}
}

func DockerCanceller(DockerOptions) ExternalCanceller {
	return func(context.Context, ExternalCancellation) error {
		return errors.New("Docker cancellation is not supported on this operating system")
	}
}
