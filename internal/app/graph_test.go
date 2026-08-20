package app_test

import (
	"testing"

	"go.uber.org/fx"

	"github.com/lihongjie0209/go-scheduler/internal/app/apiserver"
	"github.com/lihongjie0209/go-scheduler/internal/app/schedulercore"
	"github.com/lihongjie0209/go-scheduler/internal/app/schedulerserver"
)

func TestDependencyGraphs(t *testing.T) {
	t.Parallel()
	tests := map[string][]fx.Option{
		"api server":        apiserver.Options(),
		"scheduler core":    schedulercore.Options(),
		"standalone server": schedulerserver.Options(),
	}
	for name, options := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if err := fx.ValidateApp(options...); err != nil {
				t.Fatal(err)
			}
		})
	}
}
