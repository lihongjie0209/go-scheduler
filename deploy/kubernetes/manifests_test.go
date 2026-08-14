package kubernetes

import (
	"os"
	"strings"
	"testing"
)

func TestWorkloadsUseNumericNonRootIdentity(t *testing.T) {
	tests := []struct {
		name string
		file string
	}{
		{name: "API server", file: "api-server.yaml"},
		{name: "scheduler core", file: "scheduler-core.yaml"},
		{name: "executor", file: "script-executor.yaml"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			content, err := os.ReadFile(test.file)
			if err != nil {
				t.Fatal(err)
			}
			manifest := string(content)
			for _, required := range []string{"runAsNonRoot: true", "runAsUser: 65532", "runAsGroup: 65532"} {
				if !strings.Contains(manifest, required) {
					t.Fatalf("%s does not contain %q", test.file, required)
				}
			}
		})
	}
}
