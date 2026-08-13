package api

import (
	"os"
	"testing"

	"go.yaml.in/yaml/v3"
)

func TestOpenAPIDocumentIsValidYAML(t *testing.T) {
	t.Parallel()
	raw, err := os.ReadFile("../../api/openapi.yaml") // #nosec G304 -- fixed repository test fixture.
	if err != nil {
		t.Fatal(err)
	}
	var document yaml.Node
	if err = yaml.Unmarshal(raw, &document); err != nil {
		t.Fatalf("parse OpenAPI document: %v", err)
	}
	if len(document.Content) != 1 || document.Content[0].Kind != yaml.MappingNode {
		t.Fatal("OpenAPI document root must be a mapping")
	}
}
