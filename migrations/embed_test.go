package migrations

import (
	"strconv"
	"strings"
	"testing"
)

func TestAllContainsEveryUpMigration(t *testing.T) {
	t.Parallel()
	entries, err := FS.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	versions := make(map[int64]string)
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".up.sql") {
			continue
		}
		prefix, _, ok := strings.Cut(name, "_")
		if !ok {
			t.Fatalf("migration filename %q has no version prefix", name)
		}
		version, parseErr := strconv.ParseInt(prefix, 10, 64)
		if parseErr != nil {
			t.Fatalf("migration filename %q: %v", name, parseErr)
		}
		versions[version] = name
	}
	if len(All) != len(versions) {
		t.Fatalf("embedded migration registry has %d entries, filesystem has %d", len(All), len(versions))
	}
	for index, migration := range All {
		wantVersion := int64(index + 1)
		if migration.Version != wantVersion {
			t.Fatalf("All[%d].Version = %d, want %d", index, migration.Version, wantVersion)
		}
		if _, exists := versions[migration.Version]; !exists {
			t.Fatalf("registered migration version %d has no .up.sql file", migration.Version)
		}
		if strings.TrimSpace(migration.SQL) == "" {
			t.Fatalf("registered migration version %d is empty", migration.Version)
		}
	}
}
