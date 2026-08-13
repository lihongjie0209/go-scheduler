package migratecmd

import "testing"

func TestMigrationURL(t *testing.T) {
	t.Parallel()
	got, err := migrationURL("postgres://user:pass@localhost/db?sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	if got != "pgx5://user:pass@localhost/db?sslmode=disable" {
		t.Fatalf("got %s", got)
	}
}
