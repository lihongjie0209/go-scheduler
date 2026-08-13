package migratecmd

import (
	"fmt"
	"net/url"
	"os"

	"errors"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/lihongjie0209/go-scheduler/migrations"
)

func Run() {
	databaseURL := os.Getenv("DATABASE_URL")
	if databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	driverURL, err := migrationURL(databaseURL)
	if err != nil {
		fatal(err.Error())
	}
	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		fatal(err.Error())
	}
	runner, err := migrate.NewWithSourceInstance("iofs", source, driverURL)
	if err != nil {
		fatal(err.Error())
	}
	defer func() { _, _ = runner.Close() }()
	if err = runner.Up(); err != nil && !errors.Is(err, migrate.ErrNoChange) {
		fatal(err.Error())
	}
	fmt.Println("migrations applied")
}
func migrationURL(raw string) (string, error) {
	parsed, err := url.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	if parsed.Scheme != "postgres" && parsed.Scheme != "postgresql" && parsed.Scheme != "pgx5" {
		return "", fmt.Errorf("DATABASE_URL must use postgres, postgresql, or pgx5 scheme")
	}
	parsed.Scheme = "pgx5"
	return parsed.String(), nil
}
func fatal(message string) { fmt.Fprintln(os.Stderr, message); os.Exit(1) }
