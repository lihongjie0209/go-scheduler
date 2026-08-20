# Repository Guidelines

## Project Structure & Module Organization

Executable entry points live under `cmd/` (`scheduler`, `schedulerctl`, and `scheduler-bench`). Domain and infrastructure code belongs in `internal/`, grouped by concern such as `api`, `core`, `rpc`, `schedule`, and `store`; the reusable executor SDK is in `pkg/executor`. Protobuf definitions are under `api/proto`, with generated Go files in `gen/`. Migrations, manifests, documentation, scripts, and Python CLI tests live in `migrations/`, `deploy/`, `docs/`, `hack/`, and `tests/`.

## Build, Test, and Development Commands

- `make build` builds the three supported binaries into `bin/`.
- `make test` runs all Go unit tests; `make race` adds the race detector.
- `make integration` runs module, cross-module, and end-to-end use-case suites. These tagged tests use Testcontainers and require Docker.
- `make lint` runs configured `golangci-lint` checks and formatters.
- `make security` scans dependencies and call paths with `govulncheck`.
- `make generate` regenerates protobuf and gRPC sources; commit resulting `gen/` changes with the schema change.
- `make migrate-up` applies migrations using `DATABASE_URL`. See `README.md` for local PostgreSQL setup and bootstrap commands.

## Coding Style & Naming Conventions

Target the Go version in `go.mod`. Run `gofmt` and `goimports`; `.golangci.yml` defines lint rules. Use tabs as produced by `gofmt`, lowercase package names, MixedCaps identifiers, and standard initialisms (`HTTP`, `ID`, `URL`). Expose code through `pkg/` only for external consumers. Do not hand-edit generated `*.pb.go` files.

## Testing Guidelines

Place tests beside implementation files as `*_test.go`, with names such as `TestServerRejectsUnknownHandler`. Prefer table-driven cases and descriptive subtests. Mark environment-dependent suites with the `integration` build tag. Before submitting, run `make test`, `make race`, and `make lint`; run integration targets for changes touching PostgreSQL, etcd, gRPC, executors, or migrations. CI also runs `go vet`, shuffled coverage tests, module-tidiness checks, and vulnerability scanning.

## Commit & Pull Request Guidelines

History follows Conventional Commit prefixes such as `feat:`, `fix:`, `test:`, `perf:`, and `build:`. Keep subjects imperative and scoped to one logical change. Pull requests should explain behavior and operational impact, link related issues, list validation performed, and call out migrations, API/protobuf changes, or configuration additions. Include request/response or CLI examples when behavior changes; screenshots are unnecessary because this repository has no Web UI.

## Security & Configuration

Never commit database URLs, API keys, service tokens, JWT secrets, encryption keys, kubeconfigs, or registry credentials. Use environment variables and redacted examples. Preserve TLS defaults and validate authorization boundaries when changing REST, gRPC, executor, or tenant-facing code.
