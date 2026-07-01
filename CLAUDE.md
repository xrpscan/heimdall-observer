# Heimdall Observer

XRPL validation stream observer. Connects to an xrpld node via WebSocket, batches validation messages into an embedded SQLite database, then produces them to Kafka.

## Build & test

All commands run from the project root.

- `make build` — compile binary to `bin/observer`
- `make run` — tidy, build, and run
- `make test` — run tests with race detector
- `make lint` — run golangci-lint
- `make image` — build container image
- `make container` — run container locally (needs `config/config.json`)
- `make migrate-up` / `make migrate-down` — apply or roll back SQLite migrations via golang-migrate

The binary accepts `-config <path>` (default: `config/config.json`).

## Architecture

- `cmd/observer/main.go` — entrypoint; wires dependencies and manages lifecycle.
- `internal/config/` — JSON config loading and validation.
- `internal/logger/` — slog setup with file logging (lumberjack rotation) and context-based attribute propagation.
- `internal/rest/` — HTTP server, handler, and middleware stack (CORS, recovery, access log, body limit).
- `internal/proc/` — stream processing. `ValidationStreamProcessor` batches messages and flushes to DB. `DatabaseKafkaSynchronizer` polls DB and produces to Kafka.
- `internal/store/` — storage layer; `Client` interface with `Embedded` (SQLite + WAL mode) implementation.
- `pkg/xrpld/` — WebSocket client for xrpld servers; handles subscriptions, request-response correlation, and graceful shutdown.
- `pkg/kafkaesque/` — Kafka client wrapper using franz-go; supports plaintext and SCRAM-SHA-512 + TLS modes.
- `pkg/registry/` — service registry for ordered graceful shutdown (closes in reverse registration order).
- `pkg/httputils/` — HTTP response writing, typed errors, `ResponseWriterWithCode` wrapper.
- `db/migrations/` — SQL migration files for golang-migrate.

## Tech stack

- Go 1.26, module path `github.com/xrpscan/heimdall-observer`
- `coder/websocket` for the xrpld WebSocket connection
- `twmb/franz-go` for Kafka production (with `pkg/sasl/scram` for SCRAM-SHA-512)
- `modernc.org/sqlite` (pure-Go SQLite driver, no CGo)
- `natefinch/lumberjack` for log file rotation
- `golang-migrate` for schema migrations
- golangci-lint with gci formatter for import ordering

## Conventions

- `internal/` for app-specific code, `pkg/` for reusable packages.
- Errors use `fmt.Errorf` with `%w` wrapping. Sentinel errors only at package boundaries (e.g. `ErrFatal`).
- Config validation errors use field-path style: `"httpServer.addr is required"`.
- Always use `slog.InfoContext`/`slog.ErrorContext` with a context — never the context-less variants.
- Channel ownership: only the writer closes a channel.
- When adding config fields, update the struct and validation in `internal/config/config.go` and the example in `config/config.example.json`.

## Shutdown

Services that need graceful shutdown implement `pkg/registry.Closer` and are registered in `main.go`. Registration order matters — services close in reverse order. For example, the validation stream processor is registered after the DB so it flushes remaining batches before the DB connection closes.

## CI/CD

- CI runs lint, test, and build on pull requests.
- CD runs the same checks on pushes to main, then tags a semver release via semantic-release.
- Conventional commit messages are required for semantic-release to work (`feat:`, `fix:`, `chore:`, etc.).
