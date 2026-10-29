# Walera

## Project

Walera is a Go service that streams PostgreSQL row changes to clients over
Server-Sent Events. A client subscribes to one row (`/sse/v1/{table}/{pk}`) or
one table (`/sse/v1/{table}/all`); Walera asks the operator's auth backend
(`POST /auth/sessions`) which tables and columns the user may see, tails the
WAL through `pgoutput` logical replication, and delivers each committed
transaction as one SSE frame filtered down to the allowed columns.

The goal is a lightweight, secure real-time layer for small projects: one
binary, configured by 12 environment variables, no broker and no storage.
It fails closed — no grant, no data — and re-checks permissions on live
streams every TTL.

User-facing docs: `README.md`, `docs/auth.md`, `docs/postgres.md`,
`docs/operations.md`, `docs/architecture.md`. Keep them in sync with the
code; user-visible changes also go into `CHANGELOG.md` under `## [Unreleased]`.

## Constraints

- Go 1.26 (`go.mod`: `go 1.26.10`).
- Replication via `github.com/jackc/pglogrepl` and `pgoutput`; no `wal2json`,
  no Debezium/Kafka.
- PostgreSQL ≥ 14, `wal_level = logical`, direct connection (no PgBouncer),
  temporary replication slot only.
- Single instance; no clustering, no persistence, no `Last-Event-ID` resume.
- Must be `-race`-clean.
- Security/PII: never log row data, tokens, or secrets; PK values are OK (identifiers, not content); wildcards intended for publicly-enumerable entities only.
- Configuration errors name the variable, never its value; DSNs in errors
  go through `config.RedactDSN`.
- SSE endpoint is versioned (`/sse/v1/`); frame format changes need `/v2/`.
- Do not add dependencies without a strong reason.

## Stack

| Module | Use |
| --- | --- |
| `github.com/jackc/pglogrepl` | Replication protocol, `pgoutput` decoding |
| `github.com/jackc/pgx/v5` | Admin connection, `pgconn` for replication |
| `github.com/prometheus/client_golang` | Metrics |
| `github.com/rs/zerolog` | JSON logging |
| `go.uber.org/automaxprocs` | `GOMAXPROCS` from the container CPU quota |
| `github.com/testcontainers/testcontainers-go` (+ `modules/postgres`) | Integration tests |
| `go.uber.org/goleak`, `github.com/prometheus/client_model` | Tests |

HTTP is stdlib `net/http` (`http.ServeMux` patterns, h2c); no web framework.

## Commands

```bash
make build                                      # ./cdc-sse
make test                                       # go test -race ./...
make testbin/cdc-sse && make test-integration   # needs Docker
go vet ./... && go vet -tags=integration ./...
golangci-lint run ./... && golangci-lint run --build-tags=integration ./...   # golangci-lint v2.15.0
make deps-check                                 # one-way internal/ import graph
```

## Conventions

- Configuration lives in `internal/app/env.go` (`envVars`, `LoadFromEnv`);
  fixed limits are the `Defaults()` of each package.
- Dependencies are passed explicitly (`Config` + `Deps` structs, hand-wired
  in `internal/app/initialize.go`).
- The replication connection has exactly one owner goroutine (the WAL
  reader loop).
- The router never blocks on a subscriber; each stream's HTTP handler owns
  its writes.
- Comments are sparse and explain why. Conventional commits.
