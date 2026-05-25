# Walera Third-Party License Review

Walera is distributed under the [MIT License](../LICENSE). This document
covers every direct module listed in the first `require ( ... )` block of
`go.mod` (the block that does **not** carry indirect-dependency markers).
Each
license below has been reviewed and found compatible with downstream MIT
distribution: MIT, BSD-2-Clause, BSD-3-Clause, Apache-2.0, and ISC all
combine cleanly with MIT for redistributed binaries, and the Apache-2.0
patent grant is preserved in MIT redistribution per §4 of Apache-2.0.
Transitive (indirect) dependencies are explicitly out of scope for this
review per Phase 1 scope decision D-06; revisit when a future phase
introduces a copyleft or unusual-license dependency. This document is
hand-maintained — no `go-licenses` automation runs in CI today (deferred
to Phase 11).

## Direct Dependencies

| Module | Version | License (SPDX) | Notes |
|--------|---------|----------------|-------|
| github.com/cespare/xxhash/v2 | v2.3.0 | MIT | Sharded subscription index hashing |
| github.com/jackc/pglogrepl | v0.0.0-20260401131349-e37c41485510 | MIT | WAL pgoutput decoding (LOCKED choice; pseudo-version, upstream is MIT) |
| github.com/jackc/pgx/v5 | v5.9.1 | MIT | Admin DB pool and pgconn for replication |
| github.com/knadh/koanf/parsers/yaml | v0.1.0 | MIT | YAML config parser |
| github.com/knadh/koanf/providers/env/v2 | v2.0.0 | MIT | Env-var config provider |
| github.com/knadh/koanf/providers/file | v0.1.0 | MIT | File config provider |
| github.com/knadh/koanf/v2 | v2.3.4 | MIT | Config loader (YAML + env) |
| github.com/prometheus/client_golang | v1.23.2 | Apache-2.0 | Prometheus metrics (Apache-2.0 patent grant compatible with MIT redistribution) |
| github.com/prometheus/client_model | v0.6.2 | Apache-2.0 | Prometheus data model (companion to client_golang) |
| github.com/rs/zerolog | v1.35.1 | MIT | Structured zero-alloc logging |
| github.com/testcontainers/testcontainers-go | v0.42.0 | MIT | test-only — container lifecycle for integration tests |
| github.com/testcontainers/testcontainers-go/modules/postgres | v0.42.0 | MIT | test-only — PostgreSQL container module |
| go.uber.org/automaxprocs | v1.6.0 | MIT | Cgroup-aware GOMAXPROCS for k8s pods |
| golang.org/x/time | v0.15.0 | BSD-3-Clause | Token-bucket rate limiter (Go subrepo) |
| gopkg.in/yaml.v3 | v3.0.1 | MIT and Apache-2.0 | YAML v3 parser (dual-licensed; MIT applies to most files, Apache-2.0 to some — both compatible with MIT redistribution) |

## Compatibility Conclusion

All direct dependencies are licensed under MIT, BSD-3-Clause, Apache-2.0,
or a dual MIT/Apache-2.0 combination. None of these licenses impose terms
that conflict with redistributing Walera under MIT. Apache-2.0
dependencies retain their notices when Walera is redistributed in binary
form; consumers redistributing source must preserve upstream NOTICE files
from those modules.

## Review Cadence

This document is reviewed when (1) a new direct dependency is added to
`go.mod`, (2) an existing direct dependency changes its license, or (3)
the project's own license changes. CI-side license scanning
(`go-licenses check`) is deferred to Phase 11 per the Phase 1 CONTEXT
`<deferred>` section.
