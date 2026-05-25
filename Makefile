.PHONY: build test test-integration test-all lint vet clean docker k8s-deploy load-test perf-gate-baseline hooks deps-check config-check sse-stress sse-soak wal-stress lint-comments lint-comments-test comment-density comment-density-test check-changelog-date check-changelog-date-test

BINARY := cdc-sse
CMD     := ./cmd/cdc-sse

INTEGRATION_TIMEOUT := 120s

build:
	go build -o $(BINARY) $(CMD)

# Point this clone's git hooks at the versioned .githooks/ dir. Run once after
# every fresh `git clone` — core.hooksPath is per-clone config and does not
# travel with the repo.
hooks:
	git config core.hooksPath .githooks
	@echo "core.hooksPath -> .githooks"

test:
	go test -race ./...

# `testbin/cdc-sse` is the prebuilt integration-test target binary. The
# integration harness (`test/integration/harness.go::NewHarness`) execs this
# path. Rebuild on any change to cmd/ or internal/ Go sources, or to go.mod /
# go.sum. The `mkdir -p` keeps the recipe idempotent on a clean checkout.
testbin/cdc-sse: $(shell find cmd internal -name '*.go') go.mod go.sum
	@mkdir -p testbin
	go build -o testbin/cdc-sse $(CMD)

# CRITICAL — Pitfall G4: the `-tags=integration` flag and the explicit
# `./test/integration/...` path are MANDATORY. Without the tag, the
# `//go:build integration`-gated test files compile to zero tests and the
# target appears to pass while running nothing. Without the explicit path
# `./test/integration/...`, `./...` would still pass because the build tag
# filters every file; this restriction documents intent and prevents future
# accidental "go test -tags=integration ./..." invocations from sweeping in
# unintended packages.
test-integration: testbin/cdc-sse
	go test -tags=integration -race -timeout $(INTEGRATION_TIMEOUT) ./test/integration/...

# `test-all` runs the unit suite first (fast feedback) then integration. It
# has no recipe — Make handles the prerequisite chain.
test-all: test test-integration

lint:
	@command -v golangci-lint >/dev/null 2>&1 || { \
		echo "golangci-lint not found. Install v2: https://golangci-lint.run/welcome/install/"; \
		exit 1; \
	}
	golangci-lint run ./...

vet:
	go vet ./...

# Enforce the directional `internal/` import graph (DEPS-01..04). Any match in
# any of the four `go list -deps | grep -E` invocations is a violation and
# fails the build. Phase 11 wires this into CI; locally it is invoked by hand
# or via pre-push hooks. Uses bash explicitly so `pipefail` propagates a
# failed `go list` (e.g. import cycle) instead of being swallowed by the
# subsequent `grep`.
deps-check: SHELL := /bin/bash
deps-check:
	@set -eo pipefail; \
	if go list -deps ./internal/router/... | grep -E 'internal/(auth|sse)$$'; then \
		echo "deps-check: FAIL - internal/router imports auth or sse"; exit 1; \
	fi; \
	if go list -deps ./internal/wal/... | grep -E 'internal/sse$$'; then \
		echo "deps-check: FAIL - internal/wal imports sse"; exit 1; \
	fi; \
	if go list -deps ./internal/config/... | grep -E 'internal/(auth|health|limits|router|sse|wal)$$'; then \
		echo "deps-check: FAIL - internal/config imports a sibling internal/* package"; exit 1; \
	fi; \
	if go list -deps ./internal/health/... | grep -E 'internal/(auth|router|sse|wal)$$'; then \
		echo "deps-check: FAIL - internal/health imports auth, router, sse, or wal"; exit 1; \
	fi; \
	echo "deps-check: OK"

# config-check greps the source tree for any literal naming a WALERA_-prefixed
# dev-only env var (EXPERIMENTAL_, DEBUG_FORCE_, PLAN_) inside a Go file that
# does NOT carry the `dev` build tag. The runtime guard in
# internal/config/dev_guard.go refuses such env vars at load time; this
# static check catches the case where source mentions the literal so the
# intent is either build-tagged or removed before the binary is built.
config-check: SHELL := /bin/bash
config-check:
	@set -euo pipefail; \
	violations=$$(grep -rEn '"WALERA_(EXPERIMENTAL|DEBUG_FORCE|PLAN)_[A-Z0-9_]+"' \
	    --include='*.go' \
	    --exclude-dir=vendor \
	    cmd/ internal/ test/ 2>/dev/null || true); \
	if [ -z "$$violations" ]; then \
	    echo "config-check: OK"; \
	    exit 0; \
	fi; \
	bad=""; \
	while IFS= read -r line; do \
	    file=$$(echo "$$line" | cut -d: -f1); \
	    if ! head -5 "$$file" | grep -q '^//go:build .*\bdev\b'; then \
	        bad="$$bad$$line"$$'\n'; \
	    fi; \
	done <<< "$$violations"; \
	if [ -z "$$bad" ]; then \
	    echo "config-check: OK (all matches are in //go:build dev files)"; \
	    exit 0; \
	fi; \
	echo "config-check: FAIL - dev-only env var literals in non-dev-build files:"; \
	echo "$$bad"; \
	exit 1

# sse-stress runs the SSE package under the race detector with a
# count=100 soak budget. Soak escalator only — the standard `go test`
# invocation already runs the SSE tests (including
# TestPoolSlowClientIsolationStress) once with -race. Phase 9 D-15: this
# target is NOT wired into CI; Phase 11 picks it up.
# Operators may override the per-cohort subscriber count via
#   make sse-stress STRESS_SUBS=200
# which forwards to the -stress-subs test flag.
STRESS_SUBS ?= 100
sse-stress:
	go test -race -count=100 -run TestPoolSlowClientIsolationStress ./internal/sse -stress-subs=$(STRESS_SUBS)

# sse-soak runs the FULL internal/sse unit suite under count=100 without
# -race. SSE-08 flake guard: a regression that re-introduces the SSE-06
# starvation surface (e.g. a future commit that re-skips attachCh /
# shutdownCh observation in pollAllQueues) would surface as
# TestPoolWorkerLoopStarvation_AttachAndShutdown failing a fraction of the
# 100 runs. This is orthogonal to `make sse-stress`, which scopes to a
# single -race test (TestPoolSlowClientIsolationStress) at -count=100.
# Both are wired into the scheduled flake-detect workflow.
sse-soak:
	go test -count=100 ./internal/sse

# wal-stress runs the wal package under the race detector with a count=10
# soak budget. Lower than sse-stress (-count=100) because the wal package's
# integration tests run testcontainers; 100 iterations would take hours.
# Soak escalator only — standard `go test -race` already runs the wal tests
# once. Phase 10 D-12: this target is NOT wired into CI; Phase 11 picks it
# up alongside sse-stress. Note: this target runs ./internal/wal (the unit
# test package) only. The testcontainers-backed lifecycle tests live in
# ./test/integration and are soaked via `make test-integration` directly.
wal-stress:
	go test -race -count=10 ./internal/wal

# Banned-token comment lint for *.go sources. Enforces REQUIREMENTS.md
# HYGIENE-04 (no GSD-planning tokens — plan IDs, phase IDs, ticket IDs,
# PITFALL, etc. — in production Go comments) locally with the same script
# the CI gate runs (`comment-hygiene` job in .github/workflows/checks.yml,
# satisfying HYGIENE-05). The script restricts matching to Go comment
# lines so string-literal hits (e.g. t.Skip("...follow-up...")) are
# excluded. Exit 0 on clean, 1 on any hit.
lint-comments:
	@bash scripts/lint-comment-tokens.sh

# Shell-level unit test for scripts/lint-comment-tokens.sh. Asserts exit
# codes and annotation output against five synthetic fixtures (clean,
# single-token, multi-token, non-.go-ignored, string-literal-ignored).
lint-comments-test:
	@bash scripts/lint-comment-tokens_test.sh

# Per-package comment-to-code ratio gate (SWEEP-05). Default ceiling 30%.
# Sweeps the four hot packages (internal/{sse,router,app,auth}) and exits
# 1 if any package exceeds the ceiling. Mirrors lint-comments / lint-
# comments-test: pure bash + awk + grep, no Go toolchain needed.
comment-density:
	@bash scripts/comment-density.sh --all

# Shell-level unit test for scripts/comment-density.sh. Asserts exit
# codes, --max-pct override, --json shape, *_test.go exclusion, and
# --all multi-package behavior against eight synthetic fixtures.
comment-density-test:
	@bash scripts/comment-density_test.sh

# CHANGELOG date-placeholder guard. Closes REQUIREMENTS.md RELEASE-02:
# release.yml fails fast at tag-cut time on a residual `YYYY-MM-DD`
# placeholder. The CI gate `changelog-date-test` in
# .github/workflows/checks.yml runs the unit test on every PR and push
# to master so the guard cannot be silently disabled. Local invocation
# with no argument scans CHANGELOG.md at the repo root; until the v2.0
# tag is cut and the placeholder is replaced with a real date, this
# target is currently expected to exit 1 (fail-fast on placeholder).
check-changelog-date:
	@bash scripts/check-changelog-date.sh

# Shell-level unit test for scripts/check-changelog-date.sh. Asserts
# exit codes and annotation output against five synthetic fixtures
# (clean / single-placeholder / mixed / empty-file / missing-file).
check-changelog-date-test:
	@bash scripts/check-changelog-date_test.sh

# Build the production container image. Tags `walera:dev` per D4-25; the CI
# pipeline retags with the release semver before pushing to a registry.
docker:
	docker build -t walera:dev .

# Apply the kustomize-aggregated k8s manifests. Warns (but does not block) if
# neither KUBECONFIG nor ~/.kube/config exists — kubectl will surface the
# missing-config error on the next line, but the message we emit here is
# more useful for first-time operators.
k8s-deploy:
	@if [ -z "$$KUBECONFIG" ] && [ ! -f $$HOME/.kube/config ]; then \
	  echo "WARN: no KUBECONFIG and no ~/.kube/config; kubectl apply will likely fail"; \
	fi
	kubectl apply -k deploy/k8s/

# Load-testing harness is post-MVP per D4-25 / PROJECT.md "Out of Scope".
# The stub exists so `make help` (future) lists every planned target without
# 404-ing the operator.
load-test:
	@echo "Load testing harness is post-MVP; see .planning/research/FEATURES.md"

clean:
	rm -f $(BINARY) *.test coverage.out coverage.html
	rm -rf testbin

# Phase 20 GATE-03: rebuild the perf-gate thresholds.yml from a fresh bench
# capture on deeb007. Runs the FULL 5-minute Phase 19 benches (NOT the 90s
# gate window), computes 20% safety-margin floors, writes the rewritten
# YAML with refreshed captured/captured_commit metadata. The operator
# reviews the PR diff before committing the new YAML. Optional
# --regen-strace flag also re-runs the VAL-01 strace baseline:
#   make perf-gate-baseline ARGS=--regen-strace
# See scripts/perf-gate-regen.sh for the full contract.
perf-gate-baseline:
	@command -v docker >/dev/null 2>&1 || { \
		echo "docker not found on PATH; perf-gate-baseline requires docker compose"; \
		exit 1; \
	}
	bash scripts/perf-gate-regen.sh $(ARGS)
