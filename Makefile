.PHONY: build test test-integration test-all lint vet clean docker hooks deps-check check-changelog-date check-changelog-date-test

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
		echo "golangci-lint not found. Install: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0"; \
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

clean:
	rm -f $(BINARY) *.test coverage.out coverage.html
	rm -rf testbin
