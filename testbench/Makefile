# testbench/Makefile
# All targets are idempotent. `make -C testbench <target>` runs from any cwd.

COMPOSE := docker compose -f docker-compose.yml --env-file .env

.PHONY: demo-up demo-down demo-reset demo-logs bench-capture-index

demo-up:           ## Start the testbench (idempotent — re-running is a no-op if healthy).
	$(COMPOSE) up -d

demo-down:         ## Stop containers; PRESERVES volumes (use demo-reset for cold start).
	@echo ">>> demo-down preserves volumes. Use 'make demo-reset' for a fresh PG cluster."
	$(COMPOSE) down

demo-reset:        ## Stop containers AND remove volumes (cold-start guarantee).
	$(COMPOSE) down -v --remove-orphans

demo-logs:         ## Tail logs from all services.
	$(COMPOSE) logs -f

bench-capture-index: ## Re-capture the exact-index benchstat baseline (GEN-01/GEN-02 gate).
	## NOTE: -race is intentionally omitted — race instrumentation invalidates allocs/op.
	## Update the provenance header in bench-baseline-index-v2.5.txt after re-running.
	go test -bench='^BenchmarkIndex' -benchmem -count=10 -run='^$$' -timeout=15m \
		../internal/router/... > bench-baseline-index-v2.5.txt.tmp \
		&& mv bench-baseline-index-v2.5.txt.tmp bench-baseline-index-v2.5.txt
