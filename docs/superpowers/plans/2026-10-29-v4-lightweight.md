# Walera v4: Lightweight Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Довести Walera до цели «максимально легковесный real-time для пет-проектов с максимальным security»: закрыть оставшиеся дыры (публикации `FOR TABLES IN SCHEMA`, ACK при backpressure), убрать нагрузочную/процессную обвязку, сжать метрики до 5, конфиг — до env-переменных, заменить шардированный индекс на map, переписать документацию с нуля и выпустить v4.0.0.

**Architecture:** Серия независимых вычитаний и замен поверх текущего `master` (v3.0.1): каждая задача удаляет или упрощает один слой и оставляет дерево зелёным. Пакетные `Config`-структуры сохраняются (меньше churn в тестах), но koanf/YAML заменяется одним загрузчиком env в `internal/app/env.go`; всё, что не вынесено в env, становится константами в коде.

**Tech Stack:** Go 1.26.10, pgx v5 / pglogrepl, `net/http`, zerolog, Prometheus client, testcontainers-go (интеграционные тесты), golangci-lint v2.15.0, GitHub Actions.

**Spec:** Решения пользователя от 2026-10-29 (зафиксированы здесь, отдельного spec-файла нет):
- конфиг — только env (~8 переменных; итог плана — 12, см. Global Constraints), всё остальное — константы, koanf и `config.yaml` удаляются;
- нагрузочная обвязка (testbench, `cmd/writer`, `cmd/loadgen`, бенчмарки, workflow `testbench-smoke`/`bench-regression`/`flake-detect`) — удалить;
- метрики — 5 базовых: `walera_subscribers_active`, `walera_events_sent_total`, `walera_subscriber_disconnects_total{reason}`, `walera_auth_requests_total{result}`, `walera_pg_connection_status`; lag sampler и pprof — удалить;
- деплой — Dockerfile + пример compose; `deploy/k8s`, `deploy/prometheus`, `docker-compose.dev.yaml` — удалить;
- `.planning` — снести, документацию — переписать с нуля;
- хвосты из `TODO.local.md` §4a (в worktree, не в git).

**Решения, принятые при составлении плана (провизорные, без отдельного spec):**
- Env-переменных 12 (2 обязательные): лимит соединений, форвардинг cookie/заголовков и plaintext-разрешение — реальные операторские ручки, их не заменить константой.
- `limits.trusted_proxies` удаляется: client IP используется только в поле лога `client_ip`.
- Режимы `wal.bootstrap.mode` (`auto`/`verify`/`off`) и `create_roles` удаляются: публикация есть → проверяем; нет и задан `WALERA_TABLES` → создаём `FOR TABLE`; иначе — ошибка старта.
- Любая неизвестная переменная с префиксом `WALERA_` — ошибка старта (заменяет `removedConfigKeys` и dev-guard).
- `coverage.yml` и comment-hygiene (`lint-comment-tokens`, `comment-density`) — процессные артефакты, удаляются; в CI остаются `checks`, `release`, `publish-image`.
- Пропущенные (`t.Skip`) нестабильные интеграционные тесты `06_slow_consumer`, `12_sec01_slow_receiver` удаляются: поведение медленного клиента покрыто unit-тестами `internal/sse`.

## Global Constraints

- Модуль: `go 1.26.10`; новых зависимостей не добавлять; после задач — `go mod tidy` без лишних модулей.
- Каждая задача оставляет зелёными: `go vet ./... && go vet -tags=integration ./...`, `go test -race ./...`, `$(go env GOPATH)/bin/golangci-lint run ./...` и `--build-tags=integration` (0 issues).
- Security/PII: никогда не логировать данные строк, токены, секреты; PK можно. Ошибки конфигурации называют переменную, но не её значение; DSN в ошибках — через `config.RedactDSN`.
- SSE-эндпоинт остаётся `/sse/v1/…`; формат кадров не меняется.
- Env-переменные v4 (полный список, имена дословно): обязательные `WALERA_DATABASE_URL`, `WALERA_AUTH_URL`; опциональные `WALERA_ADDR` (`:8080`), `WALERA_PUBLICATION` (`walera_pub`), `WALERA_TABLES` (пусто), `WALERA_CORS_ORIGINS` (пусто), `WALERA_AUTH_TTL` (`60s`, ≥ `1s`), `WALERA_AUTH_FORWARD_COOKIES` (пусто), `WALERA_AUTH_FORWARD_HEADERS` (пусто), `WALERA_AUTH_ALLOW_PLAINTEXT` (`0`), `WALERA_MAX_CONNECTIONS` (`50000`), `WALERA_LOG_LEVEL` (`info`).
- Интеграционные тесты: перед запуском `make testbin/cdc-sse`; запуск `go test -tags=integration -race -count=1 -timeout 30m ./test/integration/...` (нужен Docker).
- Пользовательские изменения — в `CHANGELOG.md` под `## [Unreleased]` (подразделы `### Security` / `### Changed` / `### Removed` / `### Fixed`; создать недостающие; без `YYYY-MM-DD`).
- Не использовать голый `git stash`; для сравнения со старым кодом — временный worktree (`git worktree add /tmp/walera-base <sha>`, затем `git worktree remove --force`).
- Коммиты — conventional commits.

## Review Focus

1. Оператор обновляется с v3: монтирует `config.yaml` или задаёт старые имена (`WALERA_AUTH_BACKEND_URL`, `WALERA_WAL_PUBLICATION_NAME`, `WALERA_HTTP_CORS_ORIGINS`) → старт падает с ошибкой, называющей неизвестные переменные, а не молча берёт дефолты (Task 7, `TestLoadFromEnv_RejectsUnknownVars`).
2. Публикация `FOR TABLES IN SCHEMA` на PG 15+ отклоняется; на PG 14 (нет `pg_publication_namespace`) проверка не падает с SQL-ошибкой (Task 1, кейсы версии 140010 и 150004).
3. Потребитель транзакций медленный (`txCh` полон) → ридер продолжает слать standby-подтверждения, Postgres не рвёт соединение по `wal_sender_timeout` (Task 2, `TestDeliver_ACKsWhileBlocked`).
4. `WALERA_TABLES` не задан, публикации нет → понятная ошибка старта с инструкцией, а не молчаливый запуск без данных (Task 7, `TestBootstrapPublication_MissingWithoutTables`).
5. `WALERA_DATABASE_URL` с паролем и ошибкой формата → ни в ошибке, ни в логе нет пароля (Task 7, `TestLoadFromEnv_InvalidDSNRedactsPassword`).

---

### Task 1: Отказ от публикаций `FOR TABLES IN SCHEMA`

**Files:**
- Modify: `internal/app/bootstrap.go` (новая `publicationHasSchemas`, `errSchemaPublication`; вызовы в ветках `verify` и `auto` после проверки `allTables`)
- Test: `internal/app/bootstrap_test.go`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `bootstrapDB`, `lookupPublication`, `errAllTablesPublication` (уже есть в `internal/app/bootstrap.go`); фейки `fakeBootstrapDB`/`fakeRow` в `bootstrap_test.go` (QueryRow-результаты отдаются по очереди).
- Produces: `publicationHasSchemas(ctx context.Context, conn bootstrapDB, name string) (bool, error)`; `errSchemaPublication(name string) error`. Task 7 переиспользует обе.

Почему: на PG 15+ публикация `FOR TABLES IN SCHEMA public` стримит каждую таблицу схемы, включая будущие — та же дыра, что `FOR ALL TABLES`. Таблица `pg_publication_namespace` есть только с PG 15 (`server_version_num >= 150000`), на PG 14 запрос к ней падает.

- [ ] **Step 1: Write the failing tests**

В `internal/app/bootstrap_test.go` добавить:

```go
// A FOR TABLES IN SCHEMA publication (PG 15+) streams every table in the
// schema, including future ones; both checking modes must refuse it.
func TestBootstrapPublication_RejectsSchemaPublication(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"verify", "auto"} {
		t.Run(mode, func(t *testing.T) {
			db := &fakeBootstrapDB{rowResults: []fakeRow{
				{values: []any{false}},  // puballtables
				{values: []any{150004}}, // server_version_num
				{values: []any{true}},   // has schema entries
			}}
			err := bootstrapPublication(ctx, db, bootstrapConfig{Mode: mode, PublicationName: "pub", Tables: []string{"public.a"}}, zerolog.Nop())
			if err == nil || !strings.Contains(err.Error(), "FOR TABLES IN SCHEMA") {
				t.Fatalf("err = %v; want FOR TABLES IN SCHEMA rejection", err)
			}
			if len(db.execSQL) != 0 {
				t.Fatalf("exec = %#v; want no DDL", db.execSQL)
			}
		})
	}
}

// PG 14 has no pg_publication_namespace; the check must skip, not query it.
func TestPublicationHasSchemas_PG14Skips(t *testing.T) {
	db := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{140010}}}}
	got, err := publicationHasSchemas(context.Background(), db, "pub")
	if err != nil || got {
		t.Fatalf("publicationHasSchemas = %v, %v; want false, nil", got, err)
	}
	if len(db.rowResults) != 0 {
		t.Fatalf("unused rows %d; want exactly one query (version)", len(db.rowResults))
	}
}
```

Обновить существующие ожидания в `TestBootstrapPublicationModes` и `TestPrepareDatabaseUnit`: после каждой строки `{values: []any{false}}` (ответ `puballtables` для существующей публикации) вставить `{values: []any{150004}}, {values: []any{false}},` (версия + «схем нет»). Кейсы, где публикации нет (`fakeRow{err: pgx.ErrNoRows}`), не меняются.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/app/ -run 'TestBootstrapPublication|TestPublicationHasSchemas|TestPrepareDatabaseUnit' -v`
Expected: FAIL — `undefined: publicationHasSchemas`.

- [ ] **Step 3: Implement**

В `internal/app/bootstrap.go` рядом с `lookupPublication`:

```go
// publicationHasSchemas reports whether the publication includes whole
// schemas (FOR TABLES IN SCHEMA, PG 15+). PG 14 has no such publications.
func publicationHasSchemas(ctx context.Context, conn bootstrapDB, name string) (bool, error) {
	var version int
	if err := conn.QueryRow(ctx,
		"SELECT current_setting('server_version_num')::int",
	).Scan(&version); err != nil {
		return false, fmt.Errorf("server version check failed: %w", err)
	}
	if version < 150000 {
		return false, nil
	}
	var has bool
	if err := conn.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM pg_publication_namespace pn JOIN pg_publication p ON p.oid = pn.pnpubid WHERE p.pubname = $1)",
		name,
	).Scan(&has); err != nil {
		return false, fmt.Errorf("publication schema check failed (%s): %w", name, err)
	}
	return has, nil
}

func errSchemaPublication(name string) error {
	return fmt.Errorf(
		"publication %q includes FOR TABLES IN SCHEMA, which streams every table in the schema — "+
			"recreate it with an explicit list: DROP PUBLICATION %s; CREATE PUBLICATION %s FOR TABLE <list>",
		name, name, name,
	)
}
```

В ветке `verify` сразу после `if allTables { return errAllTablesPublication(...) }` и в ветке `auto` сразу после аналогичной проверки внутри `if exists {`:

```go
		if hasSchemas, err := publicationHasSchemas(ctx, conn, cfg.PublicationName); err != nil {
			return err
		} else if hasSchemas {
			return errSchemaPublication(cfg.PublicationName)
		}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/app/`
Expected: PASS.

- [ ] **Step 5: Integration check (real PG 18)**

Добавить в `internal/app/bootstrap_integration_test.go` (build tag `integration`) тест, использующий существующие `bootPGSuper`/`connAs`:

```go
func TestPublicationHasSchemas_RealPG(t *testing.T) {
	host, port := bootPGSuper(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	conn := connAs(t, ctx, dsnFor(host, port, "walera", "walera"))
	for _, ddl := range []string{
		"CREATE TABLE public.t1 (id int primary key)",
		"CREATE PUBLICATION pub_schema FOR TABLES IN SCHEMA public",
		"CREATE PUBLICATION pub_list FOR TABLE public.t1",
	} {
		if _, err := conn.Exec(ctx, ddl); err != nil {
			t.Fatalf("%s: %v", ddl, err)
		}
	}
	for name, want := range map[string]bool{"pub_schema": true, "pub_list": false} {
		got, err := publicationHasSchemas(ctx, conn, name)
		if err != nil || got != want {
			t.Errorf("%s: got %v, %v; want %v, nil", name, got, err, want)
		}
	}
}
```

Run: `go test -tags=integration -race -count=1 -run TestPublicationHasSchemas_RealPG ./internal/app/`
Expected: PASS.

- [ ] **Step 6: Changelog + commit**

`CHANGELOG.md` → `## [Unreleased]` → `### Security`:

```markdown
- **`FOR TABLES IN SCHEMA` publications are refused.** On PostgreSQL 15+
  such a publication streams every table in the schema, including tables
  added later; Walera now fails startup on it, like `FOR ALL TABLES`.
```

```bash
git add internal/app/bootstrap.go internal/app/bootstrap_test.go internal/app/bootstrap_integration_test.go CHANGELOG.md
git commit -m "fix(bootstrap): refuse FOR TABLES IN SCHEMA publications"
```

---

### Task 2: WAL — подтверждения при backpressure и дедлайн записи

**Files:**
- Modify: `internal/wal/reader.go` (отправка в `r.txCh` в `processWALMessage`; `pgConnAdapter.SendACK`)
- Test: `internal/wal/reader_test.go`
- Modify: `test/integration/18_standby_ack_test.go`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `(*Reader).sendStandby(ctx)`, `(*Reader).standbyEvery()`, поле `standbyInterval` (тестовая подмена интервала), `fakeReplConn` (`AckCalls()`), `CurrentLSN()`.
- Produces: `(*Reader).deliver(ctx context.Context, tx Tx) error`; пакетная переменная `standbyWriteTimeout = 10 * time.Second`.

Почему:
- Пока `processWALMessage` ждёт места в `txCh` (роутер не успевает), цикл не читает и не шлёт ACK. Дольше `wal_sender_timeout` (60 s по умолчанию) — Postgres рвёт соединение. До v3.0.1 это маскировал отдельный тикер.
- `pglogrepl.SendStandbyStatusUpdate` игнорирует `ctx`: при забитом сокете запись висит бесконечно, и остановка сервиса её не прерывает.
- Интеграционный тест простоя проверяет только отсутствие ошибок ACK, но не то, что ACK вообще уходят.

- [ ] **Step 1: Write the failing unit tests**

В `internal/wal/reader_test.go` (используйте существующие конструкторы фейка и ридера из соседних тестов `runLoop` — найдите тест, который создаёт `Reader` с `standbyInterval` и `fakeReplConn`, и повторите его настройку):

```go
// While the router is not draining txCh, the reader must keep the walsender
// alive with standby updates instead of going silent until wal_sender_timeout.
func TestDeliver_ACKsWhileBlocked(t *testing.T) {
	t.Parallel()
	fc := newFakeReplConn(nil)
	r := newTestReader(t, fc)         // existing helper in this file; if absent, build &Reader{...} as the neighbouring runLoop tests do
	r.standbyInterval = 20 * time.Millisecond
	r.txCh = make(chan Tx)            // unbuffered and never read: delivery blocks

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.deliver(ctx, Tx{}) }()

	deadline := time.Now().Add(2 * time.Second)
	for len(fc.AckCalls()) < 3 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("deliver err = %v; want context.Canceled", err)
	}
	if n := len(fc.AckCalls()); n < 3 {
		t.Fatalf("ACKs while blocked = %d; want >= 3", n)
	}
}

func TestDeliver_SendsWhenReceiverReady(t *testing.T) {
	t.Parallel()
	fc := newFakeReplConn(nil)
	r := newTestReader(t, fc)
	r.txCh = make(chan Tx, 1)
	if err := r.deliver(context.Background(), Tx{}); err != nil {
		t.Fatalf("deliver: %v", err)
	}
	if len(r.txCh) != 1 {
		t.Fatal("tx not delivered")
	}
}
```

Если `newTestReader`/`newFakeReplConn` называются иначе — используйте фактические имена хелперов файла; не создавайте второй набор фейков.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/wal/ -run TestDeliver -v`
Expected: FAIL — `r.deliver undefined`.

- [ ] **Step 3: Implement `deliver`**

В `internal/wal/reader.go`:

```go
// deliver hands a committed tx to the router. While the router is busy the
// reader still owes Postgres standby updates, so they go out from here (same
// goroutine; the connection keeps a single owner).
func (r *Reader) deliver(ctx context.Context, tx Tx) error {
	t := time.NewTicker(r.standbyEvery())
	defer t.Stop()
	for {
		select {
		case r.txCh <- tx:
			return nil
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			r.sendStandby(ctx)
		}
	}
}
```

В `processWALMessage`, ветка `*pglogrepl.CommitMessage`, заменить `select { case r.txCh <- *tx: … case <-ctx.Done(): return ctx.Err() }` на:

```go
			if err := r.deliver(ctx, *tx); err != nil {
				return err
			}
```

- [ ] **Step 4: Write deadline on the standby write**

```go
// standbyWriteTimeout bounds a standby update write: pglogrepl ignores ctx, so
// a stuck socket would otherwise block the reader (and shutdown) forever.
var standbyWriteTimeout = 10 * time.Second

func (a *pgConnAdapter) SendACK(ctx context.Context, ssu pglogrepl.StandbyStatusUpdate) error {
	nc := a.conn.Conn()
	_ = nc.SetWriteDeadline(time.Now().Add(standbyWriteTimeout))
	defer func() { _ = nc.SetWriteDeadline(time.Time{}) }()
	return pglogrepl.SendStandbyStatusUpdate(ctx, a.conn, ssu)
}
```

Тест на адаптер (тот же файл): `net.Pipe()` без читателя на второй стороне, `pgconn.Construct(&pgconn.HijackedConn{Conn: c1, Frontend: pgproto3.NewFrontend(c1, c1), Config: &pgconn.Config{}, ParameterStatuses: map[string]string{}, TxStatus: 'I'})`, `standbyWriteTimeout = 50*time.Millisecond` (восстановить в `t.Cleanup`), `SendACK` должен вернуть ошибку меньше чем за 1 s, `errors.Is(err, os.ErrDeadlineExceeded)` или `net.Error.Timeout()`. Если `Construct` требует других полей — заполнить их по сигнатуре pgx v5.9.2 (`$(go list -m -f '{{.Dir}}' github.com/jackc/pgx/v5)/pgconn/pgconn.go`, `type HijackedConn`). Тест не должен быть параллельным (меняет пакетную переменную).

- [ ] **Step 5: Run unit tests**

Run: `go test -race -count=20 ./internal/wal/`
Expected: PASS, без утечек (в пакете goleak).

- [ ] **Step 6: Integration test asserts ACKs actually arrive**

Переписать `test/integration/18_standby_ack_test.go`: вместо счётчика ошибок проверять, что Postgres получает ответы ридера. Скелет (сохранить имя теста и `t.Parallel()`):

```go
func Test18StandbyACKReachesPostgres(t *testing.T) {
	t.Parallel()
	h := NewHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	replyTime := func() time.Time {
		t.Helper()
		var ts time.Time
		if err := h.PG.QueryRow(ctx,
			"SELECT reply_time FROM pg_stat_replication ORDER BY reply_time DESC NULLS LAST LIMIT 1",
		).Scan(&ts); err != nil {
			t.Fatalf("pg_stat_replication: %v", err)
		}
		return ts
	}
	first := replyTime()
	time.Sleep(12 * time.Second) // > 2 standby intervals of idle
	second := replyTime()
	if !second.After(first) {
		t.Fatalf("reply_time did not advance while idle: %v -> %v", first, second)
	}
	if strings.Contains(h.Binary.Stderr(), "standby status update failed") {
		t.Fatalf("standby update failures in logs:\n%s", h.Binary.Stderr())
	}
}
```

Если у `*PG` нет `QueryRow` — используйте тот способ запроса, который уже применяет `querySlotLag` в `test/integration/15_large_tx_test.go` (`pgx.Connect(ctx, p.DSN)`). Убрать из файла проверку `walera_wal_standby_ack_failures_total` (метрика уходит в Task 4).

Run: `make testbin/cdc-sse && go test -tags=integration -race -count=3 -run Test18 ./test/integration/...`
Expected: PASS 3/3.

- [ ] **Step 7: Changelog + commit**

`### Fixed`:

```markdown
- **Standby updates keep flowing under backpressure.** While the router is
  slow to accept a transaction, the WAL reader still reports its position,
  so PostgreSQL no longer drops the replication connection after
  `wal_sender_timeout`. A standby write that cannot complete within 10s now
  fails instead of blocking the reader and shutdown.
```

```bash
git add internal/wal test/integration/18_standby_ack_test.go CHANGELOG.md
git commit -m "fix(wal): keep sending standby updates while delivery is blocked"
```

---

### Task 3: Удалить нагрузочную, деплойную и процессную обвязку

**Files (delete):**
- `testbench/` (целиком), `cmd/writer/`, `cmd/loadgen/`, `internal/writer/`, `test/perf/`
- `internal/app/initialize_bench_test.go`, `internal/router/index_bench_test.go`, `internal/router/router_bench_test.go`, `internal/metrics/registry_bench_test.go`
- `deploy/` (целиком: `k8s/`, `prometheus/`, `scripts/`, `POSTGRES-SETUP.md` — содержимое понадобится в Task 9, брать через `git show <base>:deploy/POSTGRES-SETUP.md`)
- `docker-compose.dev.yaml`
- `test/integration/06_slow_consumer_test.go`, `test/integration/12_sec01_slow_receiver_test.go` (оба `t.Skip`) и опция `WithWriteTimeout` в `test/integration/harness.go`, если больше не используется
- `.github/workflows/testbench-smoke.yml`, `bench-regression.yml`, `flake-detect.yml`, `coverage.yml`
- `scripts/bench.sh`, `scripts/bench-regression-parse.sh`, `scripts/perf-gate-regen.sh`, `scripts/coverage-gate.sh`, `scripts/coverage-gate-test.sh`, `scripts/lint-comment-tokens.sh`, `scripts/lint-comment-tokens_test.sh`, `scripts/comment-density.sh`, `scripts/comment-density_test.sh`, `scripts/testdata/` (если её используют только удаляемые скрипты — проверить `grep -rn testdata scripts`)

**Files (modify):**
- `Makefile` — удалить цели `k8s-deploy`, `load-test`, `perf-gate-baseline`, `wal-stress`, `lint-comments`, `lint-comments-test`, `comment-density`, `comment-density-test` и их записи в `.PHONY`
- `.github/workflows/checks.yml` — удалить job `comment-hygiene`; поправить шапку-комментарий со списком job
- `.golangci.yml` — удалить правило исключения `^(cmd/writer|cmd/loadgen|internal/writer)/`
- `.gitignore` — удалить `/writer`, `cmd/writer/writer` и строки про testbench/loadgen, если есть
- `docker-compose.yml` (корневой) — удалить комментарий-ссылку на `testbench/docker-compose.yml`
- `go.mod`/`go.sum` — `go mod tidy` (ожидаемо уходят `golang.org/x/time`, `gopkg.in/yaml.v3`)
- `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: дерево без перечисленного; Task 4–9 на это опираются (больше нет `internal/writer`, который импортирует koanf; нет дашбордов/алертов с метриками).

- [ ] **Step 1: Delete**

```bash
git rm -r -q testbench cmd/writer cmd/loadgen internal/writer test/perf deploy docker-compose.dev.yaml \
  internal/app/initialize_bench_test.go internal/router/index_bench_test.go internal/router/router_bench_test.go internal/metrics/registry_bench_test.go \
  test/integration/06_slow_consumer_test.go test/integration/12_sec01_slow_receiver_test.go \
  .github/workflows/testbench-smoke.yml .github/workflows/bench-regression.yml .github/workflows/flake-detect.yml .github/workflows/coverage.yml \
  scripts/bench.sh scripts/bench-regression-parse.sh scripts/perf-gate-regen.sh scripts/coverage-gate.sh scripts/coverage-gate-test.sh \
  scripts/lint-comment-tokens.sh scripts/lint-comment-tokens_test.sh scripts/comment-density.sh scripts/comment-density_test.sh
grep -rn "testdata" scripts/ || git rm -r -q scripts/testdata
```

- [ ] **Step 2: Fix references**

Применить правки из списка «Files (modify)». Затем проверить, что ссылок не осталось (кроме `CHANGELOG.md` и `docs/` — документацию переписывает Task 9):

```bash
grep -rnE "testbench|cmd/writer|cmd/loadgen|internal/writer|test/perf|deploy/(k8s|prometheus)|docker-compose\.dev|lint-comment-tokens|comment-density|coverage-gate|perf-gate|bench-regression|flake-detect" \
  --exclude-dir=.git --exclude-dir=docs --exclude=CHANGELOG.md --exclude=TODO.local.md . 
```

Expected: пусто (README/llm.txt/spec/INVARIANTS могут ещё содержать ссылки — их удаляет/переписывает Task 9; если grep показывает только их, это допустимо — перечислите в отчёте).

`WithWriteTimeout`: `grep -rn WithWriteTimeout test/` — если вызовов нет, удалить опцию и поле `writeTimeout` в `spawnConfig` вместе с `writeTimeoutLine`.

- [ ] **Step 3: Tidy and verify**

```bash
go mod tidy
git diff go.mod
go vet ./... && go vet -tags=integration ./...
go test -race ./...
$(go env GOPATH)/bin/golangci-lint run ./... && $(go env GOPATH)/bin/golangci-lint run --build-tags=integration ./...
$(go env GOPATH)/bin/actionlint .github/workflows/*.yml
make -n test lint vet build docker check-changelog-date >/dev/null
```

Expected: всё зелёное; в `go.mod` нет `golang.org/x/time`, `gopkg.in/yaml.v3`.

- [ ] **Step 4: Changelog + commit**

`### Removed`:

```markdown
- **Load-testing and deployment scaffolding.** `testbench/`, `cmd/writer`,
  `cmd/loadgen`, Go benchmarks, the perf gate, Kubernetes manifests,
  Prometheus rule files and `docker-compose.dev.yaml` are gone, along with
  the `testbench-smoke`, `bench-regression`, `flake-detect` and `coverage`
  workflows and the comment-hygiene scripts. A runnable demo lives in the
  separate `walera-demo` repository.
```

```bash
git add -A
git commit -m "chore: remove load-testing, deployment and process scaffolding"
```

---

### Task 4: Пять метрик

**Files:**
- Modify: `internal/metrics/registry.go`, `internal/metrics/registry_test.go`
- Modify (удалить вызовы удалённых метрик): `internal/router/router.go`, `internal/sse/stream.go`, `internal/auth/client.go`, `internal/auth/subscriber.go`, `internal/limits/limits.go`, `internal/wal/reader.go`, `internal/app/runnables.go`, `internal/app/runnables_test.go`, `internal/health/server_test.go`
- Delete: `internal/wal/lag_sampler.go`, `internal/wal/lag_sampler_test.go`
- Modify: `internal/wal/config.go` (поле `LagSampleInterval`, дефолт, валидация), `internal/metrics/config.go` (весь `SampleInterval`), `internal/app/config.go` (загрузка `metrics`), `config.yaml`, `test/integration/harness.go` (строки `lag_sample_interval`, `metrics:`), тесты конфига, которые на них ссылаются
- Modify (интеграция): `test/integration/07_restart_resume_test.go`, `13_sec02_oversized_request_id_test.go`, `15_large_tx_test.go`, `16_tx_scoped_delivery_test.go`
- Test: `internal/sse/stream_test.go`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `metrics.New() *Registry`, `(*Registry).Gatherer()`.
- Produces: `*metrics.Registry` ровно с методами `SubscribersActive(kind string) prometheus.Gauge`, `EventsSent(kind string) prometheus.Counter`, `SubscriberDisconnects(reason string) prometheus.Counter`, `AuthRequests(result string) prometheus.Counter`, `PGConnectionStatus() prometheus.Gauge`, `Gatherer() prometheus.Gatherer`. Пакеты `metrics.Config`/`ApplyDefaults`/`LoadConfig` исчезают.

Удаляемые серии (и их аксессоры): `walera_tx_dropped_total` (`TxDropped`), `walera_route_lookup_duration_seconds` (`RouteLookupDuration`), `walera_slow_client_drops_total` (`SlowClientDrops`), `walera_auth_request_duration_seconds` (`AuthRequestDuration`), `walera_auth_refresh_total` (`AuthRefresh`), `walera_limit_rejected_total` (`LimitRejected`), `walera_pg_reconnects_total` (`PGReconnects`), `walera_wal_standby_ack_failures_total` (`WALStandbyACKFailures`), `walera_wal_lsn_lag_bytes` (`WALLSNLagBytes`), `walera_wal_tx_size_changes` (`WALTxSizeChanges`), `walera_wal_decode_duration_seconds` (`WALDecodeDuration`), `walera_routing_fan_out` (`RoutingFanOut`), `walera_tx_fan_out_work` (`TxFanOutWork`), `walera_co_tx_beyond_anchor_total` (`CoBeyondAnchorTotal`), `walera_routing_index_size` (`RoutingIndexSize`), `walera_subscriber_queue_depth`, `walera_subscriber_lifetime_seconds` (`SubscriberLifetime`). Логи при дропах (`log.Warn` в router/sse/auth/wal) остаются — это теперь единственный сигнал по ним.

- [ ] **Step 1: Write the failing inventory test**

Заменить в `internal/metrics/registry_test.go` тесты-инвентари (`TestRegistry_GatherIncludesAllMetrics`, `TestRegistry_OBS01Inventory`, тесты удаляемых метрик) одним:

```go
func TestRegistry_ExactlyFiveFamilies(t *testing.T) {
	r := New()
	r.SubscribersActive("exact").Set(1)
	r.EventsSent("exact").Inc()
	r.SubscriberDisconnects("client_closed").Inc()
	r.AuthRequests("ok").Inc()
	r.PGConnectionStatus().Set(1)
	fams, err := r.Gatherer().Gather()
	if err != nil {
		t.Fatal(err)
	}
	var walera []string
	for _, f := range fams {
		if strings.HasPrefix(f.GetName(), "walera_") {
			walera = append(walera, f.GetName())
		}
	}
	sort.Strings(walera)
	want := []string{
		"walera_auth_requests_total",
		"walera_events_sent_total",
		"walera_pg_connection_status",
		"walera_subscriber_disconnects_total",
		"walera_subscribers_active",
	}
	if !slices.Equal(walera, want) {
		t.Fatalf("walera families = %v; want %v", walera, want)
	}
}
```

Тест `TestRegistry_DisconnectsShutdownLabelPreTouched` оставить.

- [ ] **Step 2: Run to see it fail**

Run: `go test ./internal/metrics/ -run TestRegistry_ExactlyFiveFamilies -v`
Expected: FAIL — в списке 21 семейство.

- [ ] **Step 3: Shrink the registry**

В `internal/metrics/registry.go` оставить только поля/конструкторы/аксессоры пяти метрик и pre-touch `subscriberDisconnects` по причинам `slow_consumer|tx_too_large|client_closed|shutdown`; удалить `newWALMetrics`/`newRouterMetrics`/`newLimitsMetrics` целиком, кроме `pgConnectionStatus` (перенести его в оставшийся конструктор). Удалить `internal/metrics/config.go` и `config_test.go`.

Затем удалить вызовы по списку из «Удаляемые серии» (компилятор покажет все места): router (`TxDropped`, `RouteLookupDuration`, `RoutingFanOut`, `TxFanOutWork`, `CoBeyondAnchorTotal`), sse/stream (`SlowClientDrops`, `SubscriberLifetime` и переменная `connectedAt`, если больше не нужна), auth/client (`AuthRequestDuration` и `start`/`defer`, если больше не нужны), auth/subscriber (`AuthRefresh` и `recordRefreshResult`/`refreshResultLabel`, если больше не используются — `refreshResultLabel` используется в логе: оставить), limits (`LimitRejected` и pre-touch), wal/reader (`PGReconnects`, `WALStandbyACKFailures` — лог warn остаётся, `WALTxSizeChanges`, `WALDecodeDuration` и `prometheus.NewTimer`).

Удалить `internal/wal/lag_sampler.go` и его тест; в `internal/app/runnables.go` — runnables `wal-lag-sampler` и `metrics-sampler`; в `internal/wal/config.go` — `LagSampleInterval` (поле, дефолт, проверка); в `internal/app/config.go` — `Metrics metrics.Config`, `metrics.ApplyDefaults`, `metrics.LoadConfig`; `config.yaml`/`test/integration/harness.go` — ключи `lag_sample_interval` и блок `metrics:`; поправить тесты конфига (`internal/app/config_test.go`, `internal/wal/config_test.go`, `internal/app/app_singleton_test.go`, `runnables_test.go`) — удалить кейсы про удалённые ключи. Важно: `rejectRemovedKeys` должен теперь отклонять `wal.lag_sample_interval` и `metrics.` — добавить их в `removedConfigKeys` (до Task 7 загрузчик ещё koanf).

В `internal/health/server_test.go` убрать вызовы `LimitRejected` и неиспользуемое поле `stubReader.calls` (последнее — мелочь из прошлого ревью).

- [ ] **Step 4: Disconnect reason on client cancel (unit)**

В `internal/sse/stream_test.go` добавить тест, который пиннит причину отключения через оставшуюся метрику:

```go
// A client that goes away is counted as client_closed, not left unlabelled.
func TestStream_RequestCancelCountsClientClosed(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	sub := router.NewSubscriber(router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: "users", PK: "42"},
		router.SubscriberDeps{Parent: context.Background()})
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil).WithContext(ctx)
	done := make(chan struct{})
	go func() { defer close(done); kit.h.stream(httptest.NewRecorder(), r, sub, nil) }()
	kit.bc.firstSub(t)
	cancel()
	<-done
	if got := sub.Reason(); got != "client_closed" {
		t.Fatalf("Reason = %q; want client_closed", got)
	}
	if v := testutil.ToFloat64(kit.h.metrics.SubscriberDisconnects("client_closed")); v != 1 {
		t.Fatalf("disconnects{client_closed} = %v; want 1", v)
	}
}
```

(`testutil` — `github.com/prometheus/client_golang/prometheus/testutil`; поле метрик хендлера — использовать фактическое имя, сейчас `h.metrics`; `httptest.ResponseRecorder` не поддерживает `SetWriteDeadline` — это допустимо, `stream` игнорирует ошибку.) Заодно: в `internal/sse/stream.go` удалить комментарий-дубль над второй проверкой `Done` (`// Frames queued before a drop may carry a revoked grant.`), а `TestStream_RegistersBeforeFirstFlush` переименовать в `TestStream_RegistersBeforeFirstWrite`.

- [ ] **Step 5: Integration tests off removed metrics**

- `15_large_tx_test.go`: удалить ожидание `walera_tx_dropped_total{reason="tx_too_large"}` (`scrapeMetric`/`waitForMetric` и baseline); проверка остаётся по кадру `error` с `tx_too_large`. В цикле ожидания добавить `default:` в `switch ev.Type` — игнорировать прочие события явно (мелочь из ревью).
- `16_tx_scoped_delivery_test.go`: удалить baseline/assert `walera_tx_dropped_total{reason="multi_root"}`; поведение пиннится существующей проверкой отсутствия события (если её нет — `expectNoTxWithin(t, events, errCh, 3*time.Second)` из `helpers_test.go`).
- `13_sec02_oversized_request_id_test.go`: удалить проверки `walera_limit_rejected_total{…}`; коды ответа 429/503 остаются.
- `07_restart_resume_test.go` (заодно лечим нестабильность):
  - общий `ctx` — 120 s вместо 60 s;
  - вместо роста `walera_pg_reconnects_total` ждать `walera_pg_connection_status == 0` через `waitForMetric` (до 30 s, фатально), затем — строку `reconnect` в `h.Binary.Stderr()` (опрос каждые 200 ms до 30 s, фатально);
  - фиксированное окно `select … time.After(500*time.Millisecond)` на «нет событий» заменить на `expectNoTxWithin(t, events, errCh, 2*time.Second)` для каждого подписчика;
  - остальное (≥3 `walera_subscribers_active{type="exact"}`) — без изменений.

Run: `make testbin/cdc-sse && go test -tags=integration -race -count=1 -timeout 30m ./test/integration/...` — PASS; затем `go test -tags=integration -race -count=10 -timeout 30m -run Test07 ./test/integration/...` — 10/10.

- [ ] **Step 6: Verify**

```bash
go vet ./... && go vet -tags=integration ./... && go test -race ./...
$(go env GOPATH)/bin/golangci-lint run ./... && $(go env GOPATH)/bin/golangci-lint run --build-tags=integration ./...
grep -rnE "walera_(tx_dropped|route_lookup|slow_client|auth_request_duration|auth_refresh|limit_rejected|pg_reconnects|wal_standby|wal_lsn_lag|wal_tx_size|wal_decode|routing_|tx_fan_out|co_tx|subscriber_queue|subscriber_lifetime)" --include=*.go .
```

Expected: зелёное; grep пуст.

- [ ] **Step 7: Changelog + commit**

`### Removed`:

```markdown
- **Metrics trimmed to five.** Kept: `walera_subscribers_active`,
  `walera_events_sent_total`, `walera_subscriber_disconnects_total{reason}`,
  `walera_auth_requests_total{result}`, `walera_pg_connection_status`.
  All other `walera_*` series, the WAL lag sampler and the routing-index
  sampler are removed; drops and reconnects are visible in the logs.
  Config keys `wal.lag_sample_interval` and `metrics.*` are rejected.
```

```bash
git add -A
git commit -m "refactor(metrics): keep five metrics, drop samplers"
```

---

### Task 5: Индекс подписчиков — map под RWMutex

**Files:**
- Modify: `internal/router/index.go`
- Test: `internal/router/index_test.go` (удалить `TestIndex_ShardDistribution`)
- Modify: `go.mod`/`go.sum` (`go mod tidy` уберёт `github.com/cespare/xxhash/v2`)

**Interfaces:**
- Consumes: —
- Produces: тот же API `newIndex() *index`, `Add(key string, sub *Subscriber)`, `Remove(key string, sub *Subscriber)`, `Lookup(key string) []*Subscriber`, `Len() int`, `Snapshot() []*Subscriber` (вызывается из `internal/router/router.go`).

- [ ] **Step 1: Replace the implementation**

`internal/router/index.go` целиком:

```go
package router

import "sync"

// index maps an exact channel key to its subscribers. One RWMutex is enough
// at Walera's scale; lookups take the read lock and copy out.
type index struct {
	mu   sync.RWMutex
	subs map[string]map[*Subscriber]struct{}
}

func newIndex() *index {
	return &index{subs: make(map[string]map[*Subscriber]struct{})}
}

func (idx *index) Add(key string, sub *Subscriber) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	if idx.subs[key] == nil {
		idx.subs[key] = make(map[*Subscriber]struct{}, 1)
	}
	idx.subs[key][sub] = struct{}{}
}

func (idx *index) Remove(key string, sub *Subscriber) {
	idx.mu.Lock()
	defer idx.mu.Unlock()
	subs := idx.subs[key]
	delete(subs, sub)
	if len(subs) == 0 {
		delete(idx.subs, key)
	}
}

func (idx *index) Lookup(key string) []*Subscriber {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	subs := idx.subs[key]
	if len(subs) == 0 {
		return nil
	}
	out := make([]*Subscriber, 0, len(subs))
	for sub := range subs {
		out = append(out, sub)
	}
	return out
}

func (idx *index) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	total := 0
	for _, subs := range idx.subs {
		total += len(subs)
	}
	return total
}

func (idx *index) Snapshot() []*Subscriber {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	var out []*Subscriber
	for _, subs := range idx.subs {
		for sub := range subs {
			out = append(out, sub)
		}
	}
	return out
}
```

- [ ] **Step 2: Tests**

Удалить `TestIndex_ShardDistribution` (специфичен для шардов). Остальные (`TestIndex_AddLookupRemove*`, `TestIndexConcurrent`) остаются без изменений. Добавить кейс: `Remove` для неизвестного ключа не паникует и не создаёт запись (`idx.Remove("missing", sub); if idx.Len() != 0 || len(idx.subs) != 0 { t.Fatal(...) }`).

Run: `go test -race -count=5 ./internal/router/`
Expected: PASS.

- [ ] **Step 3: Tidy, verify, commit**

```bash
go mod tidy && ! grep -q xxhash go.mod
go vet ./... && go test -race ./... && $(go env GOPATH)/bin/golangci-lint run ./...
```

`### Changed`: `- **Subscriber index is a plain map under one RWMutex.** The xxhash-sharded index and the github.com/cespare/xxhash/v2 dependency are gone.`

```bash
git add -A
git commit -m "refactor(router): plain map subscriber index"
```

---

### Task 6: Удалить pprof и trusted proxies

**Files:**
- Modify: `internal/app/runnables.go` (`newPProfHTTPServer`, runnable `pprof-server`), `internal/app/initialize.go`, `internal/app/app.go` (`PProfServer`), `internal/app/lifecycle.go` (шаг остановки pprof), `internal/app/config.go` (`HTTPConfig.PProfAddr`, дефолт, валидация, `WALERA_PPROF_ALLOW_PUBLIC`)
- Modify: `internal/limits/config.go`, `internal/limits/limits.go` (`TrustedProxies`, `IsTrustedProxy`), `internal/sse/auth.go` (`clientIP`)
- Tests: `internal/app/runnables_test.go`, `lifecycle_test.go`, `config_test.go` (кейсы `PProfAddr`), `app_singleton_test.go`; `internal/limits/*_test.go`; `internal/sse/handler_test.go` (`TestClientIP_*`, `mkClientIPHandler`)
- Modify: `config.yaml`, `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: `(*Handler).clientIP(r *http.Request) string` возвращает хост из `r.RemoteAddr` (или `r.RemoteAddr` целиком, если без порта); `limits.Config` без `TrustedProxies`; `App` без `PProfServer`.

- [ ] **Step 1: Replace clientIP test first**

В `internal/sse/handler_test.go` удалить все `TestClientIP_*` и `mkClientIPHandler`; добавить:

```go
func TestClientIP_IsPeerAddressAndIgnoresXFF(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/1", nil)
	r.RemoteAddr = "203.0.113.7:51234"
	r.Header.Set("X-Forwarded-For", "198.51.100.1")
	if got := kit.h.clientIP(r); got != "203.0.113.7" {
		t.Fatalf("clientIP = %q; want 203.0.113.7", got)
	}
}
```

Run: `go test ./internal/sse/ -run TestClientIP -v` → FAIL (`limits.Config` в `newTestHandler` пока с прокси — тест может пройти или нет; главное, что после Step 2 он проходит).

- [ ] **Step 2: Implement removals**

```go
// clientIP is the TCP peer; it only feeds the client_ip log field.
func (h *Handler) clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
```

Удалить `TrustedProxies`/`IsTrustedProxy`/разбор CIDR из `internal/limits`, ключ `limits.trusted_proxies` (дефолт, валидация), pprof целиком (сервер, runnable, шаг shutdown, поле, конфиг, env `WALERA_PPROF_ALLOW_PUBLIC`, тесты). Добавить `http.pprof_addr` и `limits.trusted_proxies` в `removedConfigKeys`.

- [ ] **Step 3: Verify, changelog, commit**

```bash
go vet ./... && go test -race ./... && $(go env GOPATH)/bin/golangci-lint run ./...
grep -rnE "pprof|TrustedProx|trusted_proxies" --include=*.go . 
```

Expected: зелёное; grep пуст (кроме `removedConfigKeys`).

`### Removed`:

```markdown
- **pprof listener and trusted-proxy parsing.** `http.pprof_addr`,
  `WALERA_PPROF_ALLOW_PUBLIC` and `limits.trusted_proxies` are gone; the
  `client_ip` log field is now the TCP peer address.
```

```bash
git add -A
git commit -m "refactor: remove pprof listener and trusted-proxy parsing"
```

---

### Task 7: Конфиг только через env; упрощённый bootstrap

**Files:**
- Create: `internal/app/env.go`, `internal/app/env_test.go`
- Delete: `internal/config/config.go`, `config_test.go`, `dev_guard.go`, `dev_guard_test.go`, `dev_guard_dev.go`, `dev_guard_dev_test.go` (остаются `format.go`/`format_test.go` с `FormatError`, `RedactDSN`); `config.yaml`
- Modify: `internal/app/config.go` (удалить koanf-загрузку, `LoadAppConfig`, `removedConfigKeys`, `ShutdownDeadline`-хук и mapstructure; оставить `AppConfig`, `validateHTTP`, `canonicalOrigin`), `internal/{wal,auth,limits,router,health}/config.go` (убрать koanf-теги, `ApplyDefaults`, `LoadConfig`; добавить `Defaults() Config`; `Validate()` оставить), их `config_test.go` (переписать на `Defaults()`+`Validate()`)
- Modify: `internal/auth/config.go` (`AllowPlaintext bool` вместо `os.Getenv("WALERA_AUTH_ALLOW_PLAINTEXT")`)
- Modify: `internal/wal/config.go` (удалить `Bootstrap.Mode`, `Bootstrap.CreateRoles`, `SlotNamePrefix`/`SlotHeadroomMin`/`NaiveTimestampAssumeUTC`/`Reconnect` как настраиваемые — сделать константами в `Defaults()`; `Tables []string` поднять на уровень `Config.Tables`)
- Modify: `internal/app/bootstrap.go`, `bootstrap_test.go`, `prepare_db.go` (без режимов и `bootstrapEnsureRole`)
- Modify: `cmd/cdc-sse/main.go` (без `--config`), `cmd/cdc-sse/bootstrap.go` (`WALERA_ADDR`)
- Modify: `test/integration/harness.go` (env вместо YAML; префикс слота `walera`), `test/integration/pg.go` (без изменений API), `test/integration/13_sec02_oversized_request_id_test.go` (per-user = 10), `15_large_tx_test.go` (10050 строк; префикс `walera_`), `14_slot_lifecycle_test.go` (префиксы/опции bootstrap)
- Modify: `Makefile` (удалить `config-check`; `deps-check` — убрать правило про `internal/config`, если оно ссылается на удалённые файлы), `.github/workflows/checks.yml` (удалить job `config-check`), `Dockerfile` (если есть `CMD`/`--config`), `docker-compose.yml` (переписать на новые env, без монтирования `config.yaml`)
- Modify: `go.mod`/`go.sum` (`go mod tidy` уберёт koanf и mapstructure), `CHANGELOG.md`

**Interfaces:**
- Consumes: `config.FormatError(key, value, problem, remediation string) error`, `config.RedactDSN(string) string`, `wal.DeriveDSNs(base string) (walconn.AdminDSN, walconn.ReplicationDSN, error)`, `publicationHasSchemas`/`errSchemaPublication`/`lookupPublication`/`errAllTablesPublication`/`bootstrapVerifyTables`/`checkSlotHeadroom` (`internal/app/bootstrap.go`).
- Produces: `app.LoadFromEnv(environ []string) (*AppConfig, error)` (вызов в main: `app.LoadFromEnv(os.Environ())`); `bootstrapConfig{PublicationName string; Tables []string}`; пакетные `Defaults() Config` в `wal`, `auth`, `limits`, `router`, `health`.

Константы (значения сохраняются, становятся неизменяемыми): slot prefix `walera`, headroom `2`, naive timestamps → UTC `true`, reconnect reset `60s`, max payload `10 MiB`, write timeout `5s`, max header `16 KiB`, h2c `true`, auth request timeout `2s`, per-user connections `10`, exact buffer `64`, wildcard buffer `512`, max changes per tx `10000`, heartbeat `15s`, readyz probe `5s`, shutdown deadline `10s`.

- [ ] **Step 1: Write the failing env tests**

`internal/app/env_test.go`:

```go
package app

import (
	"strings"
	"testing"
	"time"
)

func baseEnv() []string {
	return []string{
		"WALERA_DATABASE_URL=postgres://walera:s3cret@db:5432/app",
		"WALERA_AUTH_URL=https://auth.example",
		"PATH=/usr/bin", // non-WALERA vars are ignored
	}
}

func TestLoadFromEnv_Defaults(t *testing.T) {
	cfg, err := LoadFromEnv(baseEnv())
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.HTTP.Addr != ":8080" || string(cfg.WAL.PublicationName) != "walera_pub" ||
		cfg.Auth.DefaultTTLSeconds != 60 || cfg.Limits.GlobalConcurrent != 50000 ||
		cfg.Log.Level != "info" || cfg.Auth.AllowPlaintext || len(cfg.WAL.Tables) != 0 {
		t.Fatalf("defaults wrong: %+v", cfg)
	}
}

func TestLoadFromEnv_AllVars(t *testing.T) {
	env := append(baseEnv(),
		"WALERA_ADDR=127.0.0.1:9000",
		"WALERA_PUBLICATION=pub_x",
		"WALERA_TABLES=public.a, public.b",
		"WALERA_CORS_ORIGINS=https://App.example",
		"WALERA_AUTH_TTL=90s",
		"WALERA_AUTH_FORWARD_COOKIES=session",
		"WALERA_AUTH_FORWARD_HEADERS=X-Api-Key",
		"WALERA_AUTH_ALLOW_PLAINTEXT=1",
		"WALERA_MAX_CONNECTIONS=200",
		"WALERA_LOG_LEVEL=debug",
	)
	env[1] = "WALERA_AUTH_URL=http://auth:8000"
	cfg, err := LoadFromEnv(env)
	if err != nil {
		t.Fatalf("LoadFromEnv: %v", err)
	}
	if cfg.HTTP.Addr != "127.0.0.1:9000" || string(cfg.WAL.PublicationName) != "pub_x" ||
		strings.Join(cfg.WAL.Tables, ",") != "public.a,public.b" ||
		strings.Join(cfg.HTTP.CORSOrigins, ",") != "https://app.example" ||
		cfg.Auth.DefaultTTLSeconds != 90 || cfg.Auth.ForwardedCookies[0] != "session" ||
		cfg.Auth.ForwardedHeaders[0] != "X-Api-Key" || !cfg.Auth.AllowPlaintext ||
		cfg.Limits.GlobalConcurrent != 200 || cfg.Log.Level != "debug" {
		t.Fatalf("parsed wrong: %+v", cfg)
	}
}

func TestLoadFromEnv_RequiredVars(t *testing.T) {
	_, err := LoadFromEnv([]string{"PATH=/usr/bin"})
	if err == nil || !strings.Contains(err.Error(), "WALERA_DATABASE_URL") || !strings.Contains(err.Error(), "WALERA_AUTH_URL") {
		t.Fatalf("err = %v; want both required vars named", err)
	}
}

// v3 names and config-file era variables must fail loudly, naming the
// variable but never echoing its value.
func TestLoadFromEnv_RejectsUnknownVars(t *testing.T) {
	env := append(baseEnv(),
		"WALERA_AUTH_BACKEND_URL=https://old.example",
		"WALERA_WAL_PUBLICATION_NAME=secretish-value",
	)
	_, err := LoadFromEnv(env)
	if err == nil {
		t.Fatal("err = nil; want unknown-variable rejection")
	}
	for _, name := range []string{"WALERA_AUTH_BACKEND_URL", "WALERA_WAL_PUBLICATION_NAME"} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("err = %q; want it to name %s", err, name)
		}
	}
	if strings.Contains(err.Error(), "secretish-value") {
		t.Errorf("err = %q; must not echo values", err)
	}
}

func TestLoadFromEnv_InvalidDSNRedactsPassword(t *testing.T) {
	env := baseEnv()
	env[0] = "WALERA_DATABASE_URL=postgres://walera:s3cret@db:notaport/app"
	_, err := LoadFromEnv(env)
	if err == nil {
		t.Fatal("err = nil; want invalid DSN error")
	}
	if strings.Contains(err.Error(), "s3cret") {
		t.Fatalf("err = %q; leaked password", err)
	}
}

func TestLoadFromEnv_InvalidValues(t *testing.T) {
	for _, kv := range []string{
		"WALERA_AUTH_TTL=500ms",
		"WALERA_AUTH_TTL=soon",
		"WALERA_MAX_CONNECTIONS=0",
		"WALERA_MAX_CONNECTIONS=many",
		"WALERA_LOG_LEVEL=loud",
		"WALERA_AUTH_ALLOW_PLAINTEXT=yes",
		"WALERA_TABLES=no_schema",
		"WALERA_CORS_ORIGINS=not a url",
		"WALERA_ADDR=:99999",
		"WALERA_AUTH_FORWARD_HEADERS=Origin",
	} {
		t.Run(kv, func(t *testing.T) {
			if _, err := LoadFromEnv(append(baseEnv(), kv)); err == nil {
				t.Fatalf("%s: err = nil; want validation error", kv)
			}
		})
	}
}

func TestLoadFromEnv_TTLWholeSeconds(t *testing.T) {
	cfg, err := LoadFromEnv(append(baseEnv(), "WALERA_AUTH_TTL=2m"))
	if err != nil || time.Duration(cfg.Auth.DefaultTTLSeconds)*time.Second != 2*time.Minute {
		t.Fatalf("ttl = %v, %v; want 120s", cfg.Auth.DefaultTTLSeconds, err)
	}
}
```

Run: `go test ./internal/app/ -run TestLoadFromEnv -v` → FAIL (`undefined: LoadFromEnv`).

- [ ] **Step 2: Package defaults**

В каждом из `internal/{wal,auth,limits,router,health}/config.go`: удалить импорт koanf, теги `koanf:"…"`, `ApplyDefaults`, `LoadConfig`; добавить `Defaults()` со значениями из таблицы констант выше и текущих дефолтов. Пример для `router`:

```go
// Defaults are Walera's fixed routing parameters; they are not configurable.
func Defaults() Config {
	return Config{
		ExactBuffer:       64,
		WildcardBuffer:    512,
		MaxChangesPerTx:   10000,
		HeartbeatInterval: 15 * time.Second,
	}
}
```

`auth.Config`: добавить `AllowPlaintext bool`, в `Validate()` заменить `os.Getenv("WALERA_AUTH_ALLOW_PLAINTEXT") != "1"` на `!c.AllowPlaintext`; ремедиация в тексте ошибки — «set WALERA_AUTH_ALLOW_PLAINTEXT=1». Тексты ошибок валидации в пакетах — заменить koanf-ключи (`auth.backend_url`) на env-имена (`WALERA_AUTH_URL`) там, где поле приходит из env.

`wal.Config`: оставить `PostgresDSN`, `ReplicationDSN`, `PublicationName`, `Tables []string` (поднять из `Bootstrap`), `SlotNamePrefix`, `SlotHeadroomMin`, `NaiveTimestampAssumeUTC`, `Reconnect` (заполняются из `Defaults()`); удалить `BootstrapConfig` и проверки режимов. Валидация таблиц (`schema.table`) остаётся.

Переписать `config_test.go` пакетов: каждый — `Defaults().Validate()` возвращает nil + существующие негативные кейсы валидации через изменение полей структуры.

- [ ] **Step 3: `LoadFromEnv`**

`internal/app/env.go`:

```go
package app

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/auth"
	"github.com/walera/walera/internal/config"
	"github.com/walera/walera/internal/health"
	"github.com/walera/walera/internal/limits"
	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

// envVars is every variable Walera reads. Any other WALERA_* variable fails
// startup, so a typo or a v3 name never silently falls back to a default.
var envVars = []string{
	"WALERA_DATABASE_URL", "WALERA_AUTH_URL", "WALERA_ADDR", "WALERA_PUBLICATION",
	"WALERA_TABLES", "WALERA_CORS_ORIGINS", "WALERA_AUTH_TTL",
	"WALERA_AUTH_FORWARD_COOKIES", "WALERA_AUTH_FORWARD_HEADERS",
	"WALERA_AUTH_ALLOW_PLAINTEXT", "WALERA_MAX_CONNECTIONS", "WALERA_LOG_LEVEL",
}

// LoadFromEnv builds the configuration from environ (os.Environ() format).
// Errors name variables, never their values.
func LoadFromEnv(environ []string) (*AppConfig, error) {
	env := map[string]string{}
	var unknown []string
	for _, kv := range environ {
		k, v, _ := strings.Cut(kv, "=")
		if !strings.HasPrefix(k, "WALERA_") {
			continue
		}
		if !slices.Contains(envVars, k) {
			unknown = append(unknown, k)
			continue
		}
		env[k] = v
	}
	var errs []error
	if len(unknown) > 0 {
		slices.Sort(unknown)
		errs = append(errs, fmt.Errorf("unknown environment variables: %s (see README for the v4 list)", strings.Join(unknown, ", ")))
	}

	cfg := &AppConfig{
		Log:      LogConfig{Level: "info"},
		HTTP:     defaultHTTP(),
		WAL:      wal.Defaults(),
		Auth:     auth.Defaults(),
		Limits:   limits.Defaults(),
		Router:   router.Defaults(),
		Health:   health.Defaults(),
		Shutdown: ShutdownConfig{Deadline: ShutdownDeadline(10 * time.Second)},
	}

	for _, req := range []string{"WALERA_DATABASE_URL", "WALERA_AUTH_URL"} {
		if env[req] == "" {
			errs = append(errs, fmt.Errorf("%s is required", req))
		}
	}
	if v := env["WALERA_DATABASE_URL"]; v != "" {
		admin, repl, err := wal.DeriveDSNs(v) // already redacts the DSN in its errors
		if err != nil {
			errs = append(errs, err)
		}
		cfg.WAL.PostgresDSN, cfg.WAL.ReplicationDSN = admin, repl
	}
	cfg.Auth.BackendURL = env["WALERA_AUTH_URL"]
	if v, ok := env["WALERA_ADDR"]; ok && v != "" {
		cfg.HTTP.Addr = v
	}
	if v, ok := env["WALERA_PUBLICATION"]; ok && v != "" {
		cfg.WAL.PublicationName = wal.PublicationName(v)
	}
	cfg.WAL.Tables = splitList(env["WALERA_TABLES"])
	cfg.HTTP.CORSOrigins = splitList(env["WALERA_CORS_ORIGINS"])
	cfg.Auth.ForwardedCookies = splitList(env["WALERA_AUTH_FORWARD_COOKIES"])
	cfg.Auth.ForwardedHeaders = splitList(env["WALERA_AUTH_FORWARD_HEADERS"])

	if v := env["WALERA_AUTH_TTL"]; v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second {
			errs = append(errs, config.FormatError("WALERA_AUTH_TTL", "", "must be a duration >= 1s", "e.g. 60s or 5m"))
		} else {
			cfg.Auth.DefaultTTLSeconds = int(d / time.Second)
		}
	}
	switch env["WALERA_AUTH_ALLOW_PLAINTEXT"] {
	case "", "0":
	case "1":
		cfg.Auth.AllowPlaintext = true
	default:
		errs = append(errs, config.FormatError("WALERA_AUTH_ALLOW_PLAINTEXT", "", "must be 0 or 1", ""))
	}
	if v := env["WALERA_MAX_CONNECTIONS"]; v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n <= 0 {
			errs = append(errs, config.FormatError("WALERA_MAX_CONNECTIONS", "", "must be a positive integer", ""))
		} else {
			cfg.Limits.GlobalConcurrent = n
		}
	}
	if v := env["WALERA_LOG_LEVEL"]; v != "" {
		if _, err := zerolog.ParseLevel(v); err != nil {
			errs = append(errs, config.FormatError("WALERA_LOG_LEVEL", "", "must be debug, info, warn or error", ""))
		} else {
			cfg.Log.Level = v
		}
	}

	for _, v := range []interface{ Validate() error }{cfg.WAL, cfg.Auth, cfg.Limits, cfg.Router, cfg.Health} {
		if err := v.Validate(); err != nil {
			errs = append(errs, err)
		}
	}
	if err := validateHTTP(&cfg.HTTP); err != nil { // also canonicalizes CORS origins
		errs = append(errs, err)
	}
	if len(errs) > 0 {
		return nil, fmt.Errorf("config: %w", errors.Join(errs...))
	}
	return cfg, nil
}

func splitList(s string) []string {
	var out []string
	for _, p := range strings.Split(s, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}
```

Адаптировать под фактические сигнатуры: `validateHTTP` сейчас принимает `*HTTPConfig` и, возможно, не канонизирует CORS сам — перенести канонизацию (`config.go`, блок после валидации) внутрь или вызвать её здесь; `Validate()` пакетов может иметь pointer-receiver — тогда передавать `&cfg.WAL` и т.д. `defaultHTTP()` — новая функция в `internal/app/config.go`, возвращающая `HTTPConfig{Addr: ":8080", MaxPayloadBytes: 10 << 20, WriteTimeout: 5 * time.Second, MaxHeaderBytes: 16 << 10, H2CEnabled: true}`. Удалить из `AppConfig` поле `Metrics` (если ещё есть после Task 4) и `LogConfig.DevMode`. Ошибки пакетных `Validate()` — проверить, что ни одна не включает значение секрета (DSN идёт только через `DeriveDSNs` → `RedactDSN`).

`cmd/cdc-sse/main.go`: удалить флаг `config`; `cfg, err := app.LoadFromEnv(os.Environ())`. `cmd/cdc-sse/bootstrap.go`: `os.Getenv("WALERA_ADDR")` вместо `WALERA_HTTP_ADDR`.

Run: `go test ./internal/app/ -run TestLoadFromEnv -v` → PASS.

- [ ] **Step 4: Bootstrap without modes**

Тесты в `internal/app/bootstrap_test.go` — заменить `TestBootstrapPublicationModes`, `TestBootstrapPublication_RejectsAllTables`, `TestBootstrapPublication_RejectsSchemaPublication`, `TestBootstrapEnsureRoleUnit` на:

```go
func TestBootstrapPublication_ExistingIsVerified(t *testing.T) {
	db := &fakeBootstrapDB{
		rowResults: []fakeRow{{values: []any{false}}, {values: []any{150004}}, {values: []any{false}}, {values: []any{2}}},
		queryRows:  []pgx.Rows{&fakeRows{values: [][]any{{"public.a"}}}},
	}
	if err := bootstrapPublication(context.Background(), db, bootstrapConfig{PublicationName: "pub", Tables: []string{"public.a"}}, zerolog.Nop()); err != nil {
		t.Fatalf("existing: %v", err)
	}
	if len(db.execSQL) != 0 {
		t.Fatalf("exec = %#v; want no DDL for an existing publication", db.execSQL)
	}
}

func TestBootstrapPublication_ExistingWithoutTablesListIsVerified(t *testing.T) {
	db := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}, {values: []any{150004}}, {values: []any{false}}, {values: []any{2}}}}
	if err := bootstrapPublication(context.Background(), db, bootstrapConfig{PublicationName: "pub"}, zerolog.Nop()); err != nil {
		t.Fatalf("existing, no WALERA_TABLES: %v", err)
	}
}

func TestBootstrapPublication_MissingIsCreatedFromTables(t *testing.T) {
	db := &fakeBootstrapDB{rowResults: []fakeRow{{err: pgx.ErrNoRows}}}
	if err := bootstrapPublication(context.Background(), db, bootstrapConfig{PublicationName: "pub", Tables: []string{"public.a", "public.b"}}, zerolog.Nop()); err != nil {
		t.Fatalf("create: %v", err)
	}
	if len(db.execSQL) != 1 || !strings.Contains(db.execSQL[0], "CREATE PUBLICATION pub FOR TABLE public.a, public.b") {
		t.Fatalf("exec = %#v", db.execSQL)
	}
}

func TestBootstrapPublication_MissingWithoutTables(t *testing.T) {
	db := &fakeBootstrapDB{rowResults: []fakeRow{{err: pgx.ErrNoRows}}}
	err := bootstrapPublication(context.Background(), db, bootstrapConfig{PublicationName: "pub"}, zerolog.Nop())
	if err == nil || !strings.Contains(err.Error(), "WALERA_TABLES") || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("err = %v; want missing-publication error naming WALERA_TABLES", err)
	}
}

func TestBootstrapPublication_RejectsWideningPublications(t *testing.T) {
	cases := map[string][]fakeRow{
		"FOR ALL TABLES":       {{values: []any{true}}},
		"FOR TABLES IN SCHEMA": {{values: []any{false}}, {values: []any{150004}}, {values: []any{true}}},
	}
	for want, rows := range cases {
		t.Run(want, func(t *testing.T) {
			db := &fakeBootstrapDB{rowResults: rows}
			err := bootstrapPublication(context.Background(), db, bootstrapConfig{PublicationName: "pub", Tables: []string{"public.a"}}, zerolog.Nop())
			if err == nil || !strings.Contains(err.Error(), want) || len(db.execSQL) != 0 {
				t.Fatalf("err = %v, exec = %#v; want %s rejection and no DDL", err, db.execSQL, want)
			}
		})
	}
}
```

`TestPrepareDatabaseUnit`: убрать `Mode`, rowResults — по новой последовательности запросов (prereqs ×3, роль, `puballtables`, версия, схемы, count таблиц, headroom ×2).

Реализация в `internal/app/bootstrap.go`:

```go
type bootstrapConfig struct {
	PublicationName string
	Tables          []string
}

// bootstrapPublication verifies an existing publication or, when it is
// missing and WALERA_TABLES lists tables, creates it FOR TABLE. Publications
// that stream more than a table list are refused.
func bootstrapPublication(ctx context.Context, conn bootstrapDB, cfg bootstrapConfig, logger zerolog.Logger) error {
	exists, allTables, err := lookupPublication(ctx, conn, cfg.PublicationName)
	if err != nil {
		return err
	}
	if !exists {
		if len(cfg.Tables) == 0 {
			return fmt.Errorf(
				"publication %q does not exist — create it (CREATE PUBLICATION %s FOR TABLE <list>) or set WALERA_TABLES so Walera creates it",
				cfg.PublicationName, cfg.PublicationName,
			)
		}
		ddl := fmt.Sprintf(
			"CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')",
			cfg.PublicationName, strings.Join(cfg.Tables, ", "),
		)
		if _, err := conn.Exec(ctx, ddl); err != nil {
			return fmt.Errorf("failed to create publication %q: %w", cfg.PublicationName, err)
		}
		logger.Info().Str("publication", cfg.PublicationName).Int("table_count", len(cfg.Tables)).Msg("publication created")
		return nil
	}
	if allTables {
		return errAllTablesPublication(cfg.PublicationName)
	}
	if hasSchemas, err := publicationHasSchemas(ctx, conn, cfg.PublicationName); err != nil {
		return err
	} else if hasSchemas {
		return errSchemaPublication(cfg.PublicationName)
	}
	var tableCount int
	if err := conn.QueryRow(ctx,
		"SELECT count(*) FROM pg_publication_tables WHERE pubname = $1",
		cfg.PublicationName,
	).Scan(&tableCount); err != nil {
		return fmt.Errorf("publication table count failed (%s): %w", cfg.PublicationName, err)
	}
	if tableCount == 0 {
		return fmt.Errorf("publication %q has no tables — add tables with ALTER PUBLICATION %s ADD TABLE <list>", cfg.PublicationName, cfg.PublicationName)
	}
	logger.Info().Str("publication", cfg.PublicationName).Int("table_count", tableCount).Msg("publication check passed")
	if len(cfg.Tables) > 0 {
		bootstrapVerifyTables(ctx, conn, cfg.PublicationName, cfg.Tables, logger)
	}
	return nil
}
```

Удалить `bootstrapEnsureRole`, `pgRoleNameRe`, `net/url` (если больше не нужны) и их тесты. `prepare_db.go` — передавать `bootstrapConfig{PublicationName: string(cfg.WAL.PublicationName), Tables: cfg.WAL.Tables}`; `ReplicationDSN`/`PostgresDSN` в bootstrap больше не нужны.

Run: `go test -race ./internal/app/` → PASS.

- [ ] **Step 5: Harness and integration tests on env**

`test/integration/harness.go` `SpawnBinary`: удалить генерацию YAML и файл; `cmd := exec.Command(binaryPath())`; `cmd.Env = append(os.Environ(), …)` — но сначала отфильтровать из `os.Environ()` все `WALERA_*` хоста (иначе неизвестная переменная разработчика уронит бинарник). Переменные:

```go
env := []string{
	"WALERA_DATABASE_URL=" + pgDSN,
	"WALERA_AUTH_URL=" + mockAuthURL,
	"WALERA_ADDR=" + addr,
	"WALERA_PUBLICATION=cdc_sse_streamer",
	"WALERA_AUTH_TTL=1s",
	"WALERA_MAX_CONNECTIONS=" + strconv.Itoa(sc.globalConcurrent),
	"WALERA_LOG_LEVEL=debug",
}
if !sc.withoutPlaintextEnv {
	env = append(env, "WALERA_AUTH_ALLOW_PLAINTEXT=1")
}
if len(sc.bootstrapAutoTables) > 0 {
	env = append(env, "WALERA_TABLES="+strings.Join(sc.bootstrapAutoTables, ","))
}
if len(sc.forwardedCookies) > 0 {
	env = append(env, "WALERA_AUTH_FORWARD_COOKIES="+strings.Join(sc.forwardedCookies, ","))
}
if len(sc.forwardedHeaders) > 0 {
	env = append(env, "WALERA_AUTH_FORWARD_HEADERS="+strings.Join(sc.forwardedHeaders, ","))
}
```

Удалить `WithPerUserConcurrent`, поле `perUserConcurrent`, `yamlStringList` (если больше не используется). `NewHarness`: `pg.WaitForActiveSlot(t, "walera", 15*time.Second)`. Тесты:
- `13_sec02_oversized_request_id_test.go`: подтест per-user — открыть 10 соединений, 11-е → 429 (константа 10); `WithGlobalConcurrent` остаётся.
- `15_large_tx_test.go`: `generate_series(1, 10050)` (лимит теперь 10000); префикс слота в `querySlotLag`/`confirmedSlotDrained` — `walera_`.
- `14_slot_lifecycle_test.go`, `14…` и любые `walera_test`: `grep -rn walera_test test/` → заменить на `walera`/`walera_`; `WithBootstrapAuto` → теперь задаёт `WALERA_TABLES`, публикация отсутствует → создаётся (тот же сценарий).
- Тесты, которые полагались на heartbeat `1s` (`grep -rn -i heartbeat test/integration`): если ждут heartbeat-кадр — увеличить ожидание до 20 s или убрать зависимость.

Run: `make testbin/cdc-sse && go test -tags=integration -race -count=1 -timeout 30m ./test/integration/... && go test -tags=integration -race -count=1 ./internal/app/`
Expected: PASS.

- [ ] **Step 6: Remove koanf, files, CI job**

```bash
git rm -q config.yaml internal/config/config.go internal/config/config_test.go internal/config/dev_guard*.go
go mod tidy && ! grep -qE "koanf|mapstructure" go.mod
```

`Makefile`: удалить цель `config-check`; `deps-check`: оставить, проверить, что правило по `internal/config` по-прежнему корректно. `.github/workflows/checks.yml`: удалить job `config-check`. `docker-compose.yml` — переписать как пример быстрого старта (Postgres с `wal_level=logical` + Walera из `ghcr.io/slavnycoder/walera:latest` с `WALERA_DATABASE_URL`, `WALERA_AUTH_URL`, `WALERA_TABLES`, `WALERA_CORS_ORIGINS`; без монтирования `config.yaml`). `Dockerfile`: убрать `--config`/копирование `config.yaml`, если есть. Проверить `docker build -t walera-check .` и `docker compose -f docker-compose.yml config`.

```bash
go vet ./... && go vet -tags=integration ./... && go test -race ./...
$(go env GOPATH)/bin/golangci-lint run ./... && $(go env GOPATH)/bin/golangci-lint run --build-tags=integration ./...
grep -rnE "koanf|LoadAppConfig|removedConfigKeys|bootstrap\.mode|create_roles|WALERA_(HTTP|WAL|PPROF)_" --include=*.go --include=*.yml --include=*.yaml --include=Makefile --include=Dockerfile .
```

Expected: зелёное; grep пуст.

- [ ] **Step 7: Changelog + commit**

`### Changed`:

```markdown
- **Configuration is environment-only.** The config file, `--config`,
  koanf and every tuning key are gone; Walera reads 12 `WALERA_*`
  variables (`WALERA_DATABASE_URL` and `WALERA_AUTH_URL` are required) and
  fails startup on any other `WALERA_*` variable. Renamed:
  `WALERA_AUTH_BACKEND_URL` → `WALERA_AUTH_URL`,
  `WALERA_WAL_PUBLICATION_NAME` → `WALERA_PUBLICATION`,
  `WALERA_WAL_BOOTSTRAP_TABLES` → `WALERA_TABLES`,
  `WALERA_HTTP_CORS_ORIGINS` → `WALERA_CORS_ORIGINS`,
  `WALERA_HTTP_ADDR` → `WALERA_ADDR`.
- **Publication bootstrap has no modes.** An existing publication is
  verified; a missing one is created `FOR TABLE` from `WALERA_TABLES`, or
  startup fails with instructions. `wal.bootstrap.mode` and role creation
  are removed.
```

```bash
git add -A
git commit -m "refactor(config)!: environment-only configuration"
```

---

### Task 8: CI — актуальные actions и lint интеграционного кода

**Files:**
- Modify: `.github/workflows/checks.yml`, `release.yml`, `publish-image.yml`

**Interfaces:**
- Consumes: оставшиеся workflow (`checks`, `release`, `publish-image`).
- Produces: —

Актуальные версии (проверено 2026-10-29 через `gh api repos/<repo>/releases/latest`): `actions/checkout@v7`, `actions/setup-go@v7`, `docker/setup-qemu-action@v4`, `docker/setup-buildx-action@v4`, `docker/login-action@v4`, `docker/metadata-action@v6`, `docker/build-push-action@v7`. Перед правкой перепроверить той же командой; если вышли новые мажоры — брать их.

- [ ] **Step 1: Bump versions**

Заменить все `uses:` на версии выше. Для мажорных обновлений `docker/*` прочитать breaking changes в релизах (`gh release view <tag> -R docker/<action>`) и поправить входные параметры, если они переименованы (особенно `metadata-action` v6 и `build-push-action` v7).

- [ ] **Step 2: Lint integration-tagged code in CI**

В job `vet` после шага `golangci-lint` добавить:

```yaml
      - name: golangci-lint (integration tag)
        run: golangci-lint run --build-tags=integration ./...
```

- [ ] **Step 3: Verify**

```bash
$(go env GOPATH)/bin/actionlint .github/workflows/*.yml
```

Expected: без ошибок. Реальная проверка — после push (Task 10).

- [ ] **Step 4: Commit**

```bash
git add .github/workflows
git commit -m "ci: bump actions, lint integration-tagged code"
```

---

### Task 9: Документация с нуля

**Files:**
- Delete: `spec/`, `docs/adr/`, `docs/architecture.md`, `docs/auth.md`, `docs/delivery-semantics.md`, `docs/licenses.md`, `docs/operations.md`, `docs/perf-notes.md`, `llm.txt`, `internal/{app,auth,router,sse}/INVARIANTS.md`, `migrations/`, `docs/superpowers/plans/2026-10-28-*.md`, `docs/superpowers/plans/2026-10-29-hardening-and-cleanup.md`
- Create: `README.md` (заново), `docs/auth.md`, `docs/postgres.md`, `docs/operations.md`, `docs/architecture.md`
- Rewrite: `CLAUDE.md`
- Modify: `.gitignore` (удалить блок `# GSD planning artifacts` / `.planning/`), комментарии в коде, ссылающиеся на удалённые документы (`grep -rnE "spec/|docs/adr|INVARIANTS|llm\.txt|\.planning|POSTGRES-SETUP" --include=*.go .`)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: итоговое поведение после Task 1–8. Источники фактов для текстов — код (а не старые документы): `internal/app/env.go` (переменные), `internal/sse/*` (URL, кадры, заголовки, коды ответов), `internal/auth/*` (контракт бэкенда), `internal/app/bootstrap.go` (проверки Postgres), `internal/metrics/registry.go`, `internal/health/*`. Старые тексты доступны через `git show master:<path>` только как подсказка, что не забыть.
- Produces: документацию, на которую ссылается релиз (Task 10).

Правила для всех текстов: английский язык; коротко и по делу; ни одного упоминания GSD, фаз, планов, ID требований (`WAL-05`, `OBS-01`, `SEC-…`), testbench, k8s; каждое утверждение проверяемо по коду; примеры команд рабочие.

- [ ] **Step 1: Delete**

```bash
git rm -r -q spec docs/adr docs/architecture.md docs/auth.md docs/delivery-semantics.md docs/licenses.md docs/operations.md docs/perf-notes.md llm.txt migrations \
  internal/app/INVARIANTS.md internal/auth/INVARIANTS.md internal/router/INVARIANTS.md internal/sse/INVARIANTS.md \
  docs/superpowers/plans/2026-10-28-fail-closed-auth-and-sse-stream-loop.md docs/superpowers/plans/2026-10-28-simplification-round-2.md docs/superpowers/plans/2026-10-29-hardening-and-cleanup.md
```

(Текущий план `2026-10-29-v4-lightweight.md` удаляется в Task 10.)

- [ ] **Step 2: README.md** — обязательные разделы:
  1. Одна фраза: что это (Postgres row changes → SSE, per-entity, authorized, field-filtered).
  2. Quick start: `docker compose up` по корневому `docker-compose.yml`; `curl -N -H "Authorization: Bearer …" localhost:8080/sse/v1/<table>/<pk>`.
  3. Configuration: таблица из 12 переменных (имя, обязательна ли, дефолт, смысл) — сверить с `envVars` и `LoadFromEnv`; фраза про отказ на неизвестных `WALERA_*`.
  4. Subscribing: URL `/sse/v1/{table}/{pk}` и `/sse/v1/{table}/all`, `since_lsn`, `X-Request-ID`, CORS; кадры `initial_data`, `tx` (формат JSON — пример), `error` (`auth_revoked`, `auth_unavailable`, `slow_consumer`, `tx_too_large`), `shutdown`, heartbeat-комментарий, `retry:`; коды ответа handshake (400/401/403/404/429/503).
  5. Delivery guarantees: транзакционная атомарность, порядок, нет resume (клиент пересинхронизируется), multi-root правило для exact.
  6. Auth в одном абзаце + ссылка на `docs/auth.md`.
  7. Postgres requirements в трёх пунктах + ссылка на `docs/postgres.md`.
  8. Health & metrics: `/healthz`, `/readyz`, `/metrics` (5 метрик) + ссылка на `docs/operations.md`.
  9. Development: `make test`, `make testbin/cdc-sse && make test-integration`, `make lint`.
  10. License.
- [ ] **Step 3: docs/auth.md** — контракт `POST /auth/sessions`: когда вызывается (handshake и каждые TTL), что пересылается (Bearer, cookie/headers из allowlist, `X-Request-ID`, тело `{"channel": "table:pk"}`), ответ (`user_id`, `tables`, `ttl_seconds` — читается только из handshake, `initial_data`), статусы (200/401/403/404/прочее → unavailable, редиректы не следуют), refresh-семантика (повтор, смена `user_id` → revoke, ретраи 1s/3s/9s), `WALERA_AUTH_ALLOW_PLAINTEXT`, зарезервированные заголовки, минимальный пример бэкенда (Python, ~30 строк) — проверить пример, запустив его локально против `curl`.
- [ ] **Step 4: docs/postgres.md** — `wal_level=logical`, `max_replication_slots`/`max_wal_senders`, роль с `REPLICATION`, `GRANT SELECT`, `REPLICA IDENTITY` для UPDATE/DELETE, публикация (создать вручную или через `WALERA_TABLES`), что Walera проверяет на старте (prereqs, роль, отказ `FOR ALL TABLES`/`FOR TABLES IN SCHEMA`, пустая публикация, расхождение со списком), временный слот, отсутствие PgBouncer, managed-Postgres заметки (RDS `rds.logical_replication`, Cloud SQL флаг) — только то, что можно утверждать уверенно.
- [ ] **Step 5: docs/operations.md** — запуск контейнером, health/readiness семантика (Postgres only), 5 метрик с примерами PromQL и 3 примера алертов (`walera_pg_connection_status == 0`, доля `auth_requests_total{result="unavailable"}`, рост `subscriber_disconnects_total{reason="slow_consumer"}`), graceful shutdown (кадр `shutdown`, дедлайн 10 s), фиксированные лимиты (таблица констант из Task 7), что смотреть в логах (сообщения из кода: `standby status update failed`, `subscriber dropped: tx_too_large`, `auth permission refresh failed` и т.п. — сверить grep'ом).
- [ ] **Step 6: docs/architecture.md** — одна диаграмма (ASCII) pipeline (WAL reader → tx builder → router → per-subscriber stream), владение соединением репликации одним циклом, фильтрация по whitelist, refresh, slow consumer (bounded queue + write deadline), shutdown, решения безопасности (fail-closed, scrub, отказ широких публикаций).
- [ ] **Step 7: CLAUDE.md** — переписать: Project (2–3 абзаца), Constraints (Go 1.26, pglogrepl, PG ≥ 14, single instance, `-race`, security/PII правило дословно: «never log row data, tokens, or secrets; PK values are OK (identifiers, not content); wildcards intended for publicly-enumerable entities only»), Stack (фактический `go.mod` после Task 7 — без koanf/xxhash/x/time), Commands (build/test/integration/lint), Conventions (коротко). Без GSD-блоков и «GSD Workflow Enforcement».
- [ ] **Step 8: Code comments and .gitignore**

```bash
grep -rnE "spec/|docs/adr|INVARIANTS|llm\.txt|\.planning|POSTGRES-SETUP|delivery-semantics|perf-notes" --exclude-dir=.git --exclude=CHANGELOG.md --exclude=TODO.local.md .
```

Исправить найденное (ссылки в комментариях — на новые документы или удалить); `.gitignore` — убрать блок `.planning/`. Expected после правок: grep пуст.

- [ ] **Step 9: Verify docs against code**

- Каждая переменная из README есть в `envVars`, и наоборот: `for v in $(grep -oE 'WALERA_[A-Z_]+' README.md | sort -u); do grep -q "\"$v\"" internal/app/env.go || echo "missing in code: $v"; done`.
- Каждая метрика из docs есть в `internal/metrics/registry.go`.
- Ссылки между документами существуют: `grep -oE '\(docs/[a-z-]+\.md[^)]*\)|\(\./[a-z-]+\.md[^)]*\)' -r README.md docs/*.md`.
- `docker compose -f docker-compose.yml config` проходит.

- [ ] **Step 10: Changelog + commit**

`### Changed`: `- **Documentation rewritten.** README plus docs/auth.md, docs/postgres.md, docs/operations.md and docs/architecture.md replace the spec, ADRs, invariants files and llm.txt.`

```bash
git add -A
git commit -m "docs: rewrite documentation from scratch"
```

---

### Task 10: Релиз v4.0.0 и демо (контроллер; внешние действия — только с подтверждения пользователя)

**Files:**
- Modify: `CHANGELOG.md` (`## [4.0.0] - <дата>`), удалить `docs/superpowers/plans/2026-10-29-v4-lightweight.md` (и каталог `docs/superpowers/`, если пуст)
- Внешний репозиторий: `~/src/test-walera` (remote `walera-demo`)

- [ ] **Step 1: Final verification on the branch**

```bash
go vet ./... && go vet -tags=integration ./... && go test -race -count=1 ./...
$(go env GOPATH)/bin/golangci-lint run ./... && $(go env GOPATH)/bin/golangci-lint run --build-tags=integration ./...
$(go env GOPATH)/bin/govulncheck ./...
make testbin/cdc-sse && go test -tags=integration -race -count=3 -timeout 60m ./test/integration/...
docker build -t walera-check . && docker compose -f docker-compose.yml config >/dev/null
```

Expected: всё зелёное; интеграция 3/3 без падений.

- [ ] **Step 2: Merge, push, release** (после подтверждения пользователя)

Слить ветку в `master`, вставить `## [4.0.0] - <YYYY-MM-DD сегодня>` под `## [Unreleased]`, удалить файл плана, `bash scripts/check-changelog-date.sh`, push `master`, дождаться зелёного `Checks`, затем `git tag -a v4.0.0 -m v4.0.0 && git push origin v4.0.0`; дождаться `Release` и `Publish Image`; проверить `docker pull ghcr.io/slavnycoder/walera:4.0.0`.

- [ ] **Step 3: Demo repo** (после подтверждения пользователя)

В `~/src/test-walera`, ветка `feat/single-endpoint-auth`: в `docker-compose.yml` образ `ghcr.io/slavnycoder/walera:4.0.0`; env: `WALERA_AUTH_BACKEND_URL` → `WALERA_AUTH_URL`, `WALERA_WAL_PUBLICATION_NAME` → `WALERA_PUBLICATION`, удалить `WALERA_WAL_BOOTSTRAP_MODE` и `WALERA_HTTP_H2C_ENABLED`, `WALERA_HTTP_CORS_ORIGINS` → `WALERA_CORS_ORIGINS`, `WALERA_AUTH_ALLOW_PLAINTEXT` оставить. Поднять стек, проверить: Walera healthy, в логах нет `unknown environment variables`, `curl -N` с `demo-token` на `/sse/v1/todo_lists/1` даёт `initial_data`, затем `UPDATE todo_lists …` даёт `tx`. Обновить README демо под новые имена. Коммит, push, PR в основную ветку демо (`gh pr create`) или слияние — по решению пользователя.

- [ ] **Step 4: TODO.local.md**

Отметить выполненными пункты §4a и §5; добавить, что осталось (если что-то отложено ревью).
