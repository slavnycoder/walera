# Simplification round 2 — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Реализовать решения, принятые 2026-10-28: wildcard-подписчики получают multi-root транзакции; `bootstrap.mode=auto` требует явный список таблиц; `Broadcaster.Shutdown` без мёртвого drain; batching flush; auth сводится к одному endpoint'у `/auth/sessions` (refresh повторяет исходные credentials клиента), без HMAC, breaker, `/sse/v1/refresh`; readiness проверяет только Postgres.

**Architecture:** Router и WAL-конфиг меняются точечно (Tasks 1–2). Shutdown и стрим упрощаются в рамках уже существующих функций (Tasks 3–4). Auth переделывается в два шага: сначала refresh переключается на `OpenSession` с сохранёнными credentials и удаляются on-demand refresh и реестр подписчиков (Task 5); затем удаляется всё, что держалось только на HMAC и breaker, включая readiness-пробу (Task 6). Task 7 приводит testbench в соответствие с новым контрактом.

**Tech Stack:** Go 1.26 stdlib `net/http`, `zerolog`, `prometheus/client_golang`, `koanf/v2`; testbench — Python 3 stdlib, docker compose, Grafana JSON.

**Spec:** Решения, принятые с пользователем 2026-10-28 (память проекта `simplification-round-2-decisions`):
- multi_root: guard снимается только для wildcard; exact сохраняет тихий дроп.
- `wal.bootstrap.mode=auto` + пустой `wal.bootstrap.tables` → ошибка на старте.
- Auth: один endpoint `/auth/sessions`; refresh повторяет его с исходными bearer / forwarded cookies / headers, которые хранятся в памяти стрима. Удалить HMAC-refresh `/auth/permissions`, signing secret/kid, `_health`-sentinel, circuit breaker с метриками и алертами.
- Истёкший токен: refresh получает 401 → `auth_revoked` → клиент переподключается. `POST /sse/v1/refresh` удалить.
- Readiness: только Postgres.
- `Broadcaster.Shutdown`: только `closed` + drop всех; удалить `shutdown.drain_deadline`.
- Batching flush: забирать из очереди всё накопленное и делать один flush.

## Global Constraints

- Go `1.26.4` (из `go.mod`); никаких новых модулей в `go.mod`.
- `go test -race ./...` зелёный после каждой задачи.
- Покрытие каждого из `internal/{sse,auth,router,app,health}` ≥ 85% (`go test -cover`), проверить в Task 6.
- Никогда не логировать данные строк, токены, значения cookie/header, `initial_data`. Credentials, сохранённые в `auth.Subscriber`, не должны попадать ни в один лог, ни в одну ошибку.
- URL SSE не меняется: `/sse/v1/{table}/{pk}`, `/sse/v1/{table}/all`; wire-формат кадров не меняется.
- Обратная совместимость контракта auth-бэкенда не требуется (greenfield).
- Комментарии — короткие, только неочевидное «почему» (`make lint-comments`, `make comment-density`).
- Каждая задача добавляет запись в `CHANGELOG.md` под `## [Unreleased]` (`### Security` / `### Changed` / `### Removed`).
- Коммит после каждой задачи: `fix(router): ...`, `refactor(auth): ...` и т.п.

## Review Focus

1. **Короткоживущий токен истёк посреди стрима.** Refresh получает 401 → стрим рвётся с `auth_revoked`, без ретраев. Тест: `TestSubscriber_RefreshTick_401_DropsAuthRevoked` (существует, должен остаться зелёным после Task 5).
2. **Cookie-only клиент (без bearer).** Refresh должен повторить cookie, иначе `OpenSession` отклонит запрос как `missing_credentials` и стрим ложно умрёт с `auth_revoked`. Тест: `TestSubscriber_RefreshReplaysClientCredentials/cookie_only` (Task 5).
3. **Бэкенд на refresh вернул whitelist другого пользователя** (токен переназначен). Стрим должен рваться с `auth_revoked`. Тест: `TestSubscriber_RefreshDifferentUserRevokes` (Task 5).
4. **Bulk-запись в таблицу wildcard-подписчика больше `max_changes_per_tx`.** После снятия guard такие транзакции доходят до wildcard; при превышении лимита подписчик дропается с `tx_too_large`, а не получает частичный кадр. Тест: `TestBroadcaster_MultiRoot_WildcardBulkOverCapDropsTooLarge` (Task 1).
5. **Старт с `bootstrap.mode=auto` без таблиц.** Ошибка на старте должна называть ключ `wal.bootstrap.tables`. Тест: `TestLoadConfig_AutoModeRequiresTables` (Task 2).

## Not in this plan (осознанно)

- Exact-подписчики по-прежнему молча пропускают multi-root транзакции (решение пользователя).
- `spec/` и `.planning/` — исторические документы, не правятся.
- Мёртвое поле `router.SubscriberConfig.BufferCap` остаётся.

---

### Task 1: multi_root guard только для exact

**Files:**
- Modify: `internal/router/router.go` (`dispatchEvent`, multi-root guard ~строка 236)
- Test: `internal/router/router_test.go` (`TestBroadcaster_MultiRoot_WildcardDropsOnTwoPKs` ~строка 1563)
- Modify: `README.md` (раздел «Rule 1 — One root row per transaction», ~строки 127–148), `internal/router/INVARIANTS.md` (п. 2 про `multi_root`), `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: `Broadcaster` применяет multi-root guard только к `KindExact`.

- [ ] **Step 1: Write the failing tests**

В `internal/router/router_test.go` заменить `TestBroadcaster_MultiRoot_WildcardDropsOnTwoPKs` целиком на:

```go
// Wildcard subscribers are authorized for every row of the table, so a
// multi-root tx is delivered whole.
func TestBroadcaster_MultiRoot_WildcardDelivers(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(10000)
	sub := mkWildcardSub("public", "todo_lists", 0, 8)
	recordedFor(t, sub).useEncoder(b.enc.(*stubEncoder))
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x4010), 401,
		mkChange(wal.OpInsert, "public", "todo_lists", "1"),
		mkChange(wal.OpInsert, "public", "todo_lists", "2"),
	)
	b.routeTx(tx)

	ev := drainOne(t, sub, 200*time.Millisecond)
	if got := len(ev.Tx.Changes); got != 2 {
		t.Fatalf("delivered changes: got %d; want 2", got)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "multi_root"); v != 0 {
		t.Errorf("tx_dropped_total{reason=multi_root}: got %v; want 0", v)
	}
}

func TestBroadcaster_MultiRoot_WildcardBulkOverCapDropsTooLarge(t *testing.T) {
	t.Parallel()

	b, reg := mkBroadcaster(2)
	sub := mkWildcardSub("public", "todo_lists", 0, 8)
	b.Register(sub)

	tx := mkTx(pglogrepl.LSN(0x4011), 402,
		mkChange(wal.OpInsert, "public", "todo_lists", "1"),
		mkChange(wal.OpInsert, "public", "todo_lists", "2"),
		mkChange(wal.OpInsert, "public", "todo_lists", "3"),
	)
	b.routeTx(tx)

	expectNoFrame(t, sub, 100*time.Millisecond)
	if got := sub.Reason(); got != "tx_too_large" {
		t.Fatalf("Reason: got %q; want tx_too_large", got)
	}
	if v := gatherCounter(t, reg, "walera_tx_dropped_total", "reason", "tx_too_large"); v != 1 {
		t.Errorf("tx_dropped_total{reason=tx_too_large}: got %v; want 1", v)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/router -run 'TestBroadcaster_MultiRoot_Wildcard' -v`
Expected: `WildcardDelivers` FAIL (кадр не доставлен, `multi_root` = 1). `WildcardBulkOverCapDropsTooLarge` FAIL: guard срабатывает раньше лимита, `Reason` пустой.

- [ ] **Step 3: Implement**

В `dispatchEvent` заменить условие guard:

```go
	// Wildcard subscribers may see every row; the guard only stops an exact
	// subscriber from receiving another root's rows and children.
	if sub.Kind() == KindExact && hasMultipleAnchorRoots(tx, subSchema, subTable) {
```

Тело блока (метрика, лог, `return 0, 0`) не меняется. Комментарий над guard, описывающий его как общий для всех подписчиков, сократить до приведённых двух строк.

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/router`
Expected: PASS, включая `TestBroadcaster_MultiRoot_ExactDropsOnTwoPKs` (exact по-прежнему дропается).

- [ ] **Step 5: Docs**

`README.md`, раздел «Rule 1»: заменить абзац «This rule is **broker-enforced**. …» на:

```markdown
This rule is **broker-enforced for exact subscribers** (`/sse/v1/{table}/{pk}`):
a multi-root transaction is silently skipped for them
(`walera_tx_dropped_total{reason="multi_root"}` increments, a warn-level log
fires, the connection stays open). Wildcard subscribers (`/sse/v1/{table}/all`)
are authorized for every row and receive the transaction whole, subject to
`router.max_changes_per_tx`. Split bulk operations across roots in your
application layer if exact subscribers must see them.
```

В строке-примере `-- ✗ DROPPED for affected subscribers (multi-root violation):` заменить на `-- ✗ DROPPED for exact subscribers (multi-root violation):`.

`internal/router/INVARIANTS.md`, п. 2: дописать в конец пункта предложение `The guard applies to KindExact subscribers only.`

- [ ] **Step 6: CHANGELOG + commit**

```markdown
### Changed

- **Wildcard subscribers receive multi-root transactions.** The multi-root
  guard now applies only to exact (`/{table}/{pk}`) subscribers; wildcard
  (`/{table}/all`) subscribers, which are authorized for every row, no longer
  silently miss bulk writes.
```

```bash
git add internal/router README.md CHANGELOG.md
git commit -m "fix(router): deliver multi-root transactions to wildcard subscribers"
```

---

### Task 2: `bootstrap.mode=auto` требует явный список таблиц

**Files:**
- Modify: `internal/wal/config.go` (`Validate`, ~строки 147–184)
- Modify: `internal/app/bootstrap.go` (~строки 169–186: ветка `FOR ALL TABLES` становится недостижимой)
- Test: `internal/wal/config_test.go`
- Modify: `test/integration/harness.go` (YAML-шаблон), `docker-compose.yml`, `docker-compose.dev.yaml`, `deploy/k8s/configmap.yaml`, `testbench/docker-compose.yml`, `config.yaml`, `docs/operations.md:277`, `deploy/POSTGRES-SETUP.md` (~строки 170–200), `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: инвариант — после `wal.Config.Validate` в режиме `auto` `len(Bootstrap.Tables) > 0`.

- [ ] **Step 1: Write the failing test**

Добавить в `internal/wal/config_test.go` (рядом с `TestLoadConfig_InvalidBootstrapMode`, тот же хелпер `newK`):

```go
func TestLoadConfig_AutoModeRequiresTables(t *testing.T) {
	k := newK(t)
	_ = k.Set("wal.bootstrap.mode", "auto")
	_ = k.Set("wal.bootstrap.tables", []string{})
	_, err := wal.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "wal.bootstrap.tables") {
		t.Fatalf("LoadConfig: err = %v; want wal.bootstrap.tables required", err)
	}
}
```

Файл в пакете `wal_test`. Если `newK` не задаёт таблицы, а другие тесты грузят дефолтный конфиг и ждут успеха, добавить в `newK` строку `_ = k.Set("wal.bootstrap.tables", []string{"public.orders"})`, а новый тест явно обнуляет список (как выше).

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/wal -run TestLoadConfig_AutoModeRequiresTables -v`
Expected: FAIL — `err = <nil>`.

- [ ] **Step 3: Implement**

В `wal.Config.Validate` после цикла проверки `c.Bootstrap.Tables` добавить:

```go
	if c.Bootstrap.Mode == "auto" && len(c.Bootstrap.Tables) == 0 {
		errs = append(errs, config.FormatError(
			"wal.bootstrap.tables",
			"[]",
			"required when wal.bootstrap.mode=auto: Walera never publishes FOR ALL TABLES",
			"list the tables clients may subscribe to, e.g. public.orders,public.devices, or use bootstrap.mode=verify with a DBA-managed publication",
		))
	}
```

В `internal/app/bootstrap.go` удалить ветку `else { ... FOR ALL TABLES ... }`: DDL всегда `FOR TABLE` (условие `if len(cfg.Tables) > 0` убрать, оставить тело).

- [ ] **Step 4: Fix configs that relied on the default**

- `test/integration/harness.go`: публикацию создаёт init-скрипт `testdata/001_publication.sql`, поэтому в блок `wal:` YAML-шаблона добавить

```yaml
  bootstrap:
    mode: verify
```

- `deploy/k8s/configmap.yaml`: публикация создаётся миграциями — в блок `wal:` добавить `bootstrap:` / `mode: verify` и строку в шапке-комментарии «bootstrap.mode=verify: publication is DBA-managed (migrations/001_publication.sql)».
- `testbench/docker-compose.yml`, сервис `walera`: добавить `WALERA_WAL_BOOTSTRAP_MODE: "verify"` (публикацию создают `testbench/migrations/001,002`).
- `docker-compose.yml` и `docker-compose.dev.yaml`, сервис `walera`: добавить
  `WALERA_WAL_BOOTSTRAP_TABLES: "${WALERA_WAL_BOOTSTRAP_TABLES:?set the tables to stream, e.g. public.orders}"`
  и строку комментария над ней: `# Required: Walera only publishes the tables you list.`
- `config.yaml`, раздел `wal.bootstrap`: обновить комментарий к `auto` — «create the publication if missing FOR TABLE <bootstrap.tables> (tables are required)»; добавить закомментированный пример `# tables: [public.orders]`.
- `docs/operations.md:277`: описание ключа `wal.bootstrap.tables` → «Required when `bootstrap.mode=auto`. Schema-qualified tables to publish.»; дефолт `[]` оставить.
- `deploy/POSTGRES-SETUP.md`: в таблице режимов для `auto` указать, что `bootstrap.tables` обязателен, и удалить упоминания `FOR ALL TABLES` как поведения по умолчанию.

- [ ] **Step 5: Run tests**

Run: `go vet ./... && go test -race ./...`
Expected: PASS. Если упал тест в `internal/app` или `internal/config`, который грузит дефолтный конфиг и ждёт успешной валидации, — задать в его хелпере `wal.bootstrap.tables` (как в Step 1) или `wal.bootstrap.mode=verify`. Проверки, что дефолт по-прежнему `auto`, не трогать.

Если Docker доступен: `make test-integration` — PASS (harness теперь в `verify`).

- [ ] **Step 6: CHANGELOG + commit**

```markdown
### Security

- **No more `FOR ALL TABLES`.** `wal.bootstrap.mode=auto` now requires
  `wal.bootstrap.tables`; startup fails without it. Tables clients never
  subscribe to (users, credentials) no longer leave Postgres.
```

```bash
git add internal/wal internal/app/bootstrap.go test/integration/harness.go docker-compose.yml docker-compose.dev.yaml deploy testbench/docker-compose.yml config.yaml docs/operations.md CHANGELOG.md
git commit -m "fix(wal): require an explicit table list for bootstrap.mode=auto"
```

---

### Task 3: `Broadcaster.Shutdown` без drain-ожидания; удалить `shutdown.drain_deadline`

После перехода на стрим в горутине хендлера `Drop` сразу закрывает `Done`, поэтому ожидание drain и предупреждение «did not fully drain» мертвы. Реальное ожидание делает `http.Server.Shutdown`.

**Files:**
- Modify: `internal/router/router.go` (`Shutdown`, ~строки 375–420)
- Modify: `internal/app/lifecycle.go` (`Shutdown`, `shutdownStep1Wave`, `shutdownStep1WaveWithCallbacks`, ветка `broadcast`)
- Modify: `internal/app/config.go` (`ShutdownConfig.DrainDeadline`, decode hook, default, `validateShutdown`), `internal/app/types.go` (`DrainDeadline`)
- Test: `internal/router/shutdown_test.go`, `internal/app/lifecycle_test.go`, `internal/app/config_test.go`, `internal/app/app_singleton_test.go`, `internal/app/initialize_bench_test.go`
- Modify: `test/integration/harness.go` (строка `drain_deadline`), `config.yaml:101-105`, `deploy/k8s/configmap.yaml:21,80-85`, `docs/operations.md:339`, `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: `func (b *Broadcaster) Shutdown()` — без аргументов и без возвращаемой ошибки. `func (a *App) shutdownStep1WaveWithCallbacks(ctx context.Context, onHTTPStart, onBroadcastStart, onPProfStart func())`. `ShutdownConfig` содержит только `Deadline`.

- [ ] **Step 1: Update router tests to the new API (red)**

В `internal/router/shutdown_test.go`:
- все вызовы `b.Shutdown(context.Background(), <d>)` с проверкой `err` заменить на `b.Shutdown()` без проверки ошибки;
- удалить `TestBroadcaster_Shutdown_TimesOutWhenDrainStalls` (проверяет удаляемый таймаут);
- в `TestBroadcaster_Shutdown_FansOutToAllSubscribers` после `b.Shutdown()` проверить синхронно, без ожидания:

```go
	for i, s := range subs {
		if got := s.Reason(); got != "shutdown" {
			t.Fatalf("sub %d Reason = %q; want shutdown (Shutdown must drop synchronously)", i, got)
		}
	}
```

Run: `go vet ./internal/router`
Expected: FAIL компиляции — `not enough arguments in call to b.Shutdown`.

- [ ] **Step 2: Implement `Broadcaster.Shutdown`**

```go
// Shutdown drops every subscriber with "shutdown"; Register drops any that
// arrive later. Handlers write the shutdown frame and exit on their own.
func (b *Broadcaster) Shutdown() {
	b.closed.Store(true)
	subs := append(b.exact.Snapshot(), b.wildcard.Snapshot()...)
	for _, s := range subs {
		s.Drop("shutdown")
	}
	b.log.Info().
		Int("subscribers", len(subs)).
		Msg("broadcaster shutdown: subscribers dropped")
}
```

Удалить ставшие неиспользуемыми импорты `router.go` (`sync`, `context`, `time` — только если больше нигде в файле не используются).

Run: `go test -race ./internal/router`
Expected: PASS.

- [ ] **Step 3: Update app tests (red)**

`internal/app/lifecycle_test.go`:
- `newTestApp`: удалить параметр `drainDeadline` и строку `cfg.Shutdown.DrainDeadline = ...`; поправить все вызовы `newTestApp(...)`.
- `TestApp_ShutdownConcurrentWave`: вызов становится

```go
	a.shutdownStep1WaveWithCallbacks(
		ctx,
		func() { httpStart.Store(now()) },
		func() { bcastStart.Store(now()) },
		nil,
	)
```

`internal/app/config_test.go`: удалить проверки и кейсы `drain_deadline` / `DrainDeadline` (дефолт, env override, `must be > 0`, `must be <= shutdown.deadline`). `internal/app/app_singleton_test.go`, `internal/app/initialize_bench_test.go`: удалить строки с `DrainDeadline`.

Run: `go vet ./internal/app`
Expected: FAIL компиляции в `lifecycle_test.go` (лишний/недостающий аргумент) — red.

- [ ] **Step 4: Implement app changes**

`internal/app/lifecycle.go`:
- в `Shutdown`: `a.shutdownStep1Wave(ctx)`;
- `func (a *App) shutdownStep1Wave(ctx context.Context) { a.shutdownStep1WaveWithCallbacks(ctx, nil, nil, nil) }`;
- у `shutdownStep1WaveWithCallbacks` удалить параметр `drainDeadline time.Duration`;
- горутина broadcast:

```go
	go func() {
		defer wg.Done()
		if onBroadcastStart != nil {
			onBroadcastStart()
		}
		stepStart := time.Now()
		a.RouterIndex.Shutdown()
		a.Logger.Info().
			Str("step", "broadcast").
			Int64("elapsed_ms", time.Since(stepStart).Milliseconds()).
			Msg("router drain complete")
	}()
```

`internal/app/types.go`: удалить `DrainDeadline` и его метод. `internal/app/config.go`: удалить поле `DrainDeadline` из `ShutdownConfig`, `drainT` и ветку `DrainDeadline(d)` из `stringToNamedDurationHook` (хук обрабатывает только `ShutdownDeadline`), default `shutdown.drain_deadline`, две проверки в `validateShutdown`.

- [ ] **Step 5: Configs and docs**

Удалить строку `drain_deadline: 4s` из YAML в `test/integration/harness.go`; `drain_deadline` и его комментарий из `config.yaml` (~101–105) и `deploy/k8s/configmap.yaml` (строки 21, 80–85; в строке 21 оставить только `shutdown.deadline`); строку `WALERA_SHUTDOWN_DRAIN_DEADLINE` из `docs/operations.md:339`.

- [ ] **Step 6: Run tests**

Run: `go vet ./... && go test -race ./... && grep -rn "drain_deadline\|DrainDeadline\|DRAIN_DEADLINE" --include=*.go --include=*.yaml --include=*.yml --include=*.md . | grep -v "^./.planning\|^./spec\|CHANGELOG\|docs/superpowers"`
Expected: тесты PASS; grep пустой.

- [ ] **Step 7: CHANGELOG + commit**

```markdown
### Removed

- `shutdown.drain_deadline`. `Broadcaster.Shutdown` now just drops every
  subscriber; the wait for streams to close is `http.Server.Shutdown`, bounded
  by `shutdown.deadline`. Old configs that still set the key are ignored.
```

```bash
git add -A internal/router internal/app test/integration/harness.go config.yaml deploy docs/operations.md CHANGELOG.md
git commit -m "refactor(router): drop the dead drain wait from Broadcaster.Shutdown"
```

---

### Task 4: Batching flush в `stream`

**Files:**
- Modify: `internal/sse/stream.go` (ветка `case frame := <-queue:` в `stream`)
- Test: `internal/sse/stream_test.go` (`gatedWriter`, новый тест)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: `stream` пишет все уже накопленные кадры одним `write(frames...)` (один flush); порядок кадров сохраняется; `EventsSent` растёт на число кадров.

- [ ] **Step 1: Write the failing test**

В `internal/sse/stream_test.go` добавить в `gatedWriter` поле `flushes int` и заменить метод `Flush`:

```go
func (g *gatedWriter) Flush() {
	g.mu.Lock()
	g.flushes++
	g.mu.Unlock()
}

func (g *gatedWriter) flushCount() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.flushes
}
```

Добавить тест (импорт `fmt`, если его нет):

```go
func TestStream_BatchesQueuedFramesIntoOneFlush(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	gw := &gatedWriter{hdr: http.Header{}, blocked: make(chan struct{})}
	sub := router.NewSubscriber(router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: "users", PK: "42"},
		router.SubscriberDeps{Parent: context.Background()})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	done := make(chan struct{})
	go func() { defer close(done); kit.h.stream(gw, r, sub, nil) }()
	kit.bc.firstSub(t)

	gate := make(chan struct{})
	gw.mu.Lock()
	gw.gate = gate
	gw.mu.Unlock()
	if !sub.Send([]byte("data: 0\n\n")) {
		t.Fatal("Send 0 returned false")
	}
	<-gw.blocked
	for i := 1; i <= 5; i++ {
		if !sub.Send([]byte(fmt.Sprintf("data: %d\n\n", i))) {
			t.Fatalf("Send %d returned false", i)
		}
	}
	before := gw.flushCount()
	close(gate)

	deadline := time.Now().Add(2 * time.Second)
	for !strings.Contains(gw.String(), "data: 5\n\n") {
		if time.Now().After(deadline) {
			t.Fatalf("frames not delivered: %q", gw.String())
		}
		time.Sleep(2 * time.Millisecond)
	}
	// frame 0 + one batch for 1..5; a heartbeat may add one more.
	if got := gw.flushCount() - before; got > 3 {
		t.Fatalf("flushes for 6 frames = %d; want <= 3 (queued frames batched)", got)
	}
	body := gw.String()
	for i := 0; i < 5; i++ {
		if strings.Index(body, fmt.Sprintf("data: %d\n", i)) > strings.Index(body, fmt.Sprintf("data: %d\n", i+1)) {
			t.Fatalf("frames out of order: %q", body)
		}
	}
	sub.Drop("test")
	<-done
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/sse -run TestStream_BatchesQueuedFramesIntoOneFlush -v`
Expected: FAIL — `flushes for 6 frames = 6; want <= 3`.

- [ ] **Step 3: Implement**

В `stream` заменить ветку очереди на:

```go
		case frame := <-queue:
			select {
			case <-sub.Done():
				finish()
				return
			default:
			}
			frames := [][]byte{frame}
		batch:
			for {
				select {
				case f := <-queue:
					frames = append(frames, f)
				default:
					break batch
				}
			}
			if err := write(frames...); err != nil {
				reason = writeFailureReason(err)
				sub.Drop(reason)
				return
			}
			h.metrics.EventsSent(kind).Add(float64(len(frames)))
			hb.Reset(h.cfg.HeartbeatInterval)
```

Размер пачки ограничен ёмкостью очереди (`router.exact_buffer` / `wildcard_buffer`), поэтому отдельный лимит не нужен.

- [ ] **Step 4: Run tests**

Run: `go test -race -count=5 ./internal/sse`
Expected: PASS, включая `TestStream_NoFramesAfterRevokeErrorFrame` и `TestStream_SlowClientDoesNotStallOthers`.

- [ ] **Step 5: CHANGELOG + commit**

```markdown
- **SSE writes are batched.** Frames already queued for a subscriber are
  written with a single flush.
```

```bash
git add internal/sse CHANGELOG.md
git commit -m "perf(sse): flush all queued frames at once"
```

---

### Task 5: Refresh повторяет credentials клиента; удалить `/sse/v1/refresh` и реестр подписчиков

Refresh сейчас ходит в HMAC-endpoint `/auth/permissions` по `user_id`. После задачи он вызывает тот же `OpenSession`, что и handshake, с bearer / forwarded cookies / headers клиента, сохранёнными в `auth.Subscriber`. Logout и истечение токена доходят до стрима. Удаляются: on-demand `POST /sse/v1/refresh`, `auth.Subscribers` (нужен был только ему), блокировки `refreshMu`/`chainMu` и логика `dropIfUnchanged` (refresh идёт только из одной горутины `RefreshLoop`). HMAC-код в `auth.Client` пока остаётся (его удалит Task 6), но больше не вызывается из refresh.

**Files:**
- Modify: `internal/auth/subscriber.go`
- Delete: `internal/auth/registry.go`, `internal/auth/registry_test.go`, `internal/sse/refresh.go`, `internal/sse/refresh_test.go`
- Modify: `internal/sse/auth.go` (`handshakeResult`, `authenticate`, `runHandshake`), `internal/sse/stream.go` (`runWriter`, `headersAndPreamble`), `internal/sse/handler.go` (`authRegistry`, `AuthDeps.Subscribers`, маршруты refresh)
- Modify: `internal/app/app.go`, `internal/app/initialize.go` (`SubRegistry`)
- Modify: `internal/metrics/registry.go` (`walera_refresh_ondemand_total`)
- Test: `internal/auth/subscriber_test.go`, `internal/auth/refresh_test.go`, `internal/auth/subscriber_revocation_test.go`, `internal/sse/handler_test.go`, `internal/sse/handler_panic_test.go`, `internal/metrics/registry_test.go`, `internal/app/*_test.go`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `func (c *auth.Client) OpenSession(ctx context.Context, bearer string, fwd auth.ForwardedAuth, channel, requestID string) (*auth.Whitelist, error)` (существует).
- Produces:
  - `auth.SubscriberConfig` получает поля `Bearer string` и `Forwarded auth.ForwardedAuth`.
  - `auth.Subscriber.tryRefresh(ctx)` вызывает `OpenSession`; `*ErrUnavailable` ретраится по backoff, остальные ошибки → drop.
  - Refresh, вернувший whitelist с другим `user_id`, дропает стрим с `auth_revoked`.
  - `sse.AuthDeps{Client}` — без `Subscribers`; `App` без `SubRegistry`; пакет `auth` без `Subscribers`/`NewSubscribers`/`RefreshOwnByChannel`.

- [ ] **Step 1: Write the failing tests**

В `internal/auth/subscriber_test.go`: в `subscriberTestOpts` добавить поля `Bearer string` и `Forwarded ForwardedAuth`; в `newTestSubscriber` перед вызовом `NewSubscriber` добавить

```go
	if opts.Bearer == "" && opts.Forwarded.Empty() {
		opts.Bearer = "test-token"
	}
```

и передать `Bearer: opts.Bearer, Forwarded: opts.Forwarded` в `SubscriberConfig`.

Добавить в `internal/auth/subscriber_revocation_test.go`:

```go
func TestSubscriber_RefreshReplaysClientCredentials(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		bearer     string
		wantAuth   string
		wantCookie string
	}{
		{"bearer", "tok-1", "Bearer tok-1", "sess-1"},
		{"cookie_only", "", "", "sess-1"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			var mu sync.Mutex
			var gotPath, gotAuth, gotCookie string
			srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				gotPath = r.URL.Path
				gotAuth = r.Header.Get("Authorization")
				if c, err := r.Cookie("session_id"); err == nil {
					gotCookie = c.Value
				}
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
				_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
			})
			c, _ := newSubTestClient(t, srv.srv.URL)
			s := newTestSubscriber(t, subscriberTestOpts{
				Client:     c,
				InitialMap: makeMap(t, 100, "id", "name"),
				Bearer:     tc.bearer,
				Forwarded:  ForwardedAuth{Cookies: []*http.Cookie{{Name: "session_id", Value: "sess-1"}}},
			})

			s.tryRefresh(context.Background())

			mu.Lock()
			defer mu.Unlock()
			if gotPath != "/auth/sessions" {
				t.Errorf("refresh path = %q; want /auth/sessions", gotPath)
			}
			if gotAuth != tc.wantAuth {
				t.Errorf("Authorization = %q; want %q", gotAuth, tc.wantAuth)
			}
			if gotCookie != tc.wantCookie {
				t.Errorf("session_id cookie = %q; want %q", gotCookie, tc.wantCookie)
			}
			if r := s.Sub.Reason(); r != "" {
				t.Fatalf("Sub.Reason = %q; want alive", r)
			}
			if _, ok := s.AuthMap.Load().Tables["orders"]["name"]; ok {
				t.Error("refresh did not swap in the narrowed grant")
			}
		})
	}
}

func TestSubscriber_RefreshDifferentUserRevokes(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}})) // user_id "user-1"
	})
	c, _ := newSubTestClient(t, srv.srv.URL)
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id"),
		UserID:     "someone-else",
	})

	s.tryRefresh(context.Background())

	if got := s.Sub.Reason(); got != "auth_revoked" {
		t.Fatalf("Sub.Reason = %q; want auth_revoked (refresh resolved to another user)", got)
	}
}
```

Добавить импорт `sync`, если его нет.

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/auth -run 'TestSubscriber_Refresh(ReplaysClientCredentials|DifferentUserRevokes)' -v`
Expected: FAIL компиляции — `unknown field Bearer in struct literal of type SubscriberConfig`. После добавления полей (Step 3, п. 1) без остального — FAIL: path `/auth/permissions`, пустой Authorization; `DifferentUser` — Reason пустой.

- [ ] **Step 3: Implement in `internal/auth/subscriber.go`**

1. В `SubscriberConfig` добавить:

```go
	// Bearer and Forwarded are the client's own credentials, replayed on every
	// refresh so logout and token expiry reach a live stream. Never logged.
	Bearer    string
	Forwarded ForwardedAuth
```

2. В `Subscriber`: удалить поля `refreshMu` и `chainMu` с их комментариями; добавить `bearer string` и `fwd ForwardedAuth`; в `NewSubscriber` заполнить `bearer: cfg.Bearer, fwd: cfg.Forwarded`.

3. Удалить `refreshAttempt`, `lockedAttempt`, `dropIfUnchanged`. Комментарий над `applyFetched` заменить на `// applyFetched swaps or drops auth_revoked; transient errors are left to the caller.` (без «Must hold refreshMu»).

4. `tryRefresh` заменить целиком:

```go
// tryRefresh re-opens the session with the client's credentials. Only
// *ErrUnavailable is retried; every other outcome swaps or drops.
func (s *Subscriber) tryRefresh(ctx context.Context) {
	for attempt := 0; ; attempt++ {
		fresh, err := s.client.OpenSession(ctx, s.bearer, s.fwd, s.channel, newRequestID())
		s.recordRefreshResult(err)
		if err != nil {
			s.logRefreshError(err, attempt)
		}
		switch s.applyFetched(fresh, err) {
		case attemptSwapped, attemptRevoked:
			return
		case attemptFailed:
			s.Sub.Drop("auth_unavailable")
			return
		}
		if attempt == len(s.backoffs) {
			s.Sub.Drop("auth_unavailable")
			return
		}
		select {
		case <-ctx.Done():
			return
		case <-s.Sub.Done():
			return
		case <-time.After(s.backoffs[attempt]):
		}
	}
}
```

5. В `refreshMapAllowed` первое условие:

```go
	if table == "" || fresh == nil || fresh.UserID != s.userID {
		return false
	}
```

6. Импорт `sync` удалить, если больше не используется.

- [ ] **Step 4: Pass credentials from the handshake (`internal/sse`)**

`internal/sse/auth.go`:
- в `handshakeResult` добавить поля `bearer string` и `fwd auth.ForwardedAuth`;
- сигнатура `authenticate` становится `func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request, channelStr string, res *handshakeResult) bool`; функция записывает `res.authMap`, `res.requestID`, `res.bearer = token`, `res.fwd = fwd` и возвращает `true`; на ошибках пишет ответ и возвращает `false`, как сейчас;
- удалить блок скраба (комментарий «Drop all forwarded credentials…», `r.Header.Del(...)`, `token = ""`, `fwd = auth.ForwardedAuth{}`): credentials теперь живут в подписчике всё время стрима, затирание копий в запросе ничего не даёт;
- в `runHandshake`: `if !h.authenticate(w, r, channelStr, &res) { return res, false }`; дальше вместо локальных `authMap`/`requestID` использовать `res.authMap`; финальные присваивания `res.authMap = authMap` / `res.requestID = requestID` удалить.

`internal/sse/stream.go`:
- `headersAndPreamble` возвращает `(sub *router.Subscriber, err error)`; в `auth.SubscriberConfig{...}` добавить `Bearer: hs.bearer, Forwarded: hs.fwd`; удалить `h.authRegistry.Add(authSub)`;
- в `runWriter`: `sub, err := h.headersAndPreamble(...)`, удалить `defer h.authRegistry.Remove(authSub.ID())`.

`internal/sse/handler.go`: удалить поле `authRegistry`, строку `authRegistry: deps.Auth.Subscribers,`, поле `Subscribers` в `AuthDeps` и его проверку в `validateDeps`, маршруты `POST /sse/v1/refresh` и `OPTIONS /sse/v1/refresh`. `git rm internal/sse/refresh.go internal/sse/refresh_test.go`.

- [ ] **Step 5: Delete the registry and on-demand metric**

- `git rm internal/auth/registry.go internal/auth/registry_test.go`.
- `internal/app/app.go`: удалить поле `SubRegistry`. `internal/app/initialize.go`: удалить `SubRegistry` из `authWiring`, создание `auth.NewSubscribers(...)`, `SubRegistry: aw.SubRegistry,` в `InitializeApp`, `Subscribers: aw.SubRegistry,` в `sse.AuthDeps`.
- `internal/metrics/registry.go`: удалить `refreshOnDemandTotal` (поле, регистрацию, pre-touch цикл с `"swapped", "no_live_subscriber", ...`, конструктор `walera_refresh_ondemand_total`, метод `RefreshOnDemand`).

- [ ] **Step 6: Fix tests**

- `internal/auth/refresh_test.go`: удалить `TestTryRefresh_OnDemandNotStarvedDuringBackoff`, `TestTryRefresh_BackoffChainCoalesces`, `TestDropIfUnchanged` и все `TestRefreshOwnByChannel_*`; `TestTryRefresh_Revoked403_DropsSub` оставить.
- `internal/auth/subscriber_test.go`: удалить `TestSubscriber_TryRefreshCoalescesConcurrentCalls`.
- `internal/sse/handler_test.go`, `internal/sse/handler_panic_test.go`: удалить `auth.NewSubscribers(...)`, `Subscribers: ...` из `AuthDeps{...}`, поле `authReg` из `testHandlerKit`, кейс `{name: "AuthSubscribers", ...}`. Удалить тесты, которые проверяют скраб `Authorization`/`Cookie` из `r.Header` после handshake (поиск: `grep -n "Scrub\|scrub" internal/sse/*_test.go`).
- `internal/metrics/registry_test.go`: удалить проверки `walera_refresh_ondemand_total` / `RefreshOnDemand`.
- `internal/app/*_test.go`: удалить обращения к `SubRegistry`.

Найти остальное: `go vet ./... 2>&1 | grep -v "^#"`.

- [ ] **Step 7: Run tests**

Run: `go vet ./... && go test -race ./...`
Expected: PASS, включая новые тесты и `TestSubscriber_RefreshTick_401_DropsAuthRevoked`, `TestSubscriber_AuthOutage_DropsAfterTTL`, `TestSubscriber_RefreshTick_5xxRetriesThenDropsUnavailable` (последний ждёт 4 запроса — `OpenSession` с backoff 1ms×3 даёт ровно 4).

Если Docker доступен: `make test-integration`. Интеграционный mock уже отвечает на `/auth/sessions` по bearer и cookie и учитывает `Revoke`, поэтому ревокационные сценарии должны проходить через новый путь. Падение теста, который ждёт запросов на `/auth/permissions`, починить в Task 6 (там удаляется этот путь из mock); в отчёте перечислить такие тесты.

- [ ] **Step 8: CHANGELOG + commit**

```markdown
### Security

- **Permission refresh replays the client's own credentials.** Every TTL,
  Walera re-calls `POST /auth/sessions` with the bearer / forwarded
  cookies / headers the stream was opened with. Logout and token expiry now
  end live streams (`auth_revoked`); a refresh that resolves to a different
  `user_id` also revokes. The credentials are kept in memory for the stream's
  lifetime and are never logged.

### Removed

- `POST /sse/v1/refresh` and `walera_refresh_ondemand_total`. Clients whose
  token expires reconnect with a fresh one.
```

```bash
git add -A internal CHANGELOG.md
git commit -m "refactor(auth): refresh by replaying client credentials; drop on-demand refresh"
```

---

### Task 6: Удалить HMAC, signing, breaker и auth-пробу readiness

**Files:**
- Delete: `internal/auth/signing.go`, `internal/auth/signing_test.go`, `internal/auth/breaker.go`, `internal/auth/breaker_window.go`, `internal/auth/breaker_test.go`, `internal/auth/breaker_panic_test.go`, `internal/auth/breaker_window_test.go`, `internal/auth/assertions.go`, `test/integration/11_breaker_test.go`
- Modify: `internal/auth/client.go` (breaker hook, signer, `RefreshPermissions`, `refreshRequest`, `newNonce`, `CheckAuth`, `reservedHeaders`), `internal/auth/config.go` (`HealthChannel`, `Breaker`, `Signing`, defaults, validation), `internal/auth/errors.go` (если ссылается на breaker/signing), `internal/auth/INVARIANTS.md`
- Modify: `internal/health/server.go`, `internal/health/readyz.go`
- Modify: `internal/app/app.go` (`Breaker`), `internal/app/initialize.go` (`wireAuth`, health deps), `internal/app/runnables.go` (`auth-breaker-fsm`), `internal/config/config.go` (env-маппинги `auth.breaker_*`, `auth.signing_*`)
- Modify: `internal/metrics/registry.go` (`walera_auth_circuit_breaker_state`)
- Modify: `test/integration/mock_auth.go`, `test/integration/harness.go`
- Modify: `deploy/prometheus/alerts.yaml`, `deploy/prometheus/README.md`, `config.yaml`, `docs/auth.md`, `docs/architecture.md`, `docs/operations.md`, `docs/adr/0002-auth-model.md`, `README.md`, `CLAUDE.md`, `llm.txt`, `internal/sse/INVARIANTS.md`, `internal/app/INVARIANTS.md`
- Test: `internal/auth/client_test.go`, `internal/auth/client_session_test.go`, `internal/auth/client_panic_test.go`, `internal/auth/client_forwarding_test.go`, `internal/auth/config_test.go`, `internal/auth/subscriber_panic_test.go`, `internal/health/server_test.go`, `internal/health/server_panic_test.go`, `internal/sse/handler_test.go`, `internal/sse/handler_panic_test.go`, `internal/app/*_test.go`, `internal/config/config_test.go`, `internal/metrics/registry_test.go`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 5 (refresh больше не вызывает `RefreshPermissions`).
- Produces: `auth.Deps{Logger, Metrics}`; `auth.Config{BackendURL, DefaultTTLSeconds, RequestTimeout, ForwardedCookies, ForwardedHeaders}`; `health.Deps{Logger, PgChecker, Metrics}`; readiness = Postgres; в коде нет `Breaker`, `Signer`, `CheckAuth`, `RefreshPermissions`.

- [ ] **Step 1: Write the failing readiness test**

В `internal/health/server_test.go` добавить (`stubReader` уже есть в файле):

```go
// Auth outages surface as 503 on new opens; readiness tracks Postgres only.
func TestReadyz_IgnoresAuthBackend(t *testing.T) {
	t.Parallel()
	r := &stubReader{}
	r.connected.Store(true)
	s := New(Config{ReadyzProbeInterval: time.Hour}, Deps{
		Logger:    zerolog.Nop(),
		PgChecker: r,
		Metrics:   metrics.New(),
	})
	s.probe(context.Background())

	mux := http.NewServeMux()
	s.Routes(mux)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/readyz", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("readyz = %d; want 200 with PG healthy and no auth checker", rr.Code)
	}
}
```

Run: `go test ./internal/health -run TestReadyz_IgnoresAuthBackend -v`
Expected: FAIL — паника `health.New: Deps.AuthChecker is required`.

- [ ] **Step 2: Readiness = Postgres only**

`internal/health/server.go`: удалить интерфейс `AuthChecker`, поле `authClient`, `Deps.AuthChecker` и его проверку; `newServerInternal` теряет параметр `authClient`. `internal/health/readyz.go`: удалить вызов `CheckAuth` и ветку `case authErr != nil`. `internal/app/initialize.go`: удалить `AuthChecker: aw.AuthClient,` из `health.Deps`. В `internal/health/server_test.go` / `server_panic_test.go`: удалить `stubAuthClient`, `TestReadyz_503WhenAuthDown`, кейс nil-`AuthChecker` в panic-тесте; хелпер `newTestServer` теряет параметр auth-клиента (поправить все вызовы); `TestReadyz_200WhenBothHealthy` переименовать в `TestReadyz_200WhenPGHealthy`.

Run: `go test -race ./internal/health ./internal/app`
Expected: PASS.

- [ ] **Step 3: Delete breaker and signing**

1. `git rm` файлов из списка Delete (кроме `11_breaker_test.go` — в Step 5).
2. `internal/auth/client.go`: удалить `BreakerHook`, `nopBreaker`, поле `bk`, `Deps.Breaker`, `SetBreaker`, `setBreakerOnce`, все вызовы `c.bk.RecordResult(...)`, поле `signer` и блок `if cfg.Signing.Secret != ""`, `RefreshPermissions`, `refreshRequest`, `newNonce`, `CheckAuth`; из `reservedHeaders` удалить `X-Walera-Sig` и `X-Walera-Kid`. Удалить ставшие лишними импорты.
3. `internal/auth/config.go`: удалить поля `HealthChannel`, `Breaker`, `Signing`, типы `BreakerConfig`, `SigningConfig`, defaults `auth.health_channel`, `auth.breaker.*`, `auth.signing.kid`, проверки `auth.signing.*` и `auth.breaker.cooldown vs auth.request_timeout`.
4. `internal/app/app.go`: удалить поле `Breaker`. `internal/app/initialize.go` (`wireAuth`): удалить `auth.NewBreaker(...)`, `authClient.SetBreaker(breaker)`, `Breaker` в `authWiring`/`InitializeApp`; `auth.New(cfg.Auth, auth.Deps{Logger: logger, Metrics: registry})`. `internal/app/runnables.go`: удалить runnable `auth-breaker-fsm`.
5. `internal/config/config.go`: удалить из `multiLevelEnvKeys` строки `auth.breaker_*` и `auth.signing_*`.
6. `internal/metrics/registry.go`: удалить `authBreakerState` (поле, регистрация, конструктор `walera_auth_circuit_breaker_state`, метод `AuthBreakerState`).

- [ ] **Step 4: Fix unit tests**

- `internal/auth/client_test.go`: удалить `TestClient_CheckAuth_*`, `TestNopBreaker_AllowAndRecord`, `TestClient_SetBreaker_*`. Тесты `TestClient_RefreshPermissions_StatusPropagation`, `_NetworkError`, `_TimeoutHonored` и `TestRefreshPermissions_OutboundRequestID_*` перенести на `OpenSession` (`c.OpenSession(ctx, "tok", ForwardedAuth{}, "orders:1", <id>)`), сохранив ассерты; если в `client_session_test.go` уже есть эквивалентный тест для `OpenSession` (тот же статус/ошибка/ID-санитизация), перенесённый удалить вместо дублирования.
- Во всех тестах убрать поля `Signing`, `Breaker`, `HealthChannel` из `auth.Config{...}`, `Breaker:` из `auth.Deps{...}`, создание `auth.NewBreaker(...)` и `sseTestProber`, кейс `{name: "Breaker", ...}` в `client_panic_test.go`, если есть. `internal/auth/config_test.go`: удалить `TestLoadConfig_RequiresSigningSecret`, `_RejectsShortSigningSecret`, `_RequiresSigningKid`, `_DefaultKid`, `_BreakerCooldownLessThanRequestTimeout`, проверки `HealthChannel`/`Breaker.WindowBuckets` в `TestLoadConfig_Defaults`; `newK` больше не ставит `auth.signing.secret`. Кейсы `reserved` в forwarding-тестах: удалить `X-Walera-Sig` / `X-Walera-Kid` из списков ожидаемых зарезервированных имён.
- `internal/config/config_test.go`, `internal/app/config_test.go`, `internal/metrics/registry_test.go`: удалить проверки breaker/signing ключей и метрики `walera_auth_circuit_breaker_state`.

Run: `go vet ./... && go test -race ./...`
Expected: PASS.

- [ ] **Step 5: Integration mock and harness**

`test/integration/mock_auth.go`: удалить `refreshBody`, поля `permissionsRequests`, `signingSecret`, `signingKid`, константы `IntegrationSigningSecret`/`IntegrationSigningKid`, методы `PermissionsRequestCount`, `SigningSecret`, `SigningKid`, `serveRefresh` и регистрацию `/auth/permissions`, ветку `_health`-sentinel, импорты `crypto/hmac`, `crypto/sha256`, `encoding/hex`; комментарий у `SetCookieMap` про «periodic-refresh path» заменить на «refresh replays the same cookie to /auth/sessions». `test/integration/harness.go`: из YAML удалить `health_channel`, блоки `signing:` и `breaker:` и соответствующие аргументы `fmt.Sprintf`. `git rm test/integration/11_breaker_test.go`. Тесты, падавшие в Task 5 из-за ожидания `/auth/permissions`, переписать на `/auth/sessions` (refresh виден как дополнительные вызовы `RequestCount()`).

Run (если Docker доступен): `make test-integration`
Expected: PASS.

- [ ] **Step 6: Deploy and docs**

- `deploy/prometheus/alerts.yaml`: удалить алерт `WaleraAuthBreakerOpen`. `deploy/prometheus/README.md`: удалить его строку. Добавить в `alerts.yaml` (группа auth, рядом с другими auth-алертами) алерт на рост отказов refresh:

```yaml
        - alert: WaleraAuthRefreshUnavailable
          expr: sum(rate(walera_auth_refresh_total{result="unavailable"}[5m])) > 0
          for: 2m
          labels:
            severity: P1
            app: walera
            component: auth
          annotations:
            summary: "Walera cannot reach the auth backend for permission refresh"
            description: "Established streams drop with auth_unavailable and new opens get 503. Investigate auth backend availability."
```

  и строку в `deploy/prometheus/README.md` с тем же смыслом.
- `config.yaml`: в комментарии про env убрать `WALERA_AUTH_SIGNING_SECRET`.
- `docs/auth.md`: раздел «Backend contract» — Walera вызывает один endpoint `POST /auth/sessions` при открытии и затем каждые TTL с теми же bearer / cookies / headers клиента; удалить подразделы и примеры про `POST /auth/permissions`, `X-Walera-Sig`, `X-Walera-Kid`, `_health`, signing secret; в «Forwarding cookies and headers» убрать «Open only, never refresh» (refresh теперь их повторяет) и `X-Walera-Sig`, `X-Walera-Kid` из списка зарезервированных; раздел «Circuit breaker behavior» заменить разделом «Backend outage»: новые открытия получают `503` после `auth.request_timeout`; установленные стримы рвутся с `auth_unavailable` после ретраев refresh (оценку худшего случая ≈ 1.5×TTL + 21s сохранить); раздел «Status codes»: 401/403/404 на refresh → `auth_revoked`, в том числе при истёкшем токене — клиент переподключается со свежим токеном; добавить строку «A `200` whose `user_id` differs from the stream's user revokes the stream». Пример Django-бэкенда: один view `POST /auth/sessions`, читающий `channel` из JSON-тела, и `urls.py` с `path("auth/sessions", ...)`; убрать `service-token`/`_health`.
- `docs/architecture.md`, `docs/operations.md`, `README.md`, `llm.txt`, `internal/auth/INVARIANTS.md`, `internal/sse/INVARIANTS.md`, `internal/app/INVARIANTS.md`: удалить или переписать упоминания breaker, HMAC, signing secret/kid, `/auth/permissions`, `_health`-пробы и readiness-проверки auth (readiness = Postgres); в `docs/operations.md` удалить строки конфигурации `auth.health_channel`, `auth.breaker.*`, `auth.signing.*`.
- `docs/adr/0002-auth-model.md`: дописать в конец раздел

```markdown
## Amendment (2026-10-28)

The HMAC-signed refresh endpoint and the circuit breaker were removed.
Refresh re-calls `POST /auth/sessions` with the client's own credentials,
so logout and token expiry reach live streams; a failed refresh drops the
stream (fail-closed). Readiness no longer probes the auth backend.
```

- `CLAUDE.md`: в разделе «Circuit Breaker: Hand-rolled FSM …» заменить содержимое одной строкой «Removed 2026-10-28: refresh is fail-closed and re-calls `/auth/sessions`; there is no circuit breaker.»; в таблице «What NOT to Use» строку про `sony/gobreaker` удалить.

Run: `grep -rn "breaker\|Breaker\|HMAC\|X-Walera-Sig\|X-Walera-Kid\|signing\|SIGNING\|auth/permissions\|CheckAuth\|health_channel" --include=*.go --include=*.md --include=*.yaml --include=*.yml --include=*.txt . | grep -v "^./.planning\|^./spec\|^./testbench\|CHANGELOG\|docs/superpowers\|docs/adr/0002-auth-model.md"`
Expected: пусто (testbench — Task 7; ADR сохраняет исторический текст).

- [ ] **Step 7: Coverage gate**

Run: `go test -race -cover ./internal/sse ./internal/auth ./internal/router ./internal/app ./internal/health`
Expected: каждый пакет ≥ 85%. Если ниже — покрыть непокрытые ветки изменённого кода маленьким тестом в соответствующем `*_test.go`.

- [ ] **Step 8: CHANGELOG + commit**

```markdown
### Removed

- HMAC-signed `POST /auth/permissions`, `auth.signing.*`,
  `auth.health_channel`, the auth circuit breaker (`auth.breaker.*`,
  `walera_auth_circuit_breaker_state`, alert `WaleraAuthBreakerOpen`) and the
  auth-backend readiness probe. The backend contract is now a single
  endpoint, `POST /auth/sessions`; readiness tracks Postgres only. New alert:
  `WaleraAuthRefreshUnavailable`.
```

```bash
git add -A internal test deploy docs README.md CLAUDE.md llm.txt config.yaml CHANGELOG.md
git commit -m "refactor(auth): remove HMAC refresh, signing and the circuit breaker"
```

---

### Task 7: Testbench под новый контракт

**Files:**
- Modify: `testbench/mock-auth/server.py`, `testbench/mock-auth/Dockerfile` (если передаёт signing env), `testbench/docker-compose.yml`, `testbench/.env.example`, `testbench/README.md`
- Delete: `testbench/scripts/failure-modes/fail-auth-breaker.sh`
- Modify: `testbench/scripts/smoke-05.sh`, `testbench/scripts/smoke-08.sh`, `testbench/scripts/smoke-09.sh`, `testbench/web/modules/metrics-panel.js`, `testbench/grafana/dashboards/walera-overview.json`, `testbench/prometheus/alerts.yaml`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: контракт Task 6 (один endpoint `POST /auth/sessions`; refresh = тот же вызов с теми же credentials; метрика `walera_auth_circuit_breaker_state` удалена).
- Produces: testbench поднимается без `WALERA_AUTH_SIGNING_SECRET`.

- [ ] **Step 1: mock-auth**

`testbench/mock-auth/server.py`: удалить `_signing_secret`, `_signing_kid`, `SECRET`, `KID`, `_sign`, `_check_nonce` и хранилище nonce, `_handle_refresh`, ветку `if u.path == "/auth/permissions"` в `do_POST`, `_health`-sentinel whitelist и импорт `hmac` (и `hashlib`, если больше не используется). Docstring модуля переписать: «Walera calls POST /auth/sessions on open and again every TTL with the client's own bearer/cookies; revoke a token via the admin endpoint to end live streams.» `FAIL_MODE` должен по-прежнему влиять на `POST /auth/sessions`. `GET /_health` (liveness самого mock'а) оставить.

Run: `python3 -m py_compile testbench/mock-auth/server.py`
Expected: без ошибок.

- [ ] **Step 2: compose, env, README**

`testbench/docker-compose.yml`: удалить `WALERA_AUTH_SIGNING_SECRET`, `WALERA_AUTH_SIGNING_KID` и их комментарии у сервисов `mock-auth` и `walera`. `testbench/.env.example`: удалить строки `WALERA_AUTH_SIGNING_SECRET` / `WALERA_AUTH_SIGNING_KID`. `testbench/mock-auth/Dockerfile`: удалить signing env, если есть. `testbench/README.md`: удалить упоминания signing secret, HMAC-refresh и breaker; сценарий «auth outage» описать как «streams drop with `auth_unavailable`, new opens get 503».

Run: `docker compose -f testbench/docker-compose.yml config >/dev/null`
Expected: без ошибок (если Docker доступен; иначе указать в отчёте).

- [ ] **Step 3: scripts, dashboards, alerts**

- `git rm testbench/scripts/failure-modes/fail-auth-breaker.sh`.
- `testbench/scripts/smoke-09.sh`: удалить `fail-auth-breaker` из массива `sc3_scripts` и строку `'Auth circuit breaker trip'` с соответствующей проверкой; обновить шапку-комментарий («four run-book scripts» → «three»).
- `testbench/scripts/smoke-08.sh`: из регулярки ожидаемых метрик удалить `auth_circuit_breaker_state|walera_auth_circuit_breaker_state|`, комментарий про breaker сократить.
- `testbench/scripts/smoke-05.sh`: удалить проверки HMAC-refresh / `/auth/permissions` / signing (найти `grep -n "permissions\|SIGNING\|breaker\|HMAC" testbench/scripts/smoke-05.sh`); если проверка доказывала, что refresh работает, заменить её проверкой, что mock получил больше одного `POST /auth/sessions` за время жизни стрима.
- `testbench/web/modules/metrics-panel.js`: удалить слот `auth_breaker` (строка с `["auth_breaker", "auth_breaker"]`), `BREAKER_LABELS`, блок чтения `walera_auth_circuit_breaker_state` и строку в шапке-комментарии; удалить соответствующий элемент разметки, если он задан в HTML testbench (`grep -rn "auth_breaker" testbench/web`).
- `testbench/grafana/dashboards/walera-overview.json`: удалить панели, чьи `targets[].expr` ссылаются на `walera_auth_circuit_breaker_state` (`grep -n breaker` даёт 4 вхождения); JSON должен остаться валидным (`python3 -m json.tool testbench/grafana/dashboards/walera-overview.json >/dev/null`).
- `testbench/prometheus/alerts.yaml`: удалить алерт `WaleraAuthBreakerOpen` и упоминание breaker в шапке-комментарии; добавить `WaleraAuthRefreshUnavailable` с тем же выражением, что в `deploy/prometheus/alerts.yaml` (Task 6).

Run: `grep -rn "breaker\|SIGNING\|signing\|HMAC\|auth/permissions" testbench`
Expected: пусто.

- [ ] **Step 4: Smoke (если Docker доступен)**

Run: `cd testbench && cp -n .env.example .env; docker compose up -d --build && ./scripts/smoke-05.sh; docker compose down -v`
Expected: smoke PASS. Если Docker недоступен — указать это в отчёте.

- [ ] **Step 5: CHANGELOG + commit**

```markdown
- Testbench follows the single-endpoint auth contract: mock-auth drops the
  HMAC refresh endpoint, and the breaker failure-mode script, panels and
  alert are removed.
```

```bash
git add -A testbench CHANGELOG.md
git commit -m "chore(testbench): follow the single-endpoint auth contract"
```

---

## Final verification

- [ ] `go vet ./... && go test -race ./...` — PASS.
- [ ] `make lint-comments comment-density` — PASS.
- [ ] `make test-integration` (Docker) — PASS.
- [ ] `grep -rn "FOR ALL TABLES" internal` — пусто.
