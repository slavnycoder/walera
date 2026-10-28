# Fail-closed auth + SSE stream loop — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть четыре security-дыры (отзыв прав не доходит до живого стрима, неограниченный fail-open при открытом breaker, нефильтрованная доставка при `Filter == nil`, проглоченные паники) и заменить SSE writer pool (busy-poll + head-of-line blocking + hijack) на цикл записи в горутине самого HTTP-хендлера.

**Architecture:** Шаг 1 (Tasks 1–5) делает auth и роутер fail-closed: refresh включён всегда, любая неудача refresh (включая открытый breaker) рвёт стрим после TTL + backoff; роутер без фильтра ничего не доставляет; паника в любой горутине роняет процесс (k8s перезапустит). Шаг 2 (Tasks 6–7): каждый SSE-хендлер сам владеет своим соединением — `select` по очереди подписчика / heartbeat-тикеру / `sub.Done()` / `r.Context()`, запись через `http.ResponseController` с write deadline. Без hijack, без пула; медленный клиент блокирует только свою горутину.

**Tech Stack:** Go 1.26 stdlib `net/http`, `zerolog`, `prometheus/client_golang`, `koanf/v2`. Новых зависимостей нет.

**Spec:** Источник требований — ревью архитектуры в этой сессии (security-пункты 1, 2, 3, 5 + замена writer pool) и `CLAUDE.md` (Constraints). Отдельного spec-файла нет.

## Global Constraints

- Go `1.26.4` (из `go.mod`); никаких новых модулей в `go.mod`.
- `go test -race ./...` (`make test`) должен быть зелёным после каждой задачи — CI требует `-race`-clean.
- Покрытие пакета ≥ 85% строк (`coverage.yml`); проверить `go test -cover ./internal/sse ./internal/auth ./internal/router ./internal/app` в конце Task 7.
- Никогда не логировать данные строк, токены, cookie/header значения, `initial_data`. PK — можно.
- SSE URL версия не меняется: `/sse/v1/...`; wire-формат кадров (`retry: 15000\n\n`, `:\n\n`, `event: error`, `event: shutdown`) не меняется.
- Комментарии — короткие, только неочевидное «почему» (`make lint-comments`, `make comment-density` ≤ 30% в `internal/{sse,router,app,auth}`).
- Каждая задача добавляет строку в `CHANGELOG.md` под `## [Unreleased]` (секции `### Security` / `### Changed` / `### Removed`).
- Коммит после каждой задачи, сообщение в стиле репо: `fix(auth): ...`, `refactor(sse): ...`.

## Review Focus

1. **Отзыв прав во время стрима, когда в очереди уже лежат кадры.** После `sub.Drop("auth_revoked")` клиент должен получить `event: error` и больше ни одного `tx`-кадра. Тест: `TestStream_NoFramesAfterRevokeErrorFrame` (Task 6).
2. **Graceful shutdown с живыми стримами.** Без hijack `http.Server.Shutdown` теперь ждёт SSE-хендлеры; они обязаны выйти, как только броадкастер дропнет подписчиков с `shutdown`. Тест: `TestStream_ServerShutdownCompletesAfterDrop` (Task 6).
3. **TLS и HTTP/2 клиенты.** Раньше hijack требовал `*net.TCPConn` и падал на in-process TLS. Тест: `TestStream_ServesOverTLS` (Task 6, подтесты `http1` и `h2`).
4. **Долгий outage auth-бэкенда.** Подписчик не должен жить дольше TTL + сумма backoff (1s+3s+9s) с устаревшим грантом. Тест: `TestSubscriber_AuthOutage_DropsAfterTTL` (Task 2).
5. **Клиент перестал читать (полный TCP-буфер), а другие клиенты живы.** Остальные стримы получают кадры без задержки; зависший получает `slow_consumer` по write deadline, его горутина выходит. Тест: `TestStream_SlowClientDoesNotStallOthers` (Task 6).

## Not in this plan (осознанно)

- Тихий дроп `multi_root` (`internal/router/router.go:230`) и `FOR ALL TABLES` в bootstrap — отдельные решения по контракту.
- Удаление breaker целиком, HMAC-refresh, конфиг-зоопарк, xxhash-шардинг — следующий план.
- Мёртвое поле `router.SubscriberConfig.BufferCap` — остаётся (Task 6 перестаёт его передавать; удаление трогает много тестов без пользы).

---

### Task 1: Periodic permission refresh всегда включён

Сейчас `auth.default_ttl_seconds` по умолчанию 0 → refresh выключен → отозванный пользователь получает данные до разрыва соединения. `docs/operations.md:286` уже документирует дефолт 60 — код с документацией расходится.

**Files:**
- Modify: `internal/auth/config.go:85-98` (`ApplyDefaults`), `internal/auth/config.go:150-157` (`Validate`)
- Modify: `internal/app/runnables.go:31-49` (убрать ветку «refresh disabled»)
- Modify: `internal/sse/stream.go:114-118` (убрать guard `DefaultTTLSeconds > 0`)
- Test: `internal/auth/config_test.go`, `internal/app/config_test.go:325-327`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: инвариант `auth.Config.DefaultTTLSeconds >= 1` после `LoadConfig`. Tasks 2–3 на него опираются (refresh loop всегда запущен).

- [ ] **Step 1: Write the failing tests**

В `internal/auth/config_test.go` заменить проверку в `TestLoadConfig_Defaults`:

```go
	if cfg.DefaultTTLSeconds != 60 {
		t.Errorf("DefaultTTLSeconds = %d; want 60 (refresh is always on)", cfg.DefaultTTLSeconds)
	}
```

и добавить после `TestLoadConfig_Defaults`:

```go
func TestLoadConfig_RejectsZeroTTL(t *testing.T) {
	k := newK(t)
	_ = k.Set("auth.default_ttl_seconds", 0)
	_, err := auth.LoadConfig(k)
	if err == nil || !strings.Contains(err.Error(), "auth.default_ttl_seconds") {
		t.Fatalf("LoadConfig: err = %v; want auth.default_ttl_seconds rejected", err)
	}
}
```

В `internal/app/config_test.go:325-327` заменить ожидание:

```go
	if cfg.Auth.DefaultTTLSeconds != 60 {
		t.Errorf("Auth.DefaultTTLSeconds: got %d; want 60", cfg.Auth.DefaultTTLSeconds)
	}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/auth -run 'TestLoadConfig_(Defaults|RejectsZeroTTL)' -v && go test ./internal/app -run TestLoad -v`
Expected: FAIL — `DefaultTTLSeconds = 0; want 60` и `err = <nil>; want auth.default_ttl_seconds rejected`.

- [ ] **Step 3: Implement**

`internal/auth/config.go`, в `ApplyDefaults` удалить комментарий про «intentionally has no default» и первой строкой добавить:

```go
	_ = k.Set("auth.default_ttl_seconds", 60)
```

В `Validate` заменить блок `if c.DefaultTTLSeconds < 0 { ... }` на:

```go
	if c.DefaultTTLSeconds < 1 {
		errs = append(errs, config.FormatError(
			"auth.default_ttl_seconds",
			fmt.Sprintf("%d", c.DefaultTTLSeconds),
			"must be > 0: live streams must re-check permissions so revocations propagate",
			"omit the field (default 60) or use a positive integer",
		))
	}
```

`internal/app/runnables.go`: заменить `if a.Config.Auth.DefaultTTLSeconds > 0 { runs = append(...) } else { a.Logger.Info()...Msg("auth periodic-refresh disabled ...") }` на безусловный `runs = append(runs, Runnable{Name: "auth-stale-watcher", ...})` (тело `Run` без изменений; Task 3 удалит этот runnable целиком).

`internal/sse/stream.go`: заменить

```go
	if h.authCfg.DefaultTTLSeconds > 0 {
		safego.Go("auth-refresh-"+sub.ID(), func() {
			authSub.RefreshLoop(r.Context())
		})
	}
```

на

```go
	safego.Go("auth-refresh-"+sub.ID(), func() {
		authSub.RefreshLoop(r.Context())
	})
```

(`safego` уберёт Task 5.)

- [ ] **Step 4: Run tests**

Run: `go test -race ./internal/auth ./internal/app ./internal/sse ./internal/config`
Expected: PASS. Если упал тест, который строит конфиг через koanf и явно ставит `auth.default_ttl_seconds: 0` или проверяет лог «auth periodic-refresh disabled» — удалить такой тест (он проверяет снятое поведение). Тесты, создающие `auth.Config{}` литералом (без `Validate`), не трогать.

- [ ] **Step 5: CHANGELOG + commit**

В `CHANGELOG.md` под `## [Unreleased]`:

```markdown
### Security

- **Permission refresh is always on.** `auth.default_ttl_seconds` now
  defaults to `60` and `0` is rejected at startup. Previously the default
  was `0`, so a revoked user kept receiving a live stream until it
  disconnected.
```

```bash
git add internal/auth/config.go internal/auth/config_test.go internal/app/runnables.go internal/app/config_test.go internal/sse/stream.go CHANGELOG.md
git commit -m "fix(auth): enable permission refresh by default; reject ttl 0"
```

---

### Task 2: Refresh fail-closed — открытый breaker больше не продлевает грант

Сейчас `RefreshLoop` пропускает тик при открытом breaker (`internal/auth/subscriber.go:209`), а `tryRefresh` не дропает подписчика, если breaker открылся во время ретраев (`subscriber.go:305`). Итог: при outage бэкенда отозванный пользователь получает данные бесконечно. После задачи: тик → refresh → ошибки → backoff 1s/3s/9s → `auth_unavailable`. Breaker остаётся только для новых открытий (503).

**Files:**
- Modify: `internal/auth/subscriber.go` (удалить `breakerHook`, `SubscriberDeps.Breaker`, поле `breaker`, две проверки `Allow()`)
- Modify: `internal/sse/stream.go:99-106` (убрать `Breaker: h.breaker`)
- Modify: `internal/sse/handler.go` (удалить поле `breaker`, `AuthDeps.Breaker`, его валидацию)
- Modify: `internal/app/initialize.go` (убрать `Breaker: aw.Breaker` из `sse.AuthDeps`)
- Test: `internal/auth/subscriber_revocation_test.go`, `internal/auth/subscriber_test.go`, `internal/auth/refresh_test.go`, `internal/sse/refresh_test.go`, `internal/sse/handler_test.go`, `internal/sse/handler_panic_test.go`, `internal/sse/stream_bench_test.go`
- Modify: `docs/auth.md:254-270`, `docs/architecture.md:81-90`, `README.md:~236`, `deploy/prometheus/alerts.yaml:76`, `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 1 (refresh loop всегда запущен).
- Produces: `auth.SubscriberDeps{Sub, Client, Logger, Metrics, NowFunc, LSNFunc}` — без `Breaker`. `sse.AuthDeps{Client, Subscribers}` — без `Breaker`. `sse.Handler` больше не держит `*auth.Breaker`.

- [ ] **Step 1: Write the failing test**

Добавить в конец `internal/auth/subscriber_revocation_test.go` (поле `Breaker` пока существует — это и есть сценарий outage):

```go
// An auth outage must not extend a live grant past TTL + retry backoffs.
func TestSubscriber_AuthOutage_DropsAfterTTL(t *testing.T) {
	t.Parallel()

	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	})
	c, _ := newSubTestClient(t, srv.srv.URL)

	initial := makeMap(t, 100, "id", "email")
	initial.TTLSeconds = 0
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		Breaker:    &stubBreakerHook{allow: false}, // breaker open: the outage case
		InitialMap: initial,
		DefaultTTL: 15 * time.Millisecond,
		Backoffs:   []time.Duration{time.Millisecond, time.Millisecond, time.Millisecond},
	})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); s.RefreshLoop(ctx) }()
	defer func() { cancel(); <-done }()

	select {
	case <-s.Sub.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("subscriber still alive 2s into an auth outage; want auth_unavailable drop")
	}
	if got := s.Sub.Reason(); got != "auth_unavailable" {
		t.Fatalf("Sub.Reason = %q; want auth_unavailable", got)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/auth -run TestSubscriber_AuthOutage_DropsAfterTTL -v`
Expected: FAIL — `subscriber still alive 2s into an auth outage`.

- [ ] **Step 3: Implement in `internal/auth/subscriber.go`**

1. Удалить `type breakerHook interface { Allow() bool }`.
2. Удалить поле `Breaker breakerHook` из `SubscriberDeps`, поле `breaker breakerHook` из `Subscriber`, строку `breaker: deps.Breaker,` из `NewSubscriber`.
3. В `RefreshLoop` тик становится:

```go
		case <-ticker.C:
			s.tryRefresh(ctx)
```

4. Конец `tryRefresh` (после цикла по backoff) становится:

```go
	}

	s.dropIfUnchanged(mapAtChainStart)
}
```

- [ ] **Step 4: Remove the breaker from the sse handler**

`internal/sse/handler.go`: удалить поле `breaker *auth.Breaker`, строку `breaker: deps.Auth.Breaker,`, поле `Breaker *auth.Breaker` из `AuthDeps` и блок

```go
	if deps.Auth.Breaker == nil {
		panic("sse.NewHandler: Deps.Auth.Breaker is required")
	}
```

`internal/sse/stream.go`: удалить строку `Breaker: h.breaker,` из `auth.SubscriberDeps{...}`.

`internal/app/initialize.go` (`wireDataPlane`): удалить `Breaker: aw.Breaker,` из `sse.AuthDeps{...}`. Поле `Breaker` у `auth.Deps` (клиент) и `App.Breaker` **оставить** — breaker по-прежнему режет новые открытия.

- [ ] **Step 5: Fix tests to the new API**

- `internal/auth/subscriber_revocation_test.go`: в новом тесте удалить строку `Breaker: &stubBreakerHook{allow: false},`. Удалить `TestSubscriber_BoundedFailOpen_RevokeTakesEffectAfterRecovery` (проверяет снятое поведение) и тип `atomicBreaker`, если он больше нигде не используется.
- `internal/auth/subscriber_test.go`: удалить `TestSubscriber_RefreshTick_5xxTrippedBreakerNoDrop`, `TestSubscriber_RefreshTick_BreakerOpenSkipsCall`, тип `stubBreakerHook`, поле `Breaker` из `subscriberTestOpts` и строку `Breaker: opts.Breaker,` в `newTestSubscriber`; убрать `Breaker: bk,` из остальных литералов `subscriberTestOpts{...}` и переменные `bk`, ставшие неиспользуемыми.
- `internal/auth/refresh_test.go`, `internal/auth/registry_test.go`, `internal/auth/subscriber_panic_test.go`, `internal/sse/refresh_test.go`: удалить ключ `Breaker:` только внутри литералов `SubscriberDeps{...}` / `auth.SubscriberDeps{...}` / `subscriberTestOpts{...}` и `AuthDeps{...}`. Ключ `Breaker:` в `auth.Deps{...}` (клиент) не трогать.
- `internal/sse/handler_test.go`, `internal/sse/stream_bench_test.go`: удалить `Breaker: breaker,` из `AuthDeps{...}`.
- `internal/sse/handler_panic_test.go`: удалить `Breaker: breaker,` из `AuthDeps{...}` и кейс `{name: "Breaker", ...}`.

Найти все места: `grep -rn "Breaker" internal --include=*_test.go | grep -v "auth.Deps\|BreakerConfig\|NewBreaker\|BreakerDeps\|\*auth.Breaker\|breaker_test\|breaker_panic_test\|breaker_window_test"`.

- [ ] **Step 6: Run tests**

Run: `go vet ./... && go test -race ./internal/auth ./internal/sse ./internal/app`
Expected: PASS, включая `TestSubscriber_AuthOutage_DropsAfterTTL` и существующий `TestSubscriber_RefreshTick_5xxRetriesThenDropsUnavailable`.

- [ ] **Step 7: Docs**

`docs/auth.md`, раздел «Circuit breaker behavior» — заменить два буллета и абзац после них на:

```markdown
- **Established subscriptions** re-check permissions every TTL. If the
  backend is unreachable (including while the breaker is open), Walera
  retries with backoff (1s, 3s, 9s) and then drops the stream with
  `auth_unavailable`. A grant never outlives TTL + ~13s without a
  successful check. This is fail-closed.
- **New subscription attempts** are rejected with `503` while the
  breaker is open, so reconnecting clients do not hammer a failing
  backend.
```

`docs/architecture.md:81-90` — тот же смысл одним абзацем вместо «bounded fail-open posture». `README.md` (~236): заменить «**bounded fail-open** posture for established subscribers» на «fail-closed posture: established streams are dropped with `auth_unavailable` once a refresh fails past its retries». `deploy/prometheus/alerts.yaml:76` — `description: "New SSE opens are rejected with 503; established streams drop with auth_unavailable after TTL + retries. Investigate auth backend logs."`

- [ ] **Step 8: CHANGELOG + commit**

```markdown
- **Auth refresh is fail-closed.** An open circuit breaker no longer skips
  the periodic permission refresh. A failed refresh drops the stream with
  `auth_unavailable` after its retries, so a revoked user can no longer keep
  streaming for the whole duration of an auth-backend outage.
```

```bash
git add -A internal/auth internal/sse internal/app docs README.md deploy/prometheus/alerts.yaml CHANGELOG.md
git commit -m "fix(auth): fail closed on refresh errors while the breaker is open"
```

---

### Task 3: Удалить stale-watcher (мёртв после Task 2)

`Subscribers.WatchBreaker` / `fanoutStaleRefreshes` освежали подписчиков, переживших outage с устаревшим грантом. После Task 2 таких подписчиков нет — их дропает собственный refresh. Удаляем код, конфиг-ключ, метрику и алерт.

**Files:**
- Modify: `internal/auth/registry.go` (удалить `WatchBreaker`, `fanoutStaleRefreshes`, неиспользуемые импорты)
- Modify: `internal/auth/breaker.go` (удалить `WaitForClose`, `signalClose`, поля `closeMu`/`closeCh`, их инициализацию и вызов `b.signalClose()` в `transitionToClosed`)
- Modify: `internal/auth/config.go` (удалить `BreakerConfig.StaleRefreshJitter` и default `auth.breaker.stale_refresh_jitter`)
- Modify: `internal/config/config.go:76` (удалить маппинг `auth.breaker_stale_refresh_jitter`)
- Modify: `internal/app/runnables.go` (удалить runnable `auth-stale-watcher`)
- Modify: `internal/metrics/registry.go` (удалить `walera_auth_breaker_stale_subscribers` и `AuthBreakerStaleSubs()`)
- Modify: `deploy/prometheus/alerts.yaml:77-86` (удалить алерт `WaleraAuthBreakerStaleSubscribersOutsideBreaker`), `docs/operations.md:294` (строка `stale_refresh_jitter`), `test/integration/harness.go:196`
- Test: `internal/auth/registry_test.go`, `internal/auth/breaker_test.go`, `internal/metrics/registry_test.go`, `internal/health/server_test.go`, `internal/config/config_test.go`, `internal/app/config_test.go`, все литералы `StaleRefreshJitter:` в тестах

**Interfaces:**
- Consumes: Task 2.
- Produces: `auth.Breaker` API без `WaitForClose`; `auth.Subscribers` без `WatchBreaker`; `auth.BreakerConfig` без `StaleRefreshJitter`.

- [ ] **Step 1: Delete production code** (список выше). После удаления `registry.go` проверить импорты (`mathrand`, `safego`, `time` могут стать лишними).

- [ ] **Step 2: Compile to find every dependent test**

Run: `go vet ./... 2>&1 | grep -v "^#"`
Expected: ошибки компиляции только в тестах, перечисленных в Files.

- [ ] **Step 3: Fix tests**

- `internal/auth/registry_test.go`: удалить тесты, вызывающие `fanoutStaleRefreshes` (около строк 120–186), `TestRegistry_WatchBreakerExitsOnContextCancel`, `TestRegistry_WatchBreakerCycles`.
- `internal/auth/breaker_test.go`: удалить `TestBreaker_WaitForCloseChannelCyclesAcrossTransitions` и блок с `WaitForClose` около строк 150–180 (если это отдельный тест — удалить тест целиком; если часть теста о переходе Open→Closed — удалить только проверку канала, переход проверяется через `b.State()`).
- `internal/metrics/registry_test.go`: удалить `r.AuthBreakerStaleSubs().Set(0)` и строку `"walera_auth_breaker_stale_subscribers",` из списка имён. То же имя удалить из списка в `internal/health/server_test.go:314`.
- `internal/config/config_test.go:95-96`, `internal/app/config_test.go:349-350`: удалить проверки `stale_refresh_jitter` / `StaleRefreshJitter`.
- Все литералы `StaleRefreshJitter: ...,` в тестах (`grep -rn StaleRefreshJitter internal`) — удалить ключ.

- [ ] **Step 4: Run tests**

Run: `go vet ./... && go test -race ./...`
Expected: PASS. `grep -rn "StaleRefreshJitter\|WatchBreaker\|WaitForClose\|AuthBreakerStaleSubs\|stale_subscribers" --include=*.go --include=*.yaml --include=*.md . | grep -v "^./.planning\|CHANGELOG"` — пусто.

- [ ] **Step 5: CHANGELOG + commit**

```markdown
### Removed

- Breaker-close stale-refresh fan-out (`auth.breaker.stale_refresh_jitter`,
  `walera_auth_breaker_stale_subscribers`, alert
  `WaleraAuthBreakerStaleSubscribersOutsideBreaker`). Fail-closed refresh
  leaves no stale subscribers for it to catch.
```

```bash
git add -A internal test deploy docs CHANGELOG.md
git commit -m "refactor(auth): drop the stale-refresh watcher made dead by fail-closed refresh"
```

---

### Task 4: Роутер без фильтра ничего не доставляет

`dispatchEvent` при `sub.Filter == nil` отдаёт строку целиком (`internal/router/router.go:276`). В проде фильтр ставится всегда, но умолчание должно быть «дропнуть», а не «отдать всё».

**Files:**
- Modify: `internal/router/router.go:213-305` (`dispatchEvent`)
- Test: `internal/router/router_test.go`, `internal/router/router_bench_test.go`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: инвариант — `Broadcaster` доставляет кадр только подписчику с `Filter != nil`. Тестовый хелпер `allowAll(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool)` в `router_test.go`.

- [ ] **Step 1: Write the failing test**

В `internal/router/router_test.go` рядом с `mkTx` добавить хелпер:

```go
func allowAll(c wal.Change, _ pglogrepl.LSN) (wal.Change, bool) { return c, false }
```

и тест:

```go
func TestRouteTx_NilFilterDeliversNothing(t *testing.T) {
	t.Parallel()
	b, _ := mkBroadcaster(10000)
	sub := mkExactSub("public", "users", "42", 0, 4)
	sub.Filter = nil
	b.Register(sub)

	b.routeTx(mkTx(pglogrepl.LSN(0x300), 3, mkChange(wal.OpInsert, "public", "users", "42")))

	expectNoFrame(t, sub, 100*time.Millisecond)
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/router -run TestRouteTx_NilFilterDeliversNothing -v`
Expected: FAIL — `unexpected frame delivered (1 total)`.

- [ ] **Step 3: Implement**

В `dispatchEvent` сразу после проверки `tx.CommitLSN <= sub.StartLSN()` вставить:

```go
	// No filter means no grant: never deliver unfiltered rows.
	if sub.Filter == nil {
		return 0, 0
	}
```

Затем развернуть `if sub.Filter != nil { ...filter path... } else { ...nil-Filter path... }`: тело filter-ветки остаётся без обёртки `if`, ветку `else` (комментарий «nil-Filter path ...» и весь блок до закрывающей скобки) удалить. Строка `ev := Event{Tx: tx, MatchedIndices: indices}` становится лишней — `ev` теперь объявляется как `ev := Event{Tx: subTx, MatchedIndices: newIdx}` в конце filter-ветки.

- [ ] **Step 4: Give test subscribers a filter**

В `newRecordedExactSubWithPK` и `newRecordedWildcardSubWithCap` после `NewSubscriber(...)` добавить `s.Filter = allowAll`.

Удалить `TestRouteTxNilFilterUnchangedFastPath` (проверяет удалённый fast path).

`TestRouteTxSchemaScoped_NilFilterStripsCrossSchema` переименовать в `TestRouteTxSchemaScoped_StripsCrossSchema`, обновить комментарий над ним, а проверки после `drainOne` заменить на:

```go
	if got := len(ev.Tx.Changes); got != 1 {
		t.Fatalf("delivered changes: got %d; want 1", got)
	}
	if got := ev.Tx.Changes[0].Schema; got != "public" {
		t.Errorf("delivered change schema: got %q; want public", got)
	}
```

Если `router_bench_test.go` создаёт подписчиков через `NewSubscriber` напрямую (строки ~30–45) — добавить `s.Filter = allowAll` перед `bc.Register(s)`, иначе бенчмарк измеряет пустой путь.

- [ ] **Step 5: Run tests**

Run: `go test -race ./internal/router ./internal/sse ./internal/auth && go test -run '^$' -bench . -benchtime 1x ./internal/router`
Expected: PASS. Если упал тест, который создаёт подписчика через `NewSubscriber` и ждёт кадр, — поставить ему `Filter = allowAll`.

- [ ] **Step 6: CHANGELOG + commit**

```markdown
- **Router is fail-closed on a missing filter.** A subscriber registered
  without a whitelist filter now receives nothing instead of unfiltered rows.
```

```bash
git add internal/router CHANGELOG.md
git commit -m "fix(router): deliver nothing to a subscriber without a filter"
```

---

### Task 5: Удалить `safego` — паника роняет процесс

`safego.Go` глотает паники. Паника в `wal-reader` → `OnError` не вызывается → процесс живёт без репликации. Паника в `auth-refresh-*` → подписчик стримит без refresh (fail-open). Правильное поведение — упасть; k8s перезапустит.

**Files:**
- Delete: `internal/safego/safego.go`, `internal/safego/safego_test.go`
- Modify: `internal/app/lifecycle.go`, `internal/sse/stream.go`, `internal/health/readyz.go`, `internal/router/router.go`, `internal/wal/reader.go`, `internal/auth/registry.go` (если ещё есть), `cmd/loadgen/main.go`
- Test: `internal/app/lifecycle_test.go`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: в репозитории нет пакета `internal/safego`.

- [ ] **Step 1: Write the failing test**

В `internal/app/lifecycle_test.go` добавить (импорты `errors`, `os`, `os/exec`):

```go
// A panicking runnable must crash the process, not leave it running headless.
func TestApp_RunnablePanicCrashesProcess(t *testing.T) {
	if os.Getenv("WALERA_PANIC_CHILD") == "1" {
		a := &App{Runnables: []Runnable{{
			Name: "boom",
			Run:  func(context.Context) error { panic("boom") },
		}}}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = a.Run(ctx, cancel)
		os.Exit(0) // reached only if the panic was swallowed
	}

	cmd := exec.Command(os.Args[0], "-test.run=^TestApp_RunnablePanicCrashesProcess$")
	cmd.Env = append(os.Environ(), "WALERA_PANIC_CHILD=1")
	err := cmd.Run()
	var ee *exec.ExitError
	if !errors.As(err, &ee) || ee.ExitCode() != 2 {
		t.Fatalf("child exit = %v; want exit code 2 (unrecovered panic)", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/app -run TestApp_RunnablePanicCrashesProcess -v`
Expected: FAIL — `child exit = <nil>; want exit code 2`.

- [ ] **Step 3: Implement**

Каждое `safego.Go(<name>, func() { ... })` заменить на `go func() { ... }()` и удалить импорт `github.com/walera/walera/internal/safego`. Найти все: `grep -rn "safego" --include=*.go .`. Затем `git rm -r internal/safego`.

Пример для `internal/app/lifecycle.go`:

```go
	for _, r := range a.Runnables {
		go func() {
			err := r.Run(ctx)
			if err != nil && ctx.Err() == nil && r.OnError != nil {
				r.OnError(err)
			}
		}()
	}
```

- [ ] **Step 4: Run tests**

Run: `go vet ./... && go test -race ./...`
Expected: PASS; `grep -rn safego --include=*.go .` — пусто.

- [ ] **Step 5: CHANGELOG + commit**

```markdown
- **Panics are no longer swallowed.** Removed `internal/safego`; a panic in
  any goroutine (WAL reader, auth refresh, shutdown) now crashes the process
  so the orchestrator restarts it, instead of leaving it running without
  replication or without permission refresh.
```

```bash
git add -A internal cmd CHANGELOG.md
git commit -m "fix: let goroutine panics crash the process; remove safego"
```

---

### Task 6: SSE-стрим в горутине хендлера вместо writer pool

Пул не экономит горутины (хендлер всё равно висит до конца стрима), раз в 1 мс опрашивает очереди всех подписчиков (`worker_loop.go:42`), и пишет синхронно с дедлайном 5 с: один медленный клиент тормозит всех на своём воркере. Hijack ломает in-process TLS. Здесь хендлер начинает писать сам; пул остаётся в коде неиспользуемым (удалит Task 7).

**Files:**
- Modify: `internal/router/subscriber.go` (переименовать `send` → `Send`), `internal/router/router.go:324`, `internal/router/subscriber_test.go`
- Modify: `internal/sse/stream.go` (новый `stream`, удалить `mainLoop`, `writeInitialFrame`, `finalizeError`)
- Modify: `internal/sse/handler.go` (удалить `hijackTCPConn`, поле `pool`, `Deps.Pool` и его валидацию)
- Modify: `internal/app/initialize.go` (убрать `Pool: writerPool` из `sse.Deps`)
- Create: `internal/sse/stream_test.go`
- Modify: `internal/sse/handler_test.go` (kit без пула; переименовать `TestHandler_HijackedTCPConn_WireBytes` → `TestHandler_RawWireBytes`), `internal/sse/refresh_test.go`, `internal/sse/handler_panic_test.go`
- Delete: `internal/sse/stream_bench_test.go` (бенчмарк старого `runWriter` через пул)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 2 (`sse.AuthDeps` без `Breaker`), Task 5 (нет `safego`).
- Produces:
  - `func (s *router.Subscriber) Send(frame []byte) bool` — неблокирующая постановка кадра; `false`, если очередь полна или sink не подключён.
  - `sse.Deps{Broadcaster, Auth, Limits, Logger, Metrics}` — без `Pool`.
  - `func (h *Handler) stream(w http.ResponseWriter, r *http.Request, sub *router.Subscriber, initialFrame []byte)` — владеет ответом до конца стрима.
  - Ёмкость очереди: `router.Config.ExactBuffer` (exact) / `router.Config.WildcardBuffer` (wildcard). `http.sub_queue_size` больше не читается.

- [ ] **Step 1: Export `Send` (pure rename)**

`internal/router/subscriber.go`: `func (s *Subscriber) send(frame []byte) bool` → `func (s *Subscriber) Send(frame []byte) bool`. Обновить вызов `sub.send(frame)` в `internal/router/router.go:324` и вызовы `sub.send(` в `internal/router/subscriber_test.go`.

Run: `go test -race ./internal/router`
Expected: PASS.

- [ ] **Step 2: Write the failing tests**

Создать `internal/sse/stream_test.go`:

```go
package sse

import (
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/walera/walera/internal/router"
)

func (f *fakeBroadcaster) waitSubs(t *testing.T, n int) []*router.Subscriber {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		f.mu.Lock()
		if len(f.subs) >= n {
			out := append([]*router.Subscriber(nil), f.subs[:n]...)
			f.mu.Unlock()
			return out
		}
		f.mu.Unlock()
		if time.Now().After(deadline) {
			t.Fatalf("fakeBroadcaster: %d subscribers not registered within 2s", n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func openStream(t *testing.T, client *http.Client, url string) *http.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	req.Header.Set("Authorization", "Bearer valid")
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("Do: %v", err)
	}
	t.Cleanup(func() { _ = resp.Body.Close() })
	return resp
}

const testPrelude = "retry: 15000\n\n"

func TestStream_DeliversSentFrame(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	resp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/42")
	sub := kit.bc.firstSub(t)
	frame := "event: tx\ndata: {\"k\":1}\n\n"
	if !sub.Send([]byte(frame)) {
		t.Fatal("Send returned false on an empty queue")
	}

	body := string(readUntil(t, resp.Body, len(testPrelude)+len(frame), 2*time.Second))
	if !strings.Contains(body, frame) {
		t.Fatalf("body = %q; want it to contain %q", body, frame)
	}
	sub.Drop("test")
}

// A client that stops reading must stall only its own stream.
func TestStream_SlowClientDoesNotStallOthers(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	slow, err := net.Dial("tcp", srv.Listener.Addr().String())
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { _ = slow.Close() })
	_, _ = slow.Write([]byte("GET /sse/v1/users/1 HTTP/1.1\r\nHost: x\r\nAuthorization: Bearer valid\r\n\r\n"))
	slowSub := kit.bc.waitSubs(t, 1)[0]

	fastResp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/2")
	fastSub := kit.bc.waitSubs(t, 2)[1]

	// Fill the slow client's socket buffers and queue until Send refuses.
	big := []byte("data: " + strings.Repeat("x", 64*1024) + "\n\n")
	filled := false
	for deadline := time.Now().Add(3 * time.Second); time.Now().Before(deadline); {
		if !slowSub.Send(big) {
			filled = true
			break
		}
	}
	if !filled {
		t.Fatal("slow client's queue never filled")
	}

	frame := "event: tx\ndata: {\"fast\":true}\n\n"
	if !fastSub.Send([]byte(frame)) {
		t.Fatal("fast Send returned false")
	}
	body := string(readUntil(t, fastResp.Body, len(testPrelude)+len(frame), time.Second))
	if !strings.Contains(body, frame) {
		t.Fatalf("fast client body = %q; want %q within 1s while another client is stuck", body, frame)
	}

	select {
	case <-slowSub.Done():
	case <-time.After(8 * time.Second):
		t.Fatal("slow client was never dropped")
	}
	if got := slowSub.Reason(); got != "slow_consumer" {
		t.Fatalf("slow Reason = %q; want slow_consumer", got)
	}
	fastSub.Drop("test")
}

func TestStream_ServesOverTLS(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		http2 bool
	}{{"http1", false}, {"h2", true}} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			kit := newTestHandler(t, nil, nil)
			validMapBackend(kit.backend)
			mux := http.NewServeMux()
			kit.h.Routes(mux)
			srv := httptest.NewUnstartedServer(mux)
			srv.EnableHTTP2 = tc.http2
			srv.StartTLS()
			t.Cleanup(srv.Close)

			resp := openStream(t, srv.Client(), srv.URL+"/sse/v1/users/42")
			if got := resp.ProtoMajor; got != map[bool]int{false: 1, true: 2}[tc.http2] {
				t.Fatalf("ProtoMajor = %d", got)
			}
			body := string(readUntil(t, resp.Body, len(testPrelude), 2*time.Second))
			if !strings.HasPrefix(body, testPrelude) {
				t.Fatalf("body = %q; want prelude %q", body, testPrelude)
			}
			kit.bc.firstSub(t).Drop("test")
		})
	}
}

func TestStream_NoFramesAfterRevokeErrorFrame(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	resp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/42")
	sub := kit.bc.firstSub(t)
	_ = readUntil(t, resp.Body, len(testPrelude), 2*time.Second)

	sub.Drop("auth_revoked")
	_ = sub.Send([]byte("event: tx\ndata: {\"leak\":true}\n\n"))

	rest, _ := io.ReadAll(resp.Body)
	body := string(rest)
	if !strings.Contains(body, `"reason":"auth_revoked"`) {
		t.Fatalf("body = %q; want auth_revoked error frame", body)
	}
	if strings.Contains(body[strings.Index(body, "auth_revoked"):], "leak") {
		t.Fatalf("frame delivered after the revoke error frame: %q", body)
	}
}

// Without hijack, Server.Shutdown waits for SSE handlers; a shutdown drop must release them.
func TestStream_ServerShutdownCompletesAfterDrop(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	validMapBackend(kit.backend)
	srv := newTestServer(t, kit.h)

	resp := openStream(t, http.DefaultClient, srv.URL+"/sse/v1/users/42")
	sub := kit.bc.firstSub(t)
	_ = readUntil(t, resp.Body, len(testPrelude), 2*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() { errCh <- srv.Config.Shutdown(ctx) }()
	sub.Drop("shutdown")

	if err := <-errCh; err != nil {
		t.Fatalf("Shutdown: %v; want nil within 2s after the shutdown drop", err)
	}
	rest, _ := io.ReadAll(resp.Body)
	if !strings.Contains(string(rest), "event: shutdown") {
		t.Fatalf("body = %q; want shutdown frame", rest)
	}
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `go test ./internal/sse -run 'TestStream_' -v`
Expected: `TestStream_ServesOverTLS/http1` FAIL (hijack возвращает `*tls.Conn` → «sse hijack failed», подписчик не регистрируется). `TestStream_ServerShutdownCompletesAfterDrop` FAIL или PASS в зависимости от пути hijack — фиксируем как регрессию. `TestStream_SlowClientDoesNotStallOthers` падает, если оба подписчика попали на один воркер пула (зависит от хэша ID), — для нового кода он обязан проходить детерминированно.

- [ ] **Step 4: Implement `stream` in `internal/sse/stream.go`**

Заменить `runWriter` и удалить `mainLoop`, `writeInitialFrame`, `finalizeError`:

```go
var preludeFrame = []byte("retry: 15000\n\n")

func (h *Handler) runWriter(
	w http.ResponseWriter,
	r *http.Request,
	table, pk, channelStr string,
	kind router.Kind,
	startLSN pglogrepl.LSN,
	hs handshakeResult,
) {
	sub, authSub, err := h.headersAndPreamble(w, r, table, pk, channelStr, kind, startLSN, hs)
	if err != nil {
		return
	}
	defer h.authRegistry.Remove(authSub.ID())

	var initialFrame []byte
	if len(hs.authMap.InitialData) > 0 {
		frame, overflow := h.enc.EncodeInitialData(hs.authMap.InitialData)
		if overflow {
			h.logger.Warn().
				Str("subscriber_id", sub.ID()).
				Int("initial_data_bytes", len(hs.authMap.InitialData)).
				Msg("sse initial_data exceeds max_payload_bytes; skipping")
		} else {
			initialFrame = frame
		}
	}

	h.stream(w, r, sub, initialFrame)
}

// stream owns the response on the handler's goroutine for the subscriber's
// lifetime. The router only enqueues (non-blocking), so a slow client stalls
// nobody but itself and is dropped by the write deadline.
func (h *Handler) stream(w http.ResponseWriter, r *http.Request, sub *router.Subscriber, initialFrame []byte) {
	rc := http.NewResponseController(w)
	connectedAt := time.Now()
	kind := sub.KindString()

	queueCap := h.routerCfg.ExactBuffer
	if sub.Kind() == router.KindWildcard {
		queueCap = h.routerCfg.WildcardBuffer
	}
	queue := make(chan []byte, queueCap)
	sub.WireSendFunc(func(frame []byte) bool {
		select {
		case queue <- frame:
			return true
		default:
			return false
		}
	})

	write := func(frames ...[]byte) error {
		_ = rc.SetWriteDeadline(time.Now().Add(h.cfg.WriteTimeout))
		for _, f := range frames {
			if _, err := w.Write(f); err != nil {
				return err
			}
		}
		return rc.Flush()
	}

	reason := "client_closed"
	defer func() {
		h.metrics.SubscriberLifetime().Observe(time.Since(connectedAt).Seconds())
		h.metrics.SubscriberDisconnects(reason).Inc()
		if reason == "slow_consumer" {
			h.metrics.SlowClientDrops().Inc()
		}
	}()

	if err := write(preludeFrame, initialFrame); err != nil {
		sub.Drop(reason)
		return
	}
	h.bc.Register(sub)
	defer h.bc.Deregister(sub)

	hb := time.NewTicker(h.cfg.HeartbeatInterval)
	defer hb.Stop()
	for {
		select {
		case frame := <-queue:
			if err := write(frame); err != nil {
				reason = writeFailureReason(err)
				sub.Drop(reason)
				return
			}
			h.metrics.EventsSent(kind).Inc()
			hb.Reset(h.cfg.HeartbeatInterval)
		case <-hb.C:
			if err := write(h.enc.EncodeHeartbeat()); err != nil {
				reason = writeFailureReason(err)
				sub.Drop(reason)
				return
			}
		case <-r.Context().Done():
			sub.Drop(reason)
			return
		case <-sub.Done():
			// Queued frames are discarded: after a revoke they carry the old grant.
			switch reason = sub.Reason(); reason {
			case "", "client_closed":
				reason = "client_closed"
			case "shutdown":
				_ = write(h.enc.EncodeShutdown())
			default:
				_ = write(h.enc.EncodeError(reason))
			}
			return
		}
	}
}

func writeFailureReason(err error) string {
	var ne net.Error
	if errors.Is(err, os.ErrDeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
		return "slow_consumer"
	}
	return "client_closed"
}
```

Импорты `stream.go`: `errors`, `net`, `net/http`, `os`, `time`, `pglogrepl`, `auth`, `router`.

В `headersAndPreamble` удалить вычисление `bufCap` и строку `BufferCap: bufCap,` из `router.SubscriberConfig{...}`.

`internal/sse/handler.go`: удалить функцию `hijackTCPConn`, поле `pool *WriterPool`, строку `pool: deps.Pool,`, поле `Pool *WriterPool` в `Deps` и блок `if deps.Pool == nil { panic(...) }`. Удалить неиспользуемые импорты (`net`, `errors` — если больше не нужны). `internal/sse/errors.go`: удалить `errHijackedConnNotTCP` (`errPoolClosed` остаётся до Task 7).

`internal/app/initialize.go`: удалить `Pool: writerPool,` из `sse.Deps{...}` (пул пока создаётся и останавливается — Task 7 его удалит).

- [ ] **Step 5: Fix test kits**

- `internal/sse/handler_test.go` (`newTestHandler`): удалить создание `pool` и его `t.Cleanup`, `Pool: pool,` из `Deps`, поле `pool *WriterPool` из `testHandlerKit` и `pool: pool,`. `TestHandler_HijackedTCPConn_WireBytes` переименовать в `TestHandler_RawWireBytes` (тело без изменений: заголовок `Transfer-Encoding: identity` сохраняет wire-формат без chunked).
- `internal/sse/refresh_test.go` (`newRefreshFwdHandler`): удалить `pool := NewPool(...)`, его cleanup и `Pool: pool,`; `enc` удалить, если стал неиспользуемым.
- `internal/sse/handler_panic_test.go`: удалить создание пула, `Pool: pool,` и кейс `{name: "Pool", ...}`.
- `git rm internal/sse/stream_bench_test.go`.

- [ ] **Step 6: Run tests**

Run: `go vet ./... && go test -race ./internal/sse ./internal/router ./internal/app -count=3`
Expected: PASS, все `TestStream_*` и существующие `TestHandler_*` / `TestHandshake_*` (prelude, heartbeat, `auth_revoked`, headers, raw wire).

- [ ] **Step 7: CHANGELOG + commit**

```markdown
### Changed

- **SSE streams are written by their own handler goroutine.** No more
  shared writer pool, 1 ms busy-poll, or TCP hijack: a slow client only
  blocks itself, and in-process TLS / HTTP/2 work. Queue capacity now comes
  from `router.exact_buffer` / `router.wildcard_buffer`;
  `walera_events_sent_total` no longer counts heartbeats.
```

```bash
git add -A internal CHANGELOG.md
git commit -m "refactor(sse): write each stream on its handler goroutine; drop hijack"
```

---

### Task 7: Удалить writer pool, его конфиг, метрики и CI-обвязку

После Task 6 пул создаётся, но не используется.

**Files:**
- Delete: `internal/sse/pool.go`, `attach.go`, `drain_helpers.go`, `worker_loop.go`, `shutdown.go`, `pool_metrics_adapter.go`, `errors.go`
- Delete tests: `internal/sse/pool_test.go`, `pool_bench_test.go`, `pool_integration_test.go`, `pool_panic_test.go`, `pool_metrics_adapter_test.go`, `worker_loop_isolation_test.go`, `goroutine_bound_test.go`, `metrics_parity_test.go`, `metrics_parity_types_test.go`, `golden_capture_test.go`, `golden_parity_test.go`, `golden_metrics_capture_test.go`, `golden_fixture_helper_test.go`, `tls_smoke_test.go`, `race_on_test.go`, `race_off_test.go`
- Modify: `internal/app/app.go` (поле `SSEPool`), `internal/app/initialize.go` (`dataPlaneWiring.PoolMetrics/Pool`, создание пула), `internal/app/lifecycle.go` (pool-ветка shutdown), `internal/app/config.go` (pool-поля `HTTPConfig`, defaults, валидация)
- Modify: `internal/metrics/registry.go` (`walera_pool_worker_dirty_subs`, `walera_pool_drain_batch_size`, `walera_pool_drain_duration_seconds`)
- Test: `internal/app/lifecycle_test.go`, `internal/app/app_singleton_test.go`, `internal/app/config_test.go:1232-1400`, `internal/metrics/registry_test.go`
- Modify: `Makefile` (цели `sse-stress`, `sse-soak`), `.github/workflows/flake-detect.yml` (jobs `sse-stress`, `sse-soak`), `.github/workflows/checks.yml:20-24` (комментарий), `docs/operations.md:319-335`, `docs/architecture.md` (`PoolMetricsAdapter`), `internal/sse/INVARIANTS.md` (pool-разделы), `deploy/k8s/configmap.yaml` (если там есть pool-ключи), `CHANGELOG.md`

**Interfaces:**
- Consumes: Task 6 (хендлер не зависит от пула).
- Produces: `func (a *App) shutdownStep1WaveWithCallbacks(ctx context.Context, drainDeadline time.Duration, onHTTPStart, onBroadcastStart, onPProfStart func())` — без `onPoolStart`; `App` без `SSEPool`; `HTTPConfig` без `PoolFactor`, `SubQueueSize`, `MaxWaitMs`, `DrainThresholdSubs`, `MaxBatchBytesPerSub`, `BatchingDisabled`.

- [ ] **Step 1: Update the lifecycle tests first**

`internal/app/lifecycle_test.go`:
- `newTestApp`: удалить `poolDeps`, `pool`, `SSEPool: pool,`; `enc := sse.NewEncoder(64 * 1024)` остаётся для `router.New`.
- `TestApp_ShutdownConcurrentWave`: удалить `poolStart`; вызов становится

```go
	a.shutdownStep1WaveWithCallbacks(
		ctx,
		1*time.Second,
		func() { httpStart.Store(now()) },
		func() { bcastStart.Store(now()) },
		nil,
	)

	starts := []int64{httpStart.Load(), bcastStart.Load()}
```

- `TestApp_ShutdownStepOrdering`: удалить `poolAt`, `havePool`, ветку `case e.Step == "pool"`, проверку `if !havePool` и элемент `{"pool", poolAt}`.

`internal/app/app_singleton_test.go`: удалить блок от `poolMetrics := a.SSEPool.Metrics()` до конца проверки `PoolMetricsAdapter`.

Run: `go vet ./internal/app`
Expected: FAIL компиляции — `too many arguments` в вызове `shutdownStep1WaveWithCallbacks` (подтверждает, что тест обновлён раньше кода).

- [ ] **Step 2: Delete the pool from `internal/app`**

`lifecycle.go`: в `shutdownStep1Wave` вызов становится `a.shutdownStep1WaveWithCallbacks(ctx, drainDeadline, nil, nil, nil)`; у `shutdownStep1WaveWithCallbacks` убрать параметр `onPoolStart`, `wg.Add(3)` → `wg.Add(2)`, удалить горутину `shutdown-pool` целиком.

`app.go`: удалить поле `SSEPool *sse.WriterPool`. `initialize.go`: удалить `poolMetrics`/`writerPool`, поля `PoolMetrics`/`Pool` в `dataPlaneWiring`, строку `SSEPool: dp.Pool,`.

`config.go`: удалить поля `PoolFactor`, `SubQueueSize`, `MaxWaitMs`, `DrainThresholdSubs`, `MaxBatchBytesPerSub`, `BatchingDisabled` из `HTTPConfig`, их `k.Set("http.…")` в `applyAppDefaults` и их проверки в валидации. В `config_test.go` удалить `TestLoad_HttpPoolKnobs_*` (строки ~1232–1400).

- [ ] **Step 3: Delete the pool from `internal/sse` and `internal/metrics`**

`git rm` всех файлов из списка Delete. `internal/metrics/registry.go`: удалить поля `poolWorkerDirtySubs`, `poolDrainBatchSize`, `poolDrainDuration`, их создание, регистрацию и методы `PoolWorkerDirtySubs`, `PoolDrainBatchSize`, `PoolDrainDuration`. `internal/metrics/registry_test.go`: удалить `TestRegistry_PoolMetricsRegistered`, `r.PoolWorkerDirtySubs("0").Set(0)`, три имени `walera_pool_*` из списков и два `checkBuckets(... "walera_pool_...")`.

Если после удаления `newFakeMetrics` всё ещё нужен какому-то тесту — `go vet` укажет; такой тест проверяет пул и удаляется.

- [ ] **Step 4: Run tests**

Run: `go vet ./... && go test -race ./... && grep -rn "WriterPool\|NewPool(\|poolWorker\|PoolMetricsAdapter\|walera_pool_\|pool_factor\|sub_queue_size\|max_wait_ms\|drain_threshold_subs\|max_batch_bytes_per_sub\|batching_disabled" --include=*.go --include=*.yaml --include=*.yml --include=*.md --include=Makefile . | grep -v "^./.planning\|CHANGELOG\|internal/writer\|cmd/writer\|docs/superpowers"`
Expected: тесты PASS; grep выдаёт только места из шага 5.

- [ ] **Step 5: CI, Makefile, docs**

- `Makefile`: удалить цели `sse-stress`, `sse-soak`, их комментарии, переменную `STRESS_SUBS` и оба имени из `.PHONY`.
- `.github/workflows/flake-detect.yml`: удалить jobs `sse-stress` и `sse-soak` и упоминания в шапке файла. Если в файле не осталось jobs — удалить файл целиком (`git rm`).
- `.github/workflows/checks.yml:20-24`: заменить ссылку на `TestPoolSlowClientIsolation*` на `TestStream_SlowClientDoesNotStallOthers in internal/sse`.
- `docs/operations.md`: заголовок `#### http.* — SSE listener and writer pool` → `#### http.* — SSE listener`; удалить шесть строк таблицы pool-ключей.
- `docs/architecture.md`: абзацы про `sse.PoolMetricsAdapter` (около строк 191 и 248) удалить; в «Runtime components» SSE handler — «each connection is written by its own handler goroutine from a small bounded queue».
- `internal/sse/INVARIANTS.md`: удалить разделы про pool/worker/drain/hijack.
- `deploy/k8s/configmap.yaml`: удалить pool-ключи, если есть.

Run: `actionlint .github/workflows/*.yml` (если установлен) `&& make lint lint-comments comment-density`
Expected: без ошибок.

- [ ] **Step 6: Coverage gate**

Run: `go test -race -cover ./internal/sse ./internal/auth ./internal/router ./internal/app`
Expected: каждый пакет ≥ 85%. Если `internal/sse` ниже — покрыть непокрытые ветки `stream` (например, `writeFailureReason` с не-timeout ошибкой) маленьким тестом в `stream_test.go`.

- [ ] **Step 7: CHANGELOG + commit**

```markdown
### Removed

- SSE writer pool and its knobs (`http.pool_factor`, `http.sub_queue_size`,
  `http.max_wait_ms`, `http.drain_threshold_subs`,
  `http.max_batch_bytes_per_sub`, `http.batching_disabled`), the
  `walera_pool_*` metrics, and the `sse-stress` / `sse-soak` flake jobs.
```

```bash
git add -A internal Makefile .github docs deploy CHANGELOG.md
git commit -m "refactor(sse): remove the writer pool, its config, metrics and CI jobs"
```

---

## Final verification

- [ ] `make test` — PASS.
- [ ] `make lint lint-comments comment-density` — PASS.
- [ ] `make test-integration` (нужен Docker) — PASS или те же skip-ы, что на `master`.
- [ ] `git diff --stat master` — прод-код в минусе примерно на 1 500 строк, тесты — на несколько тысяч.
