# Hardening and Cleanup (TODO §1–4) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Закрыть хвосты после двух раундов упрощения: дыры безопасности, тихие ошибки конфигурации, гонки подписки, ложные сигналы метрик, сломанные/нестабильные тесты, мёртвый код и неработающий lint.

**Architecture:** Точечные правки в существующих пакетах без новых абстракций. Каждая задача — свой пакет и свой тестовый цикл; продуктовые фиксы гонок (`internal/sse`) идут раньше интеграционных тестов, которые на них опираются; lint — последним, когда код уже не меняется.

**Tech Stack:** Go 1.26 (`go.mod`: `go 1.26.4`), `net/http`, pgx v5 / pglogrepl, koanf v2, zerolog, Prometheus client, testcontainers-go (интеграционные тесты), golangci-lint v2.

**Spec:** `TODO.local.md` в корне worktree (локальный, не в git) — разделы 1–4 и пункт «golangci-lint» из раздела 0. Принятые ранее решения: `~/.claude/projects/-home-xvvvx-src-walera/memory/simplification-round-2-decisions.md`. Этот план самодостаточен: всё нужное из обоих файлов перенесено сюда.

**Вне плана (сознательно):**
- `git push`, обновление внешнего auth-бэкенда — внешние действия, делает человек.
- Раздел 5 TODO (конфиг → ~6 ключей, метрики, xxhash, pprof, `cmd/writer`/`loadgen`, CI) — отдельные планы.
- `event: resync` для exact multi_root — решено оставить тихий дроп.

**Решения, принятые в этом плане:**
- Публикация `FOR ALL TABLES`, уже существующая в БД, — **ошибка старта** в режимах `verify` и `auto` (спека §1: «No `FOR ALL TABLES`»).
- Удалённые ключи конфига — **ошибка старта**, а не warn: логгера на этапе загрузки конфига нет, обратная совместимость не требуется, а молча проигнорированный `WALERA_AUTH_SIGNING_SECRET` создаёт ложное чувство защищённости.
- `ttl_seconds` из ответа refresh — **не применяется, документируем** (смена интервала на лету — лишняя сложность).
- Тест «drop во время сбора пачки»: первая проверка `Done` в цикле `stream` избыточна (между ней и второй ничего не пишется). Убираем её, остаётся одна проверка после сбора пачки — её детерминированно покрывает существующий `TestStream_NoFramesAfterRevokeErrorFrame`.
- Нестабильность `Test16TxScopedDelivery` — две реальные гонки: (а) `Register` происходит **после** первого flush, поэтому клиент может получить 200 раньше, чем стал подписчиком (продуктовый фикс, Task 4); (б) harness ждёт только TCP-порт, а слот репликации создаётся параллельно (фикс harness, Task 7).
- `golangci-lint`: зафиксированный v2.12.2 не умеет разбирать export data Go 1.26 → переход на v2.15.0; `.golangci.yml` использует ключи v1 (`linters-settings`, `issues.exclude-rules`), которые v2 молча игнорирует → миграция схемы.

## Global Constraints

- Go-модуль: `go 1.26.4`; новых зависимостей не добавлять.
- `go vet ./...` и `go test -race ./...` зелёные после каждой задачи.
- Security/PII: никогда не логировать данные строк, токены, секреты, значения cookie/заголовков; PK логировать можно. В сообщениях об ошибках конфига — только имена ключей, не значения.
- SSE-эндпоинт остаётся `/sse/v1/`; обратная совместимость конфига не требуется (greenfield).
- Комментарии в `*.go` не должны содержать токены `Plan N-N`, `Phase N`, `PITFALL`, `DN-N`, `LIM-N`, `CR-N`, `HB-N`, `OBS-N`, `SC#N` (проверка: `make lint-comments`), плотность комментариев проверяет `make comment-density`.
- Пользовательские изменения поведения — запись в `CHANGELOG.md` под `## [Unreleased]` в существующие подразделы `### Security` / `### Changed` / `### Fixed` (создать `### Fixed`, если его нет; без строки `YYYY-MM-DD`).
- Интеграционные тесты: перед запуском пересобрать бинарник `make testbin/cdc-sse`; запуск `go test -tags=integration -race -timeout 15m -run '<regex>' ./test/integration/...` (нужен Docker).
- Коммиты — conventional commits (`fix(auth): …`, `test(integration): …`, `chore(lint): …`).

## Review Focus

1. Оператор пишет `origin` в нижнем регистре в `auth.forwarded_headers` → конфиг отклоняется так же, как `Origin` (Task 1, кейсы `Origin`/`origin`).
2. В режиме `verify` существующая публикация `FOR ALL TABLES` (у неё `pg_publication_tables` непустой, поэтому старая проверка «есть таблицы» проходила) → старт падает с понятной ошибкой (Task 2, `verify all tables`).
3. Удалённый ключ задан только через env (`WALERA_AUTH_SIGNING_SECRET`) → старт падает, а текст ошибки содержит имя ключа, но не значение секрета (Task 3, кейс `env signing secret`).
4. Handshake гонится с остановкой сервиса: `Register` на закрытом broadcaster сразу дропает подписчика с `shutdown` → клиент всё равно получает prelude, затем `event: shutdown`, а не обрыв (Task 4, `TestStream_LateRegistrantGetsShutdownFrame`).
5. Клиент отключается, пока идёт запрос к auth-бэкенду (handshake или refresh) → это не «бэкенд недоступен»: нет `walera_auth_requests_total{result="unavailable"}`, нет warn-лога, иначе алерт `WaleraAuthBackendUnavailable` срабатывает от обычных переподключений (Task 5, два теста).

---

### Task 1: Учётные данные клиента — `Origin` зарезервирован, очистка на всех выходах

**Files:**
- Modify: `internal/auth/client.go:44-54` (`reservedHeaders`)
- Modify: `internal/sse/auth.go` (`authenticate`)
- Test: `internal/auth/config_test.go` (`TestLoadConfig_ForwardedHeaders_RejectsReserved`)
- Test: `internal/sse/handler_test.go` (новый тест рядом с `TestAuthenticate_ScrubsCredentialsFromRequest`)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: —
- Produces: `reservedHeaders` содержит `Origin`; `(*Handler).authenticate` очищает `Authorization`, `Cookie` и форвардимые заголовки из `r.Header` на **любом** выходе.

Почему: после handshake `authenticate` удаляет форвардимые заголовки из `r.Header`. Если оператор добавит `Origin` в `auth.forwarded_headers`, `handleCORS` его уже не увидит и браузерные стримы упадут на CORS. Отдельно: при раннем отказе (нет credentials, невалидный `X-Request-ID`) очистка сейчас не выполняется.

- [ ] **Step 1: Write the failing tests**

В `internal/auth/config_test.go` в `TestLoadConfig_ForwardedHeaders_RejectsReserved` расширить список `cases`:

```go
	cases := []string{
		"Authorization",
		"authorization",
		"Cookie",
		"content-type",
		"X-Request-Id",
		"x-request-id",
		"Host",
		"Origin",
		"origin",
	}
```

В `internal/sse/handler_test.go` сразу после `TestAuthenticate_ScrubsCredentialsFromRequest` добавить:

```go
// Early rejections (bad X-Request-ID) must scrub credentials too.
func TestAuthenticate_ScrubsCredentialsOnEarlyReject(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	kit.h.authClient = auth.New(auth.Config{
		BackendURL:       kit.backend.srv.URL,
		RequestTimeout:   2 * time.Second,
		ForwardedCookies: []string{"session"},
		ForwardedHeaders: []string{"X-Api-Key"},
	}, auth.Deps{Logger: zerolog.Nop(), Metrics: metrics.New()})

	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	r.Header.Set("Authorization", "Bearer valid")
	r.AddCookie(&http.Cookie{Name: "session", Value: "sess-abc"})
	r.Header.Set("X-Api-Key", "key-123")
	r.Header.Set("X-Request-ID", "bad id with spaces")
	var res handshakeResult
	if kit.h.authenticate(httptest.NewRecorder(), r, "users:42", &res) {
		t.Fatal("authenticate = true; want rejection for invalid X-Request-ID")
	}
	for _, name := range []string{"Authorization", "Cookie", "X-Api-Key"} {
		if v := r.Header.Get(name); v != "" {
			t.Errorf("request %s = %q after early reject; want scrubbed", name, v)
		}
	}
	if kit.backend.hits.Load() != 0 {
		t.Errorf("backend hits = %d; want 0 (rejected before OpenSession)", kit.backend.hits.Load())
	}
}
```

(Если поле счётчика у `fakeAuthBackend` называется иначе, чем `hits`, использовать фактическое имя из `internal/sse/handler_test.go` — сейчас это `hits atomic.Int64`.)

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/auth/ -run TestLoadConfig_ForwardedHeaders_RejectsReserved -v && go test ./internal/sse/ -run TestAuthenticate_ScrubsCredentialsOnEarlyReject -v`
Expected: FAIL — подтесты `Origin`/`origin` с `err = nil`; новый SSE-тест сообщает, что `Authorization`/`Cookie`/`X-Api-Key` не очищены.

- [ ] **Step 3: Implement**

`internal/auth/client.go` — добавить в `reservedHeaders` (последней строкой карты) и дополнить комментарий над картой:

```go
// reservedHeaders is the set of canonical header names Walera owns on the
// OpenSession request. They can never be injected or overridden via the
// header allowlist — config.Validate rejects them, and ForwardedFromRequest /
// OpenSession skip them defensively even if validation were bypassed.
// Origin is reserved because forwarded headers are scrubbed from the inbound
// request after the handshake, and CORS still needs it.
var reservedHeaders = map[string]struct{}{
	textproto.CanonicalMIMEHeaderKey("Authorization"):     {},
	textproto.CanonicalMIMEHeaderKey("Host"):              {},
	textproto.CanonicalMIMEHeaderKey("Content-Length"):    {},
	textproto.CanonicalMIMEHeaderKey("Content-Type"):      {},
	textproto.CanonicalMIMEHeaderKey("Accept"):            {},
	textproto.CanonicalMIMEHeaderKey("Connection"):        {},
	textproto.CanonicalMIMEHeaderKey("Transfer-Encoding"): {},
	textproto.CanonicalMIMEHeaderKey("Cookie"):            {},
	textproto.CanonicalMIMEHeaderKey("X-Request-Id"):      {},
	textproto.CanonicalMIMEHeaderKey("Origin"):            {},
}
```

`internal/sse/auth.go` — в `authenticate` перенести очистку в `defer` сразу после вычисления `fwd` и удалить старый блок после `OpenSession`:

```go
func (h *Handler) authenticate(w http.ResponseWriter, r *http.Request, channelStr string, res *handshakeResult) bool {
	authHdr := r.Header.Get("Authorization")
	token := ""
	if t, ok := strings.CutPrefix(authHdr, "Bearer "); ok {
		token = t
	}
	fwd := h.authClient.ForwardedFromRequest(r)
	// The subscriber holds the copies it replays; the request should not keep
	// a second copy, whichever way the handshake ends.
	defer func() {
		r.Header.Del("Authorization")
		r.Header.Del("Cookie")
		for name := range fwd.Headers {
			r.Header.Del(name)
		}
	}()
	if token == "" && fwd.Empty() {

		h.writeStatusOnlyError(w, r, http.StatusUnauthorized, 0)
		return false
	}
	requestID := r.Header.Get("X-Request-ID")
	if requestID == "" {
		requestID = newRequestID()
	} else if !validRequestID(requestID) {

		h.writeJSONError(w, r, http.StatusBadRequest, "invalid_request_id")
		return false
	}

	authMap, err := h.authClient.OpenSession(r.Context(), token, fwd, channelStr, requestID)
	if err != nil {
		h.writeAuthError(w, r, err)
		return false
	}
	res.authMap = authMap
	res.requestID = requestID
	res.bearer = token
	res.fwd = fwd
	return true
}
```

Важно: `writeAuthError`/`writeJSONError` вызываются **до** срабатывания `defer`, поэтому `handleCORS` в них по-прежнему видит `Origin` (он и так не удаляется — его нет в `fwd.Headers`).

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/auth/ ./internal/sse/`
Expected: PASS.

- [ ] **Step 5: Docs**

`docs/auth.md`, раздел `## Forwarding cookies and headers`: найти перечисление зарезервированных заголовков (`Authorization`, `Cookie`, `Host`, …) и добавить `Origin` с пояснением «needed for CORS after the handshake scrub». Если явного перечисления нет — добавить одно предложение: «Reserved headers (`Authorization`, `Cookie`, `Host`, `Content-Length`, `Content-Type`, `Accept`, `Connection`, `Transfer-Encoding`, `X-Request-Id`, `Origin`) are rejected at startup.»

`CHANGELOG.md` → `### Security`, новый пункт в начало списка:

```markdown
- **`Origin` cannot be forwarded to the auth backend.** It is now a
  reserved header: listing it in `auth.forwarded_headers` fails startup.
  Forwarded headers are scrubbed from the request after the handshake,
  so forwarding `Origin` silently broke CORS for browser streams. The
  scrub now also runs when the handshake is rejected early.
```

- [ ] **Step 6: Commit**

```bash
git add internal/auth/client.go internal/auth/config_test.go internal/sse/auth.go internal/sse/handler_test.go docs/auth.md CHANGELOG.md
git commit -m "fix(auth): reserve Origin and scrub credentials on every handshake exit"
```

---

### Task 2: Отказ от существующей публикации `FOR ALL TABLES`

**Files:**
- Modify: `internal/app/bootstrap.go` (`bootstrapPublication`, новые `lookupPublication`, `errAllTablesPublication`)
- Test: `internal/app/bootstrap_test.go` (`TestBootstrapPublicationModes`, `TestPrepareDatabaseUnit`, новый `TestBootstrapPublication_RejectsAllTables`)
- Modify: `test/integration/pg.go` (удалить `CreatePublication` и `joinIdents`)
- Modify: `docs/operations.md:103-105`, `llm.txt:91,98-99`, `deploy/POSTGRES-SETUP.md` (таблица режимов, ~строка 176), `CHANGELOG.md`

**Interfaces:**
- Consumes: `bootstrapDB` (интерфейс в `internal/app/bootstrap.go`), `fakeBootstrapDB`/`fakeRow` из `bootstrap_test.go`.
- Produces: `lookupPublication(ctx context.Context, conn bootstrapDB, name string) (exists, allTables bool, err error)`; `errAllTablesPublication(name string) error`.

Почему: проверка «`wal.bootstrap.tables` обязателен» ловит только создание новой публикации. Публикация, созданная раньше как `FOR ALL TABLES`, принимается и в `verify`, и в `auto` — и Walera стримит каждую таблицу БД (включая пользователей/учётки) в свой процесс.

- [ ] **Step 1: Write the failing tests**

В `internal/app/bootstrap_test.go` заменить тело `TestBootstrapPublicationModes` (новый первый запрос в `verify` и `auto` — `SELECT puballtables FROM pg_publication …`):

```go
func TestBootstrapPublicationModes(t *testing.T) {
	ctx := context.Background()
	if err := bootstrapPublication(ctx, &fakeBootstrapDB{}, bootstrapConfig{Mode: "off", PublicationName: "pub"}, zerolog.Nop()); err != nil {
		t.Fatalf("off: %v", err)
	}

	verifyOK := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}, {values: []any{2}}}}
	if err := bootstrapPublication(ctx, verifyOK, bootstrapConfig{Mode: "verify", PublicationName: "pub"}, zerolog.Nop()); err != nil {
		t.Fatalf("verify ok: %v", err)
	}

	verifyEmpty := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}, {values: []any{0}}}}
	if err := bootstrapPublication(ctx, verifyEmpty, bootstrapConfig{Mode: "verify", PublicationName: "pub"}, zerolog.Nop()); err == nil {
		t.Fatal("verify empty returned nil")
	}

	verifyMissing := &fakeBootstrapDB{rowResults: []fakeRow{{err: pgx.ErrNoRows}}}
	err := bootstrapPublication(ctx, verifyMissing, bootstrapConfig{Mode: "verify", PublicationName: "pub"}, zerolog.Nop())
	if err == nil || !strings.Contains(err.Error(), "does not exist") {
		t.Fatalf("verify missing: err = %v; want 'does not exist'", err)
	}

	autoTables := &fakeBootstrapDB{rowResults: []fakeRow{{err: pgx.ErrNoRows}}}
	if err := bootstrapPublication(ctx, autoTables, bootstrapConfig{Mode: "auto", PublicationName: "pub", Tables: []string{"public.a", "public.b"}}, zerolog.Nop()); err != nil {
		t.Fatalf("auto tables: %v", err)
	}
	if len(autoTables.execSQL) != 1 || !strings.Contains(autoTables.execSQL[0], "FOR TABLE public.a, public.b") {
		t.Fatalf("auto tables exec = %#v", autoTables.execSQL)
	}

	existing := &fakeBootstrapDB{
		rowResults: []fakeRow{{values: []any{false}}, {values: []any{1}}},
		queryRows:  []pgx.Rows{&fakeRows{values: [][]any{{"public.a"}}}},
	}
	if err := bootstrapPublication(ctx, existing, bootstrapConfig{Mode: "auto", PublicationName: "pub", Tables: []string{"public.a"}}, zerolog.Nop()); err != nil {
		t.Fatalf("existing: %v", err)
	}
}

// A pre-existing FOR ALL TABLES publication streams every table in the
// database; both checking modes must refuse it, and auto must not touch it.
func TestBootstrapPublication_RejectsAllTables(t *testing.T) {
	ctx := context.Background()
	for _, mode := range []string{"verify", "auto"} {
		t.Run(mode, func(t *testing.T) {
			db := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{true}}, {values: []any{42}}}}
			err := bootstrapPublication(ctx, db, bootstrapConfig{Mode: mode, PublicationName: "pub", Tables: []string{"public.a"}}, zerolog.Nop())
			if err == nil || !strings.Contains(err.Error(), "FOR ALL TABLES") {
				t.Fatalf("err = %v; want FOR ALL TABLES rejection", err)
			}
			if len(db.execSQL) != 0 {
				t.Fatalf("exec = %#v; want no DDL", db.execSQL)
			}
		})
	}
}
```

В `TestPrepareDatabaseUnit` (режим `verify`) вставить `{values: []any{false}},` перед `{values: []any{1}},` в `rowResults` первого `fakeBootstrapDB`:

```go
	db := &fakeBootstrapDB{rowResults: []fakeRow{
		{values: []any{"logical"}},
		{values: []any{"2"}},
		{values: []any{"2"}},
		{values: []any{"repl", true, false}},
		{values: []any{false}},
		{values: []any{1}},
		{values: []any{10}},
		{values: []any{2}},
	}}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/app/ -run 'TestBootstrapPublication|TestPrepareDatabaseUnit' -v`
Expected: FAIL — `verify missing` без «does not exist», `RejectsAllTables/verify` и `/auto` возвращают nil.

- [ ] **Step 3: Implement**

`internal/app/bootstrap.go`: добавить `"errors"` в импорты. Добавить над `bootstrapPublication`:

```go
// lookupPublication reports whether the publication exists and whether it
// was created FOR ALL TABLES.
func lookupPublication(ctx context.Context, conn bootstrapDB, name string) (exists, allTables bool, err error) {
	err = conn.QueryRow(ctx,
		"SELECT puballtables FROM pg_publication WHERE pubname = $1",
		name,
	).Scan(&allTables)
	if errors.Is(err, pgx.ErrNoRows) {
		return false, false, nil
	}
	if err != nil {
		return false, false, fmt.Errorf("publication existence check failed (%s): %w", name, err)
	}
	return true, allTables, nil
}

func errAllTablesPublication(name string) error {
	return fmt.Errorf(
		"publication %q is FOR ALL TABLES, which streams every table in the database — "+
			"recreate it with an explicit list: DROP PUBLICATION %s; CREATE PUBLICATION %s FOR TABLE ...",
		name, name, name,
	)
}
```

Заменить ветки `verify` и `auto` в `bootstrapPublication`:

```go
	case "verify":
		exists, allTables, err := lookupPublication(ctx, conn, cfg.PublicationName)
		if err != nil {
			return err
		}
		if !exists {
			return fmt.Errorf("publication %q does not exist — create it with an explicit FOR TABLE list", cfg.PublicationName)
		}
		if allTables {
			return errAllTablesPublication(cfg.PublicationName)
		}
		var tableCount int
		if err := conn.QueryRow(ctx,
			"SELECT count(*) FROM pg_publication_tables WHERE pubname = $1",
			cfg.PublicationName,
		).Scan(&tableCount); err != nil {
			return fmt.Errorf("publication existence check failed (%s): %w", cfg.PublicationName, err)
		}
		if tableCount == 0 {
			return fmt.Errorf("publication %q exists but has no tables — create publication and add tables", cfg.PublicationName)
		}
		logger.Info().
			Str("publication", cfg.PublicationName).
			Int("table_count", tableCount).
			Msg("publication check passed")
	case "auto":

		if cfg.CreateRoles {
			bootstrapEnsureRole(ctx, conn, cfg.ReplicationDSN, true, logger)
			bootstrapEnsureRole(ctx, conn, cfg.PostgresDSN, false, logger)
		}

		exists, allTables, err := lookupPublication(ctx, conn, cfg.PublicationName)
		if err != nil {
			return err
		}
		if exists {
			if allTables {
				return errAllTablesPublication(cfg.PublicationName)
			}
			var tableCount int
			if err := conn.QueryRow(ctx,
				"SELECT count(*) FROM pg_publication_tables WHERE pubname = $1",
				cfg.PublicationName,
			).Scan(&tableCount); err != nil {
				return fmt.Errorf("publication existence check failed (%s): %w", cfg.PublicationName, err)
			}
			if tableCount == 0 {
				logger.Warn().
					Str("publication", cfg.PublicationName).
					Msg("publication exists with zero tables; nothing to stream yet — clients will see no events until tables are added")
			} else {
				logger.Info().
					Str("publication", cfg.PublicationName).
					Int("table_count", tableCount).
					Msg("publication check passed")
			}
			// wal.Config validation guarantees a non-empty table list in auto mode.
			bootstrapVerifyTables(ctx, conn, cfg.PublicationName, cfg.Tables, logger)
		} else {

			ddl := fmt.Sprintf(
				"CREATE PUBLICATION %s FOR TABLE %s WITH (publish = 'insert, update, delete')",
				cfg.PublicationName,
				strings.Join(cfg.Tables, ", "),
			)
			if _, err := conn.Exec(ctx, ddl); err != nil {
				return fmt.Errorf("failed to create publication %q for bootstrap: %w", cfg.PublicationName, err)
			}
			logger.Info().
				Str("publication", cfg.PublicationName).
				Str("mode", "FOR TABLE").
				Int("table_count", len(cfg.Tables)).
				Msg("publication created (bootstrap.mode=auto)")
		}
```

`test/integration/pg.go`: удалить функцию `(p *PG) CreatePublication` целиком и функцию `joinIdents` (используется только в ней). Проверка: `grep -rn "joinIdents\|CreatePublication" test/` — пусто.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/app/ && go vet -tags=integration ./test/integration/...`
Expected: PASS, vet без ошибок (интеграционный пакет компилируется без удалённых функций).

- [ ] **Step 5: Docs**

- `docs/operations.md:103-105`: заменить «Use an explicit table list. `FOR ALL TABLES` is supported by PostgreSQL but is discouraged because it makes the streamed surface opaque.» на «Use an explicit table list. Walera refuses to start (`verify` and `auto` modes) if the publication is `FOR ALL TABLES`: it would stream every table in the database, including ones no client may read.»
- `llm.txt:91`: комментарий `# schema-qualified; preferred over FOR ALL TABLES` → `# schema-qualified; required in auto mode`.
- `llm.txt:98-99`: пункт «`CREATE PUBLICATION ... FOR TABLE` from the configured list, or fall back to `FOR ALL TABLES` when the list is empty.» → «`CREATE PUBLICATION ... FOR TABLE` from the configured list (the list is required). An existing `FOR ALL TABLES` publication is rejected.»
- `deploy/POSTGRES-SETUP.md`, таблица режимов: в строку `auto` и в строку `verify` добавить «Fails if the publication is `FOR ALL TABLES`.»
- `CHANGELOG.md` → `### Security`, расширить существующий пункт «**No more `FOR ALL TABLES`.**» последним предложением: «An existing `FOR ALL TABLES` publication now also fails startup in `verify` and `auto` modes.»

- [ ] **Step 6: Commit**

```bash
git add internal/app/bootstrap.go internal/app/bootstrap_test.go test/integration/pg.go docs/operations.md llm.txt deploy/POSTGRES-SETUP.md CHANGELOG.md
git commit -m "fix(bootstrap): refuse an existing FOR ALL TABLES publication"
```

---

### Task 3: Удалённые ключи конфига — ошибка старта

**Files:**
- Modify: `internal/app/config.go` (`LoadAppConfig`, новые `removedConfigKeys`, `rejectRemovedKeys`)
- Test: `internal/app/config_test.go` (новый `TestLoad_RejectsRemovedKeys`)
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: `config.LoadKoanf` (`internal/config/config.go`), хелперы тестов `setPhase3RequiredEnv`, `writeTempYAML`.
- Produces: `rejectRemovedKeys(k *koanf.Koanf) error`.

Почему: ключи HMAC-подписи, circuit breaker, пула SSE-писателей и `shutdown.drain_deadline` удалены, но koanf принимает их без ошибки. Оператор с `WALERA_AUTH_SIGNING_SECRET` считает, что refresh подписан, — а подписи больше нет.

Формы ключей: YAML даёт вложенные пути (`auth.signing.secret`), env — после `envTransform` в `internal/config/config.go` заменяется только первый `_` (`WALERA_AUTH_SIGNING_SECRET` → `auth.signing_secret`, `WALERA_HTTP_POOL_FACTOR` → `http.pool_factor`). Поэтому сопоставление по **префиксу**.

- [ ] **Step 1: Write the failing test**

Добавить в `internal/app/config_test.go`:

```go
func TestLoad_RejectsRemovedKeys(t *testing.T) {
	cases := []struct {
		name    string
		yaml    string
		envKey  string
		wantKey string
	}{
		{"yaml signing", "auth:\n  signing:\n    secret: s3cr3t-value\n", "", "auth.signing.secret"},
		{"yaml breaker", "auth:\n  breaker:\n    cooldown: 30s\n", "", "auth.breaker.cooldown"},
		{"yaml health channel", "auth:\n  health_channel: _health\n", "", "auth.health_channel"},
		{"yaml drain deadline", "shutdown:\n  drain_deadline: 8s\n", "", "shutdown.drain_deadline"},
		{"yaml pool factor", "http:\n  pool_factor: 2\n", "", "http.pool_factor"},
		{"yaml batching", "http:\n  batching_disabled: true\n", "", "http.batching_disabled"},
		{"env signing secret", "", "WALERA_AUTH_SIGNING_SECRET", "auth.signing_secret"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setPhase3RequiredEnv(t)
			if tc.envKey != "" {
				t.Setenv(tc.envKey, "s3cr3t-value")
			}
			path := writeTempYAML(t, "database:\n  url: \"postgres://a:b@localhost/db\"\n"+tc.yaml)
			_, err := LoadAppConfig(path)
			if err == nil {
				t.Fatal("LoadAppConfig: err = nil; want removed-key rejection")
			}
			if !strings.Contains(err.Error(), "removed config keys") || !strings.Contains(err.Error(), tc.wantKey) {
				t.Errorf("err = %q; want it to name removed key %q", err.Error(), tc.wantKey)
			}
			if strings.Contains(err.Error(), "s3cr3t-value") {
				t.Errorf("err = %q; must not echo the key's value", err.Error())
			}
		})
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/app/ -run TestLoad_RejectsRemovedKeys -v`
Expected: FAIL — `LoadAppConfig: err = nil` во всех подтестах.

- [ ] **Step 3: Implement**

`internal/app/config.go`: добавить `"slices"` в импорты. Добавить после `applyAppDefaults`:

```go
// removedConfigKeys are keys from earlier releases that no longer do anything.
// Setting one fails startup so nobody believes a removed control (HMAC
// signing, the circuit breaker, the writer pool) is still in effect. Prefix
// match covers both the YAML (auth.signing.secret) and the env
// (WALERA_AUTH_SIGNING_SECRET -> auth.signing_secret) spellings.
var removedConfigKeys = []string{
	"auth.signing",
	"auth.breaker",
	"auth.health_channel",
	"shutdown.drain_deadline",
	"http.pool_factor",
	"http.sub_queue_size",
	"http.max_wait_ms",
	"http.drain_threshold_subs",
	"http.max_batch_bytes_per_sub",
	"http.batching_disabled",
}

// rejectRemovedKeys names offending keys only; values may be secrets.
func rejectRemovedKeys(k *koanf.Koanf) error {
	var found []string
	for _, key := range k.Keys() {
		for _, prefix := range removedConfigKeys {
			if strings.HasPrefix(key, prefix) {
				found = append(found, key)
				break
			}
		}
	}
	if len(found) == 0 {
		return nil
	}
	slices.Sort(found)
	return fmt.Errorf("removed config keys are set: %s — delete them (see CHANGELOG.md)", strings.Join(found, ", "))
}
```

В `LoadAppConfig` сразу после `config.LoadKoanf`:

```go
	k, err := config.LoadKoanf(path, applyAppDefaults)
	if err != nil {
		return nil, err
	}
	if err := rejectRemovedKeys(k); err != nil {
		return nil, fmt.Errorf("config: %w", err)
	}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/app/ ./internal/config/`
Expected: PASS (включая все существующие тесты загрузки конфига).

- [ ] **Step 5: Changelog**

`CHANGELOG.md` → `### Changed`, новый пункт:

```markdown
- **Removed config keys fail startup.** `auth.signing.*`, `auth.breaker.*`,
  `auth.health_channel`, `shutdown.drain_deadline` and the former SSE
  writer-pool keys (`http.pool_factor`, `http.sub_queue_size`,
  `http.max_wait_ms`, `http.drain_threshold_subs`,
  `http.max_batch_bytes_per_sub`, `http.batching_disabled`) — and their
  `WALERA_*` env forms — are rejected with an error naming the key.
```

- [ ] **Step 6: Commit**

```bash
git add internal/app/config.go internal/app/config_test.go CHANGELOG.md
git commit -m "fix(config): fail startup on removed config keys"
```

---

### Task 4: Жизненный цикл SSE-подписчика

**Files:**
- Modify: `internal/sse/stream.go` (`headersAndPreamble`, `stream`)
- Modify: `internal/router/subscriber.go:22-37` (удалить `SubscriberConfig.BufferCap`)
- Modify (удалить строки `BufferCap: N,`): `internal/router/index_test.go`, `internal/router/subscriber_test.go`, `internal/router/wildcard_test.go`, `internal/router/shutdown_test.go`, `internal/router/router_test.go`, `internal/auth/subscriber_panic_test.go`, `internal/auth/subscriber_test.go`
- Test: `internal/sse/stream_test.go` (три новых теста), `internal/sse/handler_test.go` (переименование)
- Modify: `docs/operations.md:319`, `docs/perf-notes.md:38-46`, `CHANGELOG.md`

**Interfaces:**
- Consumes: `gatedWriter`, `fakeBroadcaster`, `newTestHandler` из тестов `internal/sse`.
- Produces: инварианты, на которые опирается Task 7:
  (1) когда клиент получил ответ (первый flush), подписчик уже зарегистрирован в broadcaster;
  (2) `router.Subscriber.Done()` закрывается только через `Drop`, поэтому после `Done` `Reason()` всегда непустой.

Почему:
- `stream` пишет и flush'ит prelude **до** `h.bc.Register(sub)`. Клиент видит `200` и prelude, пишет строку в БД, а router её ещё не может доставить — событие теряется. Это и есть основная причина нестабильности `Test16TxScopedDelivery`.
- Родитель `router.Subscriber` — `r.Context()`: при отмене запроса `Done` закрывается раньше, чем `Drop` выставит `Reason`. `stream` сам вызывает `sub.Drop` при `<-r.Context().Done()`, поэтому привязка к контексту запроса не нужна.
- Первая проверка `Done` в цикле `stream` (сразу после чтения кадра) избыточна: до второй (после сбора пачки) ничего не пишется.
- `SubscriberConfig.BufferCap` — мёртвое поле: ёмкость очереди берётся из `router.exact_buffer`/`wildcard_buffer`.

- [ ] **Step 1: Write the failing tests**

В `internal/sse/stream_test.go` добавить (импорты `auth` и `pglogrepl` не нужны; `router`, `httptest`, `context`, `strings`, `http` уже есть):

```go
// Once the client has the response, it must already be routable: a row it
// writes right after connecting is never missed.
func TestStream_RegistersBeforeFirstFlush(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	gate := make(chan struct{})
	gw := &gatedWriter{hdr: http.Header{}, gate: gate, blocked: make(chan struct{})}
	sub := router.NewSubscriber(router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: "users", PK: "42"},
		router.SubscriberDeps{Parent: context.Background()})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	done := make(chan struct{})
	go func() { defer close(done); kit.h.stream(gw, r, sub, nil) }()

	<-gw.blocked // the first write is parked: nothing has reached the client yet
	kit.bc.mu.Lock()
	registered := len(kit.bc.subs)
	kit.bc.mu.Unlock()
	close(gate)
	sub.Drop("test")
	<-done
	if registered != 1 {
		t.Fatalf("registered subscribers at first write = %d; want 1", registered)
	}
}

// shutdownBroadcaster mimics a closed router.Broadcaster: Register drops the
// late registrant with "shutdown".
type shutdownBroadcaster struct{ fakeBroadcaster }

func (s *shutdownBroadcaster) Register(sub *router.Subscriber) {
	s.fakeBroadcaster.Register(sub)
	sub.Drop("shutdown")
}

// A handshake racing service shutdown still gets the prelude and then an
// explicit shutdown frame, not a bare disconnect.
func TestStream_LateRegistrantGetsShutdownFrame(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	kit.h.bc = &shutdownBroadcaster{}
	gw := &gatedWriter{hdr: http.Header{}, blocked: make(chan struct{})}
	sub := router.NewSubscriber(router.SubscriberConfig{Kind: router.KindExact, Schema: "public", Table: "users", PK: "42"},
		router.SubscriberDeps{Parent: context.Background()})
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil)
	kit.h.stream(gw, r, sub, nil)

	body := gw.String()
	if !strings.HasPrefix(body, testPrelude) {
		t.Fatalf("body = %q; want prelude first", body)
	}
	if !strings.Contains(body, "event: shutdown") {
		t.Fatalf("body = %q; want a shutdown frame", body)
	}
}
```

В `internal/sse/handler_test.go` добавить (нужны импорты `auth`, `router` — уже есть в файле):

```go
// The subscriber's Done must close only through Drop, so Reason is always set
// once Done fires — even when the request context is cancelled first.
func TestHeadersAndPreamble_SubscriberOutlivesRequestContext(t *testing.T) {
	t.Parallel()
	kit := newTestHandler(t, nil, nil)
	ctx, cancel := context.WithCancel(context.Background())
	r := httptest.NewRequest(http.MethodGet, "/sse/v1/users/42", nil).WithContext(ctx)
	hs := handshakeResult{
		authMap: &auth.Whitelist{UserID: "u1", Tables: map[string]map[string]struct{}{"users": {"id": {}}}},
		userID:  "u1",
	}
	sub, err := kit.h.headersAndPreamble(httptest.NewRecorder(), r, "users", "42", "users:42", router.KindExact, 0, hs)
	if err != nil {
		t.Fatalf("headersAndPreamble: %v", err)
	}
	cancel()
	select {
	case <-sub.Done():
		t.Fatalf("Done closed by request cancellation (Reason=%q); want it closed only by Drop", sub.Reason())
	default:
	}
	sub.Drop("client_closed")
	if got := sub.Reason(); got != "client_closed" {
		t.Fatalf("Reason = %q; want client_closed", got)
	}
}
```

Там же переименовать `TestHandshake_HappyPath_RegistersSubscriberWithFilter` → `TestHandshake_HappyPath_SetsFilter` (тело не менять).

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/sse/ -run 'TestStream_RegistersBeforeFirstFlush|TestStream_LateRegistrantGetsShutdownFrame|TestHeadersAndPreamble_SubscriberOutlivesRequestContext' -v`
Expected: FAIL — `registered subscribers at first write = 0`; `Done closed by request cancellation`. `LateRegistrantGetsShutdownFrame` может пройти и сейчас (он фиксирует поведение, которое перестановка `Register` не должна сломать).

- [ ] **Step 3: Implement**

`internal/sse/stream.go`, `headersAndPreamble`:

```go
	sub = router.NewSubscriber(
		router.SubscriberConfig{
			Kind:     kind,
			Schema:   "public",
			Table:    table,
			PK:       pk,
			StartLSN: startLSN,
		},
		// Not tied to the request: stream drops the subscriber itself when the
		// client goes away, so Done never fires without a Reason.
		router.SubscriberDeps{Parent: context.WithoutCancel(r.Context())},
	)
```

(добавить `"context"` в импорты `stream.go`, если его нет).

`internal/sse/stream.go`, `stream` — `Register` перед первой записью:

```go
	// Registered before the first flush: once the client sees the response,
	// rows written after that moment reach it.
	h.bc.Register(sub)
	defer h.bc.Deregister(sub)
	if err := write(preludeFrame, initialFrame); err != nil {
		reason = writeFailureReason(err)
		sub.Drop(reason)
		return
	}
```

(удалить прежние строки `if err := write(preludeFrame, initialFrame) … }` и `h.bc.Register(sub)` / `defer h.bc.Deregister(sub)`, стоявшие после неё).

`internal/sse/stream.go`, `stream`, ветка `case frame := <-queue:` — одна проверка `Done` после сбора пачки:

```go
		case frame := <-queue:
			frames := [][]byte{frame}
		batch:
			for len(frames) < queueCap {
				select {
				case f := <-queue:
					frames = append(frames, f)
				default:
					break batch
				}
			}
			// Frames queued before a drop may carry a revoked grant.
			select {
			case <-sub.Done():
				finish()
				return
			default:
			}
			if err := write(frames...); err != nil {
				reason = writeFailureReason(err)
				sub.Drop(reason)
				return
			}
			h.metrics.EventsSent(kind).Add(float64(len(frames)))
			hb.Reset(h.cfg.HeartbeatInterval)
```

`internal/router/subscriber.go`: удалить из `SubscriberConfig` поле `BufferCap int` (и пустую строку перед ним). Затем удалить его из тестов и выровнять:

```bash
grep -rl "BufferCap" internal | xargs sed -i '/^[[:space:]]*BufferCap:/d'
gofmt -w internal/router internal/auth
grep -rn "BufferCap" --include=*.go . 
```

Последняя команда должна ничего не вывести.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go vet ./... && go test -race ./internal/sse/ ./internal/router/ ./internal/auth/`
Expected: PASS, включая `TestStream_NoFramesAfterRevokeErrorFrame` (он покрывает единственную оставшуюся проверку `Done`).

Дополнительно прогнать новые тесты на устойчивость: `go test -race -count=50 ./internal/sse/ -run 'TestStream_RegistersBeforeFirstFlush|TestStream_LateRegistrantGetsShutdownFrame|TestHeadersAndPreamble_SubscriberOutlivesRequestContext|TestStream_NoFramesAfterRevokeErrorFrame'` — PASS все 50 раз.

- [ ] **Step 5: Docs**

- `docs/operations.md:319`, колонка описания `WALERA_HTTP_WRITE_TIMEOUT`: `Per-frame write deadline.` → `Write deadline per flush (one flush may carry a batch of queued frames).`
- `docs/perf-notes.md`: сразу под таблицей CPU-профиля (после строки с `runtime.mallocgc / scanobject`) добавить строку:

```markdown
> Profiled before the SSE writer pool was removed (2026-10-28): `poolWorker` no longer exists — each handler goroutine now writes its own stream.
```

- `CHANGELOG.md` → `### Fixed` (создать подраздел после `### Changed`, если его нет):

```markdown
- **No missed events right after connecting.** A subscriber is now
  registered before the first byte of the response is flushed, so a row
  written as soon as the client sees `200` is delivered.
```

- [ ] **Step 6: Commit**

```bash
git add internal/sse internal/router internal/auth docs/operations.md docs/perf-notes.md CHANGELOG.md
git commit -m "fix(sse): register subscribers before the first flush"
```

---

### Task 5: Refresh и отключение клиента — без ложных сигналов; документация auth

**Files:**
- Modify: `internal/auth/client.go` (`doAndDecode`; `switch` по статусу — без изменений тут, это Task 8)
- Modify: `internal/auth/subscriber.go` (`tryRefresh`)
- Test: `internal/auth/client_test.go`, `internal/auth/subscriber_test.go`
- Delete test: `TestClient_OpenSession_401PropagatesBody` в `internal/auth/client_session_test.go` (дубль `TestClient_OpenSession_StatusPropagation/401`)
- Modify: `docs/auth.md` (таблица полей ответа, ~строка 158), `docs/adr/0002-auth-model.md:52-53`, `spec/02-authorization.md` (§2.5–2.6), `spec/08-observability.md` (строки про breaker), `CHANGELOG.md`

**Interfaces:**
- Consumes: `newTestClient`, `gatherCounter` (`client_test.go`); `newScriptServer`, `newSubTestClient`, `newTestSubscriber`, `subscriberTestOpts`, `makeMap`, `threadSafeBuf`, `gatherAuthRefreshCount` (`subscriber_test.go`).
- Produces: `OpenSession` при отменённом вызывающим контексте возвращает `*ErrUnavailable`, но **не** инкрементирует `walera_auth_requests_total` и не пишет warn; `tryRefresh` при отменённом контексте выходит молча.

Почему: таймаут бэкенда реализован через `http.Client.Timeout` и **не** отменяет `req.Context()`. Значит, `req.Context().Err() != nil` после ошибки однозначно означает «вызывающий ушёл» (клиент отключился во время handshake или refresh). Сейчас это считается `result="unavailable"` и попадает в числитель алерта `WaleraAuthBackendUnavailable`, а refresh пишет warn «auth permission refresh failed».

- [ ] **Step 1: Write the failing tests**

`internal/auth/client_test.go`:

```go
// A caller that went away (client disconnect) is not a backend failure: it
// must not feed walera_auth_requests_total{result="unavailable"}.
func TestClient_OpenSession_CallerCancelledNotCounted(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(validBody))
	}))
	t.Cleanup(srv.Close)
	c, mc := newTestClient(t, srv.URL, time.Second)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := c.OpenSession(ctx, "tok", ForwardedAuth{}, "users:1", "req-1")
	var ue *ErrUnavailable
	if !errors.As(err, &ue) {
		t.Fatalf("err = %v; want *ErrUnavailable", err)
	}
	if v := gatherCounter(t, mc, "walera_auth_requests_total", "result", "unavailable"); v != 0 {
		t.Errorf("auth_requests_total{unavailable} = %v; want 0 for a cancelled caller", v)
	}
}
```

`internal/auth/subscriber_test.go`:

```go
// A refresh cut short by the client disconnecting is silent: no warn log, no
// refresh metric, no drop.
func TestSubscriber_TryRefresh_CancelledContextIsSilent(t *testing.T) {
	t.Parallel()
	srv := newScriptServer(t, func(_ int64, w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(permissionsBody(t, map[string][]string{"orders": {"id"}}))
	})
	c, mc := newSubTestClient(t, srv.srv.URL)
	var buf threadSafeBuf
	s := newTestSubscriber(t, subscriberTestOpts{
		Client:     c,
		InitialMap: makeMap(t, 100, "id"),
		Metrics:    mc,
		Log:        zerolog.New(&buf),
	})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s.tryRefresh(ctx)

	if buf.Len() != 0 {
		t.Errorf("log = %q; want nothing for a cancelled refresh", buf.String())
	}
	if got := gatherAuthRefreshCount(t, mc, "unavailable"); got != 0 {
		t.Errorf("auth_refresh_total{unavailable} = %v; want 0", got)
	}
	select {
	case <-s.Sub.Done():
		t.Errorf("subscriber dropped with %q; want untouched", s.Sub.Reason())
	default:
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `go test ./internal/auth/ -run 'TestClient_OpenSession_CallerCancelledNotCounted|TestSubscriber_TryRefresh_CancelledContextIsSilent' -v`
Expected: FAIL — `auth_requests_total{unavailable} = 1`; непустой лог и `auth_refresh_total{unavailable} = 1`.

- [ ] **Step 3: Implement**

`internal/auth/client.go`, начало `doAndDecode`:

```go
func (c *Client) doAndDecode(req *http.Request, requestID string) (*Whitelist, error) {
	resp, err := c.hc.Do(req)
	if err != nil {
		// The backend timeout is http.Client.Timeout, which leaves the request
		// context alone: a done context means the caller left, not an outage.
		if req.Context().Err() != nil {
			return nil, &ErrUnavailable{Cause: err}
		}
		c.mc.AuthRequests("unavailable").Inc()
		c.log.Warn().
			Str("auth_request_id", requestID).
			Err(err).
			Msg("auth: HTTP dispatch failed")
		return nil, &ErrUnavailable{Cause: err}
	}
```

`internal/auth/subscriber.go`, `tryRefresh` — выход сразу после `OpenSession`, до метрики и лога:

```go
func (s *Subscriber) tryRefresh(ctx context.Context) {
	for attempt := 0; ; attempt++ {
		fresh, err := s.client.OpenSession(ctx, s.bearer, s.fwd, s.channel, newRequestID())
		if ctx.Err() != nil {
			return // the stream is gone; nothing to refresh or report
		}
		s.recordRefreshResult(err)
		if err != nil {
			s.logRefreshError(err, attempt)
		}
```

(остальное тело функции не менять).

`internal/auth/client_session_test.go`: удалить функцию `TestClient_OpenSession_401PropagatesBody` целиком; если после этого какие-то импорты файла становятся неиспользуемыми — удалить их.

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test -race ./internal/auth/ ./internal/sse/`
Expected: PASS.

- [ ] **Step 5: Docs**

- `docs/auth.md`, строка таблицы `ttl_seconds`: `Refresh interval Walera uses for this permission map.` → `Refresh interval, read from the handshake response only. A different value in a refresh response is ignored until the client reconnects. Used only when auth.default_ttl_seconds > 0.`
- `docs/adr/0002-auth-model.md:52-53`: `circuit-breaker behavior described in [Auth](../auth.md#circuit-breaker-behavior).` → `fail-closed outage behavior described in [Auth](../auth.md#backend-outage).`
- `spec/02-authorization.md`: сразу под заголовком `## 2.6. Circuit breaker` вставить:

```markdown
> **Superseded (2026-10-28).** Walera has no circuit breaker and no fail-open
> window: refresh is fail-closed and re-calls `/auth/sessions`. Current
> behavior: [docs/auth.md — Backend outage](../docs/auth.md#backend-outage).
> The text below is kept as design history.
```

  и в §2.5 в конец абзаца «A separate, **bounded** fail-open exists…» добавить предложение `(Superseded — see §2.6.)`.
- `spec/08-observability.md`: в конец строк про `auth_circuit_breaker_state` (секция метрик), `auth_breaker_stale_subscribers` и двух алертов про breaker добавить ` — *removed 2026-10-28 with the circuit breaker; see [docs/auth.md](../docs/auth.md#backend-outage).*`
- `CHANGELOG.md` → `### Fixed`:

```markdown
- **Client disconnects no longer count as auth-backend failures.** A
  handshake or refresh abandoned because the client went away no longer
  increments `walera_auth_requests_total{result="unavailable"}` or logs
  `auth permission refresh failed`, so `WaleraAuthBackendUnavailable`
  is not triggered by ordinary reconnect churn.
```

- [ ] **Step 6: Commit**

```bash
git add internal/auth docs/auth.md docs/adr/0002-auth-model.md spec/02-authorization.md spec/08-observability.md CHANGELOG.md
git commit -m "fix(auth): do not report a departed caller as backend unavailability"
```

---

### Task 6: Детерминированная проверка остановки readiness-пробы

**Files:**
- Create: `internal/health/leak_main_test.go`
- Modify: `internal/health/server_test.go` (`TestStartReadinessProbe_UpdatesCacheOverTime`, строки ~300-307)

**Interfaces:**
- Consumes: `go.uber.org/goleak` (уже в `go.mod`, используется в `internal/sse/leak_main_test.go`).
- Produces: —

Почему: тест проверяет остановку пробы через окна `time.Sleep` (30 мс + 100 мс) и допуск «+1 вызов». goleak в `TestMain` ловит неостановленную горутину детерминированно (он сам ждёт и повторяет проверку).

- [ ] **Step 1: Add the leak gate**

Создать `internal/health/leak_main_test.go`:

```go
package health

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m,
		goleak.IgnoreCurrent(),
	)
}
```

- [ ] **Step 2: Run the package to see the gate is green before the edit**

Run: `go test -race -count=1 ./internal/health/`
Expected: PASS (все тесты пакета отменяют контексты пробы; если goleak что-то нашёл — это реальная утечка, починить в тесте-виновнике через `defer cancel()`).

Проверка, что гейт работает: временно закомментировать `case <-ctx.Done(): return` в `internal/health/readyz.go` — `go test ./internal/health/` должен упасть с `found unexpected goroutines`. Вернуть строку обратно.

- [ ] **Step 3: Remove the sleep-based assertion**

В `TestStartReadinessProbe_UpdatesCacheOverTime` удалить хвост, начиная с комментария `// The probe goroutine must stop after cancel; allow one in-flight probe.` и до конца функции, заменив на:

```go
	// Probe shutdown on cancel is enforced by goleak in TestMain.
	cancel()
}
```

- [ ] **Step 4: Run tests**

Run: `go test -race -count=3 ./internal/health/`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/health/leak_main_test.go internal/health/server_test.go
git commit -m "test(health): check probe shutdown with goleak instead of sleeps"
```

---

### Task 7: Интеграционные тесты — harness, большая транзакция, падение auth

**Files:**
- Modify: `test/integration/pg.go` (новый `(*PG).WaitForActiveSlot`)
- Modify: `test/integration/harness.go` (`NewHarness`)
- Rewrite: `test/integration/15_large_tx_test.go`
- Modify: `test/integration/04_auth_lifecycle_test.go` (новый подтест)
- Modify: `testbench/scripts/smoke-09.sh:174` (комментарий)

**Interfaces:**
- Consumes: инварианты Task 4 (регистрация до первого flush); `MockAuth.FailMode(bool)`, `MockAuth.SetTTL`, `rawHandshake`, `scrapeMetric`, `waitForMetric`, `readTxEvent`, `querySlotLag`, `confirmedSlotDrained`.
- Produces: `func (p *PG) WaitForActiveSlot(t *testing.T, prefix string, deadline time.Duration)`.

Почему:
- `SpawnBinary` ждёт только TCP-порт, а слот репликации создаётся параллельно с HTTP-листенером. Строка, вставленная до старта стриминга, не попадёт ни в один слот.
- `TestLargeTransactionBackpressure` падает всегда: он ждёт «тихий multi_root-дроп» у wildcard-подписчика, а после раунда 2 wildcard получает multi-root транзакции; при `max_changes_per_tx: 1000` в harness 10 000 строк дают `tx_too_large` (подтверждено прогоном: `subscriber received error event … {"reason":"tx_too_large"}`). Кроме того, он мерил heap **процесса теста**, а не Walera — эта проверка ничего не доказывала.
- fail-closed при падении auth-бэкенда покрыт только unit-тестами; `MockAuth.FailMode` никем не вызывается.

- [ ] **Step 1: Harness waits for the slot**

`test/integration/pg.go` — добавить метод (импорты `context`, `time`, `pgx` в файле уже есть; если `time` нет — добавить):

```go
// WaitForActiveSlot blocks until Walera's replication slot is streaming, so
// rows a test writes afterwards are always captured. Walera starts its HTTP
// listener concurrently with slot creation.
func (p *PG) WaitForActiveSlot(t *testing.T, prefix string, deadline time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), deadline)
	defer cancel()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		t.Fatalf("WaitForActiveSlot: connect: %v", err)
	}
	defer conn.Close(context.Background()) //nolint:errcheck
	for {
		var active bool
		err := conn.QueryRow(ctx,
			"SELECT EXISTS(SELECT 1 FROM pg_replication_slots WHERE slot_name LIKE $1 AND active)",
			prefix+"%",
		).Scan(&active)
		if err == nil && active {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatalf("replication slot %s* not active within %v (last err: %v)", prefix, deadline, err)
		case <-time.After(50 * time.Millisecond):
		}
	}
}
```

`test/integration/harness.go`, `NewHarness` — после `SpawnBinary`:

```go
	bin := SpawnBinary(t, pg.DSN, pg.ReplicationDSN(), auth.URL(), opts...)
	pg.WaitForActiveSlot(t, "walera_test", 15*time.Second)
	client := NewClient(bin.BaseURL())
```

(префикс `walera_test` — значение `slot_name_prefix` из конфига, который генерирует `SpawnBinary`).

- [ ] **Step 2: Rewrite the large-tx test**

Заменить содержимое `test/integration/15_large_tx_test.go` целиком:

```go
//go:build integration

package integration

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
)

func querySlotLag(t *testing.T, p *PG, prefix string) (int64, bool) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, p.DSN)
	if err != nil {
		t.Fatalf("querySlotLag: connect: %v", err)
	}
	defer conn.Close(ctx) //nolint:errcheck
	var lag int64
	err = conn.QueryRow(ctx, `
        SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), COALESCE(confirmed_flush_lsn, pg_current_wal_lsn()))::bigint
        FROM pg_replication_slots
        WHERE slot_name LIKE $1
        ORDER BY slot_name LIMIT 1
    `, prefix+"%").Scan(&lag)
	if err == pgx.ErrNoRows {
		return 0, false
	}
	if err != nil {
		t.Fatalf("querySlotLag: query: %v", err)
	}
	return lag, true
}

func confirmedSlotDrained(t *testing.T, p *PG, prefix string, tolerance int64, deadline time.Duration) (int64, bool) {
	t.Helper()
	end := time.Now().Add(deadline)
	var last int64
	for time.Now().Before(end) {
		lag, ok := querySlotLag(t, p, prefix)
		if !ok {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		last = lag
		if lag <= tolerance {
			return lag, true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return last, false
}

// TestLargeTransactionBackpressure (WAL-05): one Postgres transaction with
// 10 000 row changes must not wedge the pipeline. The harness caps
// router.max_changes_per_tx at 1000, so the wildcard subscriber is dropped
// with an explicit tx_too_large error frame (never a silent truncation), the
// replication slot still drains, and the service keeps serving new
// subscribers.
func TestLargeTransactionBackpressure(t *testing.T) {
	h := NewHarness(t)
	h.Auth.SetMap(
		"test-token", "test-user",
		[]string{"users"},
		map[string][]string{"users": {"id", "email", "name"}},
	)

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	events, errCh, closeFn := h.Client.Connect(ctx, "users/all", "test-token")
	defer closeFn()

	metricsURL := h.Binary.BaseURL() + "/metrics"
	baseTooLarge, err := scrapeMetric(ctx, metricsURL, `walera_tx_dropped_total{reason="tx_too_large"}`)
	if err != nil {
		t.Fatalf("scrape baseline tx_too_large: %v", err)
	}

	if err := h.PG.Exec(ctx, `
        INSERT INTO users (id, email, name)
        SELECT g + 10000, 'u' || g || '@x', repeat('n', 50)
        FROM generate_series(1, 10000) g
    `); err != nil {
		t.Fatalf("large insert: %v", err)
	}

	deadline := time.After(15 * time.Second)
wait:
	for {
		select {
		case ev, ok := <-events:
			if !ok {
				t.Fatalf("events closed without a tx_too_large error frame; stderr:\n%s", h.Binary.Stderr())
			}
			switch ev.Type {
			case "tx":
				t.Fatalf("oversized tx delivered (%d bytes); want tx_too_large drop", len(ev.Data))
			case "error":
				if !strings.Contains(string(ev.Data), `"tx_too_large"`) {
					t.Fatalf("error event = %s; want tx_too_large", ev.Data)
				}
				break wait
			}
		case err := <-errCh:
			t.Fatalf("client error before tx_too_large: %v", err)
		case <-deadline:
			t.Fatalf("no tx_too_large frame within 15s; stderr:\n%s", h.Binary.Stderr())
		}
	}

	if _, err := waitForMetric(ctx, t, metricsURL,
		`walera_tx_dropped_total{reason="tx_too_large"}`,
		func(v float64) bool { return v >= baseTooLarge+1 },
		5*time.Second, 50*time.Millisecond,
	); err != nil {
		t.Fatalf("tx_too_large counter did not increment (baseline=%v): %v", baseTooLarge, err)
	}

	if lag, drained := confirmedSlotDrained(t, h.PG, "walera_test_", 4*1024, 15*time.Second); !drained {
		t.Errorf("slot never drained to <= 4 KiB lag (last=%d bytes after 15s)", lag)
	}

	// The service keeps serving: a fresh wildcard subscriber gets a small tx.
	events2, errCh2, closeFn2 := h.Client.Connect(ctx, "users/all", "test-token")
	defer closeFn2()
	if err := h.PG.Exec(ctx,
		"INSERT INTO users (id, email, name) VALUES ($1, $2, $3)",
		1, "a@b.c", "Alice",
	); err != nil {
		t.Fatalf("follow-up insert: %v", err)
	}
	ev := readTxEvent(ctx, t, h, events2, errCh2)
	if len(ev.Changes) != 1 {
		t.Fatalf("follow-up tx changes = %d; want 1 (%+v)", len(ev.Changes), ev.Changes)
	}
}
```

Перед заменой проверить, что `querySlotLag`/`confirmedSlotDrained` больше нигде не объявлены (`grep -rn "func querySlotLag\|func confirmedSlotDrained" test/integration`) — если объявлены в другом файле, удалить их из этого файла, а не дублировать.

- [ ] **Step 3: Add the auth-outage subtest**

`test/integration/04_auth_lifecycle_test.go`: добавить `"net/http"` в импорты и новый подтест в `Test04AuthLifecycle` (после `Connect_OK_ThenRevoke_MidStream`):

```go
	// An auth-backend outage is fail-closed on both paths: a live stream is
	// dropped with auth_unavailable once its refresh retries run out, and a
	// new handshake gets 503.
	t.Run("Backend_Outage_FailsClosed", func(t *testing.T) {
		t.Parallel()
		h := NewHarness(t)
		h.Auth.SetMap("test-token", "test-user", []string{"users"},
			map[string][]string{"users": {"id"}})
		if err := h.Auth.SetTTL("test-token", 1); err != nil {
			t.Fatalf("SetTTL: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
		defer cancel()
		events, errCh, closeFn := h.Client.Connect(ctx, "users/42", "test-token")
		defer closeFn()

		h.Auth.FailMode(true)

		// First refresh within ~1.5s, then retries after 1s, 3s and 9s.
		deadline := time.After(30 * time.Second)
	wait:
		for {
			select {
			case ev, ok := <-events:
				if !ok {
					t.Fatalf("events closed before auth_unavailable; stderr:\n%s", h.Binary.Stderr())
				}
				if ev.Type == "error" {
					if !strings.Contains(string(ev.Data), "auth_unavailable") {
						t.Fatalf("error event = %s; want auth_unavailable", ev.Data)
					}
					break wait
				}
			case err := <-errCh:
				t.Fatalf("client error before auth_unavailable: %v", err)
			case <-deadline:
				t.Fatalf("no auth_unavailable within 30s; stderr:\n%s", h.Binary.Stderr())
			}
		}

		status, retryAfter := rawHandshake(ctx, h.Binary.BaseURL(), "users/42", "test-token")
		if status != http.StatusServiceUnavailable {
			t.Fatalf("handshake during outage: status = %d; want 503", status)
		}
		if retryAfter == "" {
			t.Error("503 during outage without Retry-After")
		}
	})
```

- [ ] **Step 4: smoke-09 comment**

`testbench/scripts/smoke-09.sh`, над строкой `# SC1.c — walera-overview dashboard: UID + 7 panels …` добавить:

```bash
# 7 panels: the "Auth circuit breaker state" panel went away with the breaker (2026-10-28).
```

Проверка синтаксиса: `bash -n testbench/scripts/smoke-09.sh`.

- [ ] **Step 5: Run integration tests**

```bash
make testbin/cdc-sse
go test -tags=integration -race -timeout 15m -count=1 -run 'TestLargeTransactionBackpressure|Test04AuthLifecycle' ./test/integration/...
go test -tags=integration -race -timeout 20m -count=5 -run 'Test16TxScopedDelivery' ./test/integration/...
go test -tags=integration -race -timeout 30m -count=1 ./test/integration/...
```

Expected: всё PASS; `Test16TxScopedDelivery` — 5 из 5. Если какой-то другой интеграционный тест падает — сначала проверить, падает ли он на `master` до этого плана (`git stash`-ом не пользоваться: временный worktree `git worktree add /tmp/walera-base master`), и указать это в отчёте, а не чинить вслепую.

- [ ] **Step 6: Commit**

```bash
git add test/integration testbench/scripts/smoke-09.sh
git commit -m "test(integration): wait for the slot, pin tx_too_large and auth-outage behaviour"
```

---

### Task 8: golangci-lint — рабочий конфиг v2, ноль замечаний, шаг в CI

**Files:**
- Rewrite: `.golangci.yml`
- Modify: `internal/auth/client.go` (`switch` по статусу; `//nolint` у копии cookie)
- Modify: `internal/sse/auth.go` (`writeAuthError`)
- Modify: `internal/app/lifecycle.go` (две горутины shutdown)
- Modify: `internal/wal/reader.go:91`, `internal/wal/mapper.go:331`
- Modify: `cmd/cdc-sse/bootstrap.go:19`, `cmd/writer/main.go:~308`
- Modify: `internal/router/router_test.go:75`
- Modify: `.github/workflows/checks.yml` (job `vet`), `Makefile` (сообщение в `lint`), `CLAUDE.md:81,111`

**Interfaces:**
- Consumes: —
- Produces: `golangci-lint run ./...` → `0 issues` с v2.15.0; CI запускает его.

Почему: lint ни разу не запускался после двух раундов. Зафиксированный v2.12.2 падает на `typecheck` с Go 1.26 («export data version 5 is greater than maximum supported version 4»). `.golangci.yml` объявлен как `version: "2"`, но использует ключи v1 (`linters-settings`, `issues.exclude-rules`) — v2 их игнорирует, поэтому не работают ни исключения для тестов, ни настройка `exhaustive`. На момент написания плана с исправленной схемой — 22 замечания (перечислены ниже); номера строк могут сдвинуться после Task 1–7.

- [ ] **Step 1: Install the linter and migrate the config**

```bash
go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0
```

Заменить `.golangci.yml` целиком:

```yaml
version: "2"

linters:
  enable:
    - errcheck
    - staticcheck
    - govet
    - gosec
    - ineffassign
    - unused
    - misspell
    - exhaustive
    - nilerr
    - bodyclose
  settings:
    exhaustive:
      # Require exhaustive type-switches on pgoutput message types.
      default-signifies-exhaustive: true
  exclusions:
    rules:
      # Test helpers routinely ignore errors and close bodies via t.Cleanup.
      - path: '_test\.go'
        linters:
          - errcheck
          - gosec
          - bodyclose
      # Load-generation tooling, not part of the shipped service.
      - path: '^(cmd/writer|cmd/loadgen|internal/writer)/'
        linters:
          - gosec
```

- [ ] **Step 2: Run to get the current list**

Run: `$(go env GOPATH)/bin/golangci-lint run ./...`
Expected (на момент написания, после исключений из Step 1 — примерно так): `cmd/writer/main.go` errcheck (приём из `commitDone`), `internal/router/router_test.go` exhaustive (`wal.OpDelete`), gosec G704 `cmd/cdc-sse/bootstrap.go`, G118 ×2 `internal/app/lifecycle.go`, G124 `internal/auth/client.go` (копия cookie), G705 ×3 `internal/sse/auth.go`, G404 `internal/wal/reader.go`, nilerr `internal/wal/mapper.go`, staticcheck QF1002 `internal/auth/client.go`.

- [ ] **Step 3: Fix each finding**

Исправлять по фактическому выводу Step 2. Для каждого замечания из списка выше:

1. `internal/auth/client.go`, `doAndDecode`, QF1002 — тегированный `switch`:

```go
	switch resp.StatusCode {
	case http.StatusOK:
		// тело ветки без изменений
	case http.StatusUnauthorized:
		// без изменений
	case http.StatusForbidden:
		// без изменений
	case http.StatusNotFound:
		// без изменений
	default:
		// без изменений
	}
```

(заменить только строку `switch {` на `switch resp.StatusCode {` и каждое `case resp.StatusCode == X:` на `case X:`).

2. `internal/auth/client.go`, `ForwardedFromRequest`, G124:

```go
				fwd.Cookies = append(fwd.Cookies, &http.Cookie{Name: ck.Name, Value: ck.Value}) //nolint:gosec // G124: outbound copy of a request cookie, never sent as Set-Cookie
```

3. `internal/sse/auth.go`, `writeAuthError`, G705 — три строки `_, _ = w.Write(e.Body)`:

```go
		_, _ = w.Write(e.Body) //nolint:gosec // G705: backend JSON, served as application/json with nosniff
```

4. `internal/app/lifecycle.go`, G118 — у двух `go func() {` в функции shutdown (pprof и main HTTP) поставить на строке `go func() {`:

```go
		go func() { //nolint:gosec // G118: shutdown runs after the parent ctx is cancelled and needs its own deadline
```

(для второй горутины отступ на один таб меньше — как в файле).

5. `internal/wal/reader.go:91`, G404:

```go
		rng: mathrand.New(mathrand.NewPCG(uint64(time.Now().UnixNano()), uint64(os.Getpid()))), //nolint:gosec // G404: reconnect backoff jitter, not security-sensitive
```

6. `internal/wal/mapper.go:331`, nilerr — у `return s, nil` внутри `if err1 != nil || err2 != nil || err3 != nil {`:

```go
			return s, nil //nolint:nilerr // an unparseable interval passes through verbatim by design
```

7. `cmd/cdc-sse/bootstrap.go:19`, G704:

```go
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz") //nolint:gosec // G704: loopback self-probe, port from our own config
```

8. `cmd/writer/main.go`, `gracefulShutdown`, errcheck:

```go
	select {
	case err := <-commitDone:
		if err != nil {
			logger.Warn().Err(err).Msg("commit loop exited with error")
		}
	case <-time.After(2 * time.Second):
	}
```

9. `internal/router/router_test.go:75`, exhaustive — добавить ветку в `switch op`:

```go
	switch op {
	case wal.OpInsert:
		c.Data = map[string]any{"id": pk}
	case wal.OpUpdate:
		c.Changed = map[string]any{"id": pk}
	case wal.OpDelete:
		// PK-only change: no column payload.
	}
```

Если Step 2 показал замечание, которого нет в этом списке: в продуктовом коде (`internal/`, кроме `internal/writer`, и `cmd/cdc-sse`) — исправить по существу; `//nolint` допустим только с конкретной причиной в той же строке. Новые исключения в `.golangci.yml` не добавлять.

- [ ] **Step 4: Verify zero issues and green tests**

Run: `$(go env GOPATH)/bin/golangci-lint run ./... && go vet ./... && go test -race ./... && make lint-comments && make comment-density`
Expected: `0 issues.`, тесты PASS, обе проверки комментариев OK.

- [ ] **Step 5: CI and pins**

`.github/workflows/checks.yml`, job `vet`: заменить шаги `Install staticcheck` и `staticcheck` (staticcheck входит в golangci-lint):

```yaml
      - name: Install golangci-lint
        run: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0

      - name: golangci-lint
        run: golangci-lint run ./...
```

и в шапке файла строку `#   - vet               go vet, staticcheck (via make lint)` → `#   - vet               go vet, golangci-lint`.

`Makefile`, цель `lint`: текст подсказки `Install v2: https://golangci-lint.run/welcome/install/` → `Install: go install github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.15.0`.

`CLAUDE.md`: строка 81 `### Build Tooling: golangci-lint v2 \`v2.12.2\`` → `### Build Tooling: golangci-lint v2 \`v2.15.0\``; строка 111 в таблице Dev Tools `v2.12.2` → `v2.15.0`, и в колонку Purpose дописать `(v2.12.2 cannot type-check Go 1.26)`.

Run: `$(go env GOPATH)/bin/actionlint .github/workflows/checks.yml` (или `actionlint -color .github/workflows/*.yml`, если установлен) — без ошибок.

- [ ] **Step 6: Commit**

```bash
git add .golangci.yml .github/workflows/checks.yml Makefile CLAUDE.md cmd internal
git commit -m "chore(lint): migrate golangci config to v2 schema and clear findings"
```

---

### Task 9: Финальная проверка (контроллер, без коммита кода)

**Files:**
- Modify (локально, не в git): `TODO.local.md`

- [ ] **Step 1: Full suite on the branch**

```bash
go vet ./... && go test -race ./...
$(go env GOPATH)/bin/golangci-lint run ./...
make testbin/cdc-sse && go test -tags=integration -race -timeout 30m -count=1 ./test/integration/...
```

Expected: всё зелёное; результаты (включая длительность интеграционного прогона) — в отчёт.

- [ ] **Step 2: Live testbench smoke 08/09**

```bash
cp -n testbench/.env.example testbench/.env
docker compose -f testbench/docker-compose.yml up -d --build
bash testbench/scripts/smoke-08.sh
bash testbench/scripts/smoke-09.sh
docker compose -f testbench/docker-compose.yml down -v --remove-orphans
```

Expected: оба скрипта завершаются с кодом 0. Если стек не поднимается (занятые порты, нет ресурсов) — зафиксировать вывод в отчёте как «не прогнано, причина …», не обходить.

- [ ] **Step 3: Update TODO.local.md**

Отметить `[x]` все пункты разделов 1–4 и пункт «Прогнать `golangci-lint`»; у пункта «`ttl_seconds` из ответа refresh» дописать «— задокументировано»; у «Нет детерминированного теста на drop во время сбора пачки» — «— первая проверка `Done` убрана, оставшуюся покрывает `TestStream_NoFramesAfterRevokeErrorFrame`». Раздел 5 и пункты «Запушить `master`» / «Обновить auth-бэкенд» не трогать.
