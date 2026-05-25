package app

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/rs/zerolog"
)

type fakeBootstrapDB struct {
	rowResults []fakeRow
	queryRows  []pgx.Rows
	queryErrs  []error
	execErrs   []error
	execSQL    []string
}

func (f *fakeBootstrapDB) QueryRow(context.Context, string, ...any) pgx.Row {
	if len(f.rowResults) == 0 {
		return fakeRow{err: errors.New("unexpected QueryRow")}
	}
	row := f.rowResults[0]
	f.rowResults = f.rowResults[1:]
	return row
}

func (f *fakeBootstrapDB) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	var rows pgx.Rows
	if len(f.queryRows) > 0 {
		rows = f.queryRows[0]
		f.queryRows = f.queryRows[1:]
	}
	var err error
	if len(f.queryErrs) > 0 {
		err = f.queryErrs[0]
		f.queryErrs = f.queryErrs[1:]
	}
	return rows, err
}

func (f *fakeBootstrapDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	f.execSQL = append(f.execSQL, sql)
	var err error
	if len(f.execErrs) > 0 {
		err = f.execErrs[0]
		f.execErrs = f.execErrs[1:]
	}
	return pgconn.CommandTag{}, err
}

type fakeRow struct {
	values []any
	err    error
}

func (r fakeRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) > len(r.values) {
		return errors.New("not enough values")
	}
	for i := range dest {
		if err := assign(dest[i], r.values[i]); err != nil {
			return err
		}
	}
	return nil
}

type fakeRows struct {
	values  [][]any
	err     error
	scanErr error
	idx     int
	closed  bool
}

func (r *fakeRows) Close()                                       { r.closed = true }
func (r *fakeRows) Err() error                                   { return r.err }
func (r *fakeRows) CommandTag() pgconn.CommandTag                { return pgconn.CommandTag{} }
func (r *fakeRows) FieldDescriptions() []pgconn.FieldDescription { return nil }
func (r *fakeRows) Values() ([]any, error)                       { return r.values[r.idx-1], nil }
func (r *fakeRows) RawValues() [][]byte                          { return nil }
func (r *fakeRows) Conn() *pgx.Conn                              { return nil }

func (r *fakeRows) Next() bool {
	if r.idx >= len(r.values) {
		r.closed = true
		return false
	}
	r.idx++
	return true
}

func (r *fakeRows) Scan(dest ...any) error {
	if r.scanErr != nil {
		return r.scanErr
	}
	row := r.values[r.idx-1]
	for i := range dest {
		if err := assign(dest[i], row[i]); err != nil {
			return err
		}
	}
	return nil
}

func assign(dest any, value any) error {
	v := reflect.ValueOf(dest)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return errors.New("dest must be pointer")
	}
	src := reflect.ValueOf(value)
	if !src.Type().AssignableTo(v.Elem().Type()) {
		if src.Type().ConvertibleTo(v.Elem().Type()) {
			src = src.Convert(v.Elem().Type())
		} else {
			return errors.New("type mismatch")
		}
	}
	v.Elem().Set(src)
	return nil
}

func TestVerifyPGPrereqsUnit(t *testing.T) {
	ctx := context.Background()
	db := &fakeBootstrapDB{rowResults: []fakeRow{
		{values: []any{"logical"}},
		{values: []any{"2"}},
		{values: []any{"3"}},
	}}
	if err := verifyPGPrereqs(ctx, db, zerolog.Nop()); err != nil {
		t.Fatalf("verifyPGPrereqs ok: %v", err)
	}

	badWal := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{"replica"}}}}
	if err := verifyPGPrereqs(ctx, badWal, zerolog.Nop()); err == nil || !strings.Contains(err.Error(), "wal_level") {
		t.Fatalf("verifyPGPrereqs wal_level err = %v", err)
	}

	badSlots := &fakeBootstrapDB{rowResults: []fakeRow{
		{values: []any{"logical"}},
		{values: []any{"0"}},
	}}
	if err := verifyPGPrereqs(ctx, badSlots, zerolog.Nop()); err == nil || !strings.Contains(err.Error(), "max_replication_slots") {
		t.Fatalf("verifyPGPrereqs slots err = %v", err)
	}
}

func TestVerifyReplicationRoleUnit(t *testing.T) {
	ctx := context.Background()
	for _, row := range []fakeRow{
		{values: []any{"repl", true, false}},
		{values: []any{"super", false, true}},
	} {
		if err := verifyReplicationRole(ctx, &fakeBootstrapDB{rowResults: []fakeRow{row}}, zerolog.Nop()); err != nil {
			t.Fatalf("verifyReplicationRole ok: %v", err)
		}
	}

	err := verifyReplicationRole(ctx, &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{"login", false, false}}}}, zerolog.Nop())
	if err == nil || !strings.Contains(err.Error(), "ALTER ROLE login REPLICATION") {
		t.Fatalf("verifyReplicationRole err = %v", err)
	}
}

func TestBootstrapPublicationModes(t *testing.T) {
	ctx := context.Background()
	if err := bootstrapPublication(ctx, &fakeBootstrapDB{}, bootstrapConfig{Mode: "off", PublicationName: "pub"}, zerolog.Nop()); err != nil {
		t.Fatalf("off: %v", err)
	}

	verifyOK := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{2}}}}
	if err := bootstrapPublication(ctx, verifyOK, bootstrapConfig{Mode: "verify", PublicationName: "pub"}, zerolog.Nop()); err != nil {
		t.Fatalf("verify ok: %v", err)
	}

	verifyEmpty := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{0}}}}
	if err := bootstrapPublication(ctx, verifyEmpty, bootstrapConfig{Mode: "verify", PublicationName: "pub"}, zerolog.Nop()); err == nil {
		t.Fatal("verify empty returned nil")
	}

	autoAll := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}}}
	if err := bootstrapPublication(ctx, autoAll, bootstrapConfig{Mode: "auto", PublicationName: "pub"}, zerolog.Nop()); err != nil {
		t.Fatalf("auto all: %v", err)
	}
	if len(autoAll.execSQL) != 1 || !strings.Contains(autoAll.execSQL[0], "FOR ALL TABLES") {
		t.Fatalf("auto all exec = %#v", autoAll.execSQL)
	}

	autoTables := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}}}
	if err := bootstrapPublication(ctx, autoTables, bootstrapConfig{Mode: "auto", PublicationName: "pub", Tables: []string{"public.a", "public.b"}}, zerolog.Nop()); err != nil {
		t.Fatalf("auto tables: %v", err)
	}
	if len(autoTables.execSQL) != 1 || !strings.Contains(autoTables.execSQL[0], "FOR TABLE public.a, public.b") {
		t.Fatalf("auto tables exec = %#v", autoTables.execSQL)
	}

	existing := &fakeBootstrapDB{
		rowResults: []fakeRow{{values: []any{true}}, {values: []any{1}}},
		queryRows:  []pgx.Rows{&fakeRows{values: [][]any{{"public.a"}}}},
	}
	if err := bootstrapPublication(ctx, existing, bootstrapConfig{Mode: "auto", PublicationName: "pub", Tables: []string{"public.a"}}, zerolog.Nop()); err != nil {
		t.Fatalf("existing: %v", err)
	}
}

func TestBootstrapEnsureRoleUnit(t *testing.T) {
	ctx := context.Background()
	db := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}}}
	bootstrapEnsureRole(ctx, db, "postgres://repl:pa%27ss@host/db", true, zerolog.Nop())
	if len(db.execSQL) != 1 || !strings.Contains(db.execSQL[0], "CREATE ROLE repl WITH LOGIN REPLICATION PASSWORD 'pa''ss'") {
		t.Fatalf("replication role exec = %#v", db.execSQL)
	}

	admin := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}}}
	bootstrapEnsureRole(ctx, admin, "postgres://admin:pw@host/db", false, zerolog.Nop())
	if len(admin.execSQL) != 2 || !strings.Contains(admin.execSQL[0], "CREATE ROLE admin WITH LOGIN PASSWORD 'pw'") || !strings.Contains(admin.execSQL[1], "GRANT pg_monitor TO admin") {
		t.Fatalf("admin role exec = %#v", admin.execSQL)
	}

	existing := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{true}}}}
	bootstrapEnsureRole(ctx, existing, "postgres://admin:pw@host/db", false, zerolog.Nop())
	if len(existing.execSQL) != 0 {
		t.Fatalf("existing role exec = %#v", existing.execSQL)
	}

	for _, dsn := range []string{"%", "postgres://bad-name:pw@host/db", "postgres://nopw@host/db", "postgres://admin:bad%5Cpw@host/db"} {
		db := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}}}
		bootstrapEnsureRole(ctx, db, dsn, false, zerolog.Nop())
		if len(db.execSQL) != 0 {
			t.Fatalf("dsn %q exec = %#v", dsn, db.execSQL)
		}
	}
}

func TestCheckSlotHeadroomUnit(t *testing.T) {
	ctx := context.Background()
	checkSlotHeadroom(ctx, &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{10}}, {values: []any{3}}}}, 2, "slot", zerolog.Nop())
	checkSlotHeadroom(ctx, &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{4}}, {values: []any{3}}}}, 2, "slot", zerolog.Nop())
	checkSlotHeadroom(ctx, &fakeBootstrapDB{rowResults: []fakeRow{{err: errors.New("boom")}}}, 2, "slot", zerolog.Nop())
	checkSlotHeadroom(ctx, &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{4}}, {err: errors.New("boom")}}}, 2, "slot", zerolog.Nop())
}

func TestBootstrapVerifyTablesUnit(t *testing.T) {
	ctx := context.Background()
	bootstrapVerifyTables(ctx, &fakeBootstrapDB{queryRows: []pgx.Rows{&fakeRows{values: [][]any{{"public.b"}, {"public.a"}}}}}, "pub", []string{"public.a", "public.b"}, zerolog.Nop())
	bootstrapVerifyTables(ctx, &fakeBootstrapDB{queryRows: []pgx.Rows{&fakeRows{values: [][]any{{"public.extra"}}}}}, "pub", []string{"public.missing"}, zerolog.Nop())
	bootstrapVerifyTables(ctx, &fakeBootstrapDB{queryErrs: []error{errors.New("boom")}}, "pub", []string{"public.a"}, zerolog.Nop())
	bootstrapVerifyTables(ctx, &fakeBootstrapDB{queryRows: []pgx.Rows{&fakeRows{values: [][]any{{"public.a"}}, scanErr: errors.New("boom")}}}, "pub", []string{"public.a"}, zerolog.Nop())
	bootstrapVerifyTables(ctx, &fakeBootstrapDB{queryRows: []pgx.Rows{&fakeRows{values: [][]any{{"public.a"}}, err: errors.New("boom")}}}, "pub", []string{"public.a"}, zerolog.Nop())
}

func TestPrepareDatabaseUnit(t *testing.T) {
	ctx := context.Background()
	cfg := newSingletonTestConfig(t)
	cfg.WAL.Bootstrap.Mode = "verify"
	cfg.WAL.PublicationName = "pub"
	cfg.WAL.SlotHeadroomMin = 2

	db := &fakeBootstrapDB{rowResults: []fakeRow{
		{values: []any{"logical"}},
		{values: []any{"2"}},
		{values: []any{"2"}},
		{values: []any{"repl", true, false}},
		{values: []any{1}},
		{values: []any{10}},
		{values: []any{2}},
	}}
	if err := prepareDatabase(ctx, cfg, zerolog.Nop(), db, func() (string, error) { return "Host.Name", nil }, func() int { return 123 }); err != nil {
		t.Fatalf("prepareDatabase ok: %v", err)
	}

	fail := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{"replica"}}}}
	if err := prepareDatabase(ctx, cfg, zerolog.Nop(), fail, func() (string, error) { return "", errors.New("host") }, func() int { return 123 }); err == nil {
		t.Fatal("prepareDatabase prereq error = nil")
	}
}
