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

func TestBootstrapPublication_ExistingEmptyFails(t *testing.T) {
	db := &fakeBootstrapDB{rowResults: []fakeRow{{values: []any{false}}, {values: []any{150004}}, {values: []any{false}}, {values: []any{0}}}}
	err := bootstrapPublication(context.Background(), db, bootstrapConfig{PublicationName: "pub", Tables: []string{"public.a"}}, zerolog.Nop())
	if err == nil || !strings.Contains(err.Error(), "has no tables") || len(db.execSQL) != 0 {
		t.Fatalf("err = %v, exec = %#v; want no-tables error and no DDL", err, db.execSQL)
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
	cfg.WAL.PublicationName = "pub"
	cfg.WAL.SlotHeadroomMin = 2

	db := &fakeBootstrapDB{rowResults: []fakeRow{
		{values: []any{"logical"}},
		{values: []any{"2"}},
		{values: []any{"2"}},
		{values: []any{"repl", true, false}},
		{values: []any{false}},
		{values: []any{150004}},
		{values: []any{false}},
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
