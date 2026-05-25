package wal

import (
	"errors"
	"testing"

	"github.com/jackc/pglogrepl"
)

func makeRelMsg(id uint32, schema, table string, cols []struct {
	name     string
	dataType uint32
	isPK     bool
}) *pglogrepl.RelationMessage {
	msg := &pglogrepl.RelationMessage{
		RelationID:   id,
		Namespace:    schema,
		RelationName: table,
	}
	for _, c := range cols {
		col := &pglogrepl.RelationMessageColumn{
			Name:     c.name,
			DataType: c.dataType,
		}
		if c.isPK {
			col.Flags = 0x01
		}
		msg.Columns = append(msg.Columns, col)
	}
	return msg
}

func TestRelationCacheUpdate(t *testing.T) {
	t.Parallel()
	cache := newRelationCache()

	msg := makeRelMsg(100, "public", "users", []struct {
		name     string
		dataType uint32
		isPK     bool
	}{
		{"id", OIDInt4, true},
		{"name", OIDText, false},
		{"email", OIDVarchar, false},
	})

	if err := cache.Update(msg); err != nil {
		t.Fatalf("expected no error, got: %v", err)
	}

	info, ok := cache.Get(100)
	if !ok {
		t.Fatal("expected relation 100 to be in cache")
	}
	if info.OID != 100 {
		t.Errorf("OID: got %d, want %d", info.OID, 100)
	}
	if info.Schema != "public" {
		t.Errorf("Schema: got %q, want %q", info.Schema, "public")
	}
	if info.Table != "users" {
		t.Errorf("Table: got %q, want %q", info.Table, "users")
	}
	if len(info.PKCols) != 1 || info.PKCols[0] != "id" {
		t.Errorf("PKCols: got %v, want [id]", info.PKCols)
	}
	if info.PKColOID != OIDInt4 {
		t.Errorf("PKColOID: got %d, want %d", info.PKColOID, OIDInt4)
	}
	if len(info.Columns) != 3 {
		t.Errorf("Columns count: got %d, want 3", len(info.Columns))
	}
}

func TestRelationCacheCompositePKRejected(t *testing.T) {
	t.Parallel()
	cache := newRelationCache()

	msg := makeRelMsg(101, "public", "order_items", []struct {
		name     string
		dataType uint32
		isPK     bool
	}{
		{"order_id", OIDInt4, true},
		{"item_id", OIDInt4, true},
		{"qty", OIDInt4, false},
	})

	err := cache.Update(msg)
	if err == nil {
		t.Fatal("expected errCompositePK, got nil")
	}
	if !errors.Is(err, errCompositePK) {
		t.Errorf("expected errCompositePK, got: %v", err)
	}
}

func TestRelationCacheUnsupportedPKOID(t *testing.T) {
	t.Parallel()

	unsupportedOIDs := []struct {
		name string
		oid  uint32
	}{
		{"jsonb", OIDJSONB},
		{"bytea", OIDBytea},
		{"numeric", OIDNumeric},
		{"float4", OIDFloat4},
		{"float8", OIDFloat8},
		{"bool", OIDBool},
		{"timestamp", OIDTimestamp},
		{"timestamptz", OIDTimestampTZ},
		{"custom_oid_9999", 9999},
	}

	for _, tc := range unsupportedOIDs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cache := newRelationCache()
			msg := makeRelMsg(200, "public", "bad_table", []struct {
				name     string
				dataType uint32
				isPK     bool
			}{
				{"id", tc.oid, true},
				{"value", OIDText, false},
			})
			err := cache.Update(msg)
			if err == nil {
				t.Fatalf("OID %d (%s): expected errUnsupportedPKType, got nil", tc.oid, tc.name)
			}
			if !errors.Is(err, errUnsupportedPKType) {
				t.Errorf("OID %d (%s): expected errUnsupportedPKType, got: %v", tc.oid, tc.name, err)
			}
		})
	}
}

func TestRelationCacheUUIDDataType(t *testing.T) {
	t.Parallel()

	allowedCases := []struct {
		name string
		oid  uint32
	}{
		{"uuid", OIDUUID},
		{"text", OIDText},
		{"int8", OIDInt8},
		{"int2", OIDInt2},
		{"int4", OIDInt4},
	}

	for _, tc := range allowedCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cache := newRelationCache()
			msg := makeRelMsg(300, "app", "entities", []struct {
				name     string
				dataType uint32
				isPK     bool
			}{
				{"id", tc.oid, true},
				{"data", OIDText, false},
			})
			if err := cache.Update(msg); err != nil {
				t.Errorf("OID %d (%s): unexpected error: %v", tc.oid, tc.name, err)
			}
			info, ok := cache.Get(300)
			if !ok {
				t.Errorf("OID %d: relation not found in cache after update", tc.oid)
			}
			if info != nil && info.PKColOID != tc.oid {
				t.Errorf("OID %d: PKColOID mismatch: got %d", tc.oid, info.PKColOID)
			}
		})
	}
}

func TestRelationCacheOverwrite(t *testing.T) {
	t.Parallel()
	cache := newRelationCache()

	msg1 := makeRelMsg(400, "public", "products", []struct {
		name     string
		dataType uint32
		isPK     bool
	}{
		{"id", OIDInt4, true},
		{"name", OIDText, false},
	})
	if err := cache.Update(msg1); err != nil {
		t.Fatalf("first update: %v", err)
	}
	info1, _ := cache.Get(400)
	if len(info1.Columns) != 2 {
		t.Fatalf("first update: expected 2 columns, got %d", len(info1.Columns))
	}

	msg2 := makeRelMsg(400, "public", "products", []struct {
		name     string
		dataType uint32
		isPK     bool
	}{
		{"id", OIDInt4, true},
		{"name", OIDText, false},
		{"price", OIDNumeric, false},
	})
	if err := cache.Update(msg2); err != nil {
		t.Fatalf("second update: %v", err)
	}
	info2, ok := cache.Get(400)
	if !ok {
		t.Fatal("relation 400 not found after second update")
	}
	if len(info2.Columns) != 3 {
		t.Errorf("second update: expected 3 columns, got %d", len(info2.Columns))
	}

	if info1 == info2 {
		t.Error("second update: expected a new relationInfo pointer, got same pointer")
	}
}

func TestRelationCacheNoPKRejected(t *testing.T) {
	t.Parallel()
	cache := newRelationCache()

	msg := makeRelMsg(500, "public", "logs", []struct {
		name     string
		dataType uint32
		isPK     bool
	}{
		{"message", OIDText, false},
		{"created_at", OIDTimestampTZ, false},
	})

	err := cache.Update(msg)
	if err == nil {
		t.Fatal("expected error for table with no PK, got nil")
	}

	if !errors.Is(err, errCompositePK) {
		t.Errorf("expected errCompositePK, got: %v", err)
	}
}

func TestRelationCacheGetMiss(t *testing.T) {
	t.Parallel()
	cache := newRelationCache()
	info, ok := cache.Get(9999)
	if ok {
		t.Error("expected ok=false for unknown OID")
	}
	if info != nil {
		t.Errorf("expected nil info for unknown OID, got %v", info)
	}
}
