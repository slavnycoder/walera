package wal

import (
	"time"

	"github.com/jackc/pglogrepl"
)

type Op string

const (
	OpInsert Op = "insert"

	OpUpdate Op = "update"

	OpDelete Op = "delete"
)

type Change struct {
	Schema string

	Table string

	Op Op

	PK string

	PKCol string

	Data map[string]any

	Changed map[string]any
}

func (c Change) Key() string {
	return c.Schema + "." + c.Table + ":" + c.PK
}

func (c Change) WildcardKey() string {
	return c.Schema + "." + c.Table
}

type Tx struct {
	ID uint32

	CommitLSN pglogrepl.LSN

	CommitTS time.Time

	Changes []Change
}
