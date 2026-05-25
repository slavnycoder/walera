package router

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"sync/atomic"

	"github.com/jackc/pglogrepl"

	"github.com/walera/walera/internal/wal"
)

type Kind string

const (
	KindExact Kind = "exact"

	KindWildcard Kind = "wildcard"
)

type SubscriberConfig struct {
	ID string

	Kind Kind

	Schema string

	Table string

	PK string

	StartLSN pglogrepl.LSN

	BufferCap int
}

type SubscriberDeps struct {
	Parent context.Context
}

type Subscriber struct {
	id       string
	kind     Kind
	schema   string
	table    string
	pk       string
	startLSN pglogrepl.LSN

	sendFunc atomic.Value

	ctx    context.Context
	cancel context.CancelFunc

	reasonOnce sync.Once

	reasonPtr atomic.Pointer[string]

	Filter func(c wal.Change, txCommitLSN pglogrepl.LSN) (wal.Change, bool)
}

func NewSubscriber(cfg SubscriberConfig, deps SubscriberDeps) *Subscriber {
	id := cfg.ID
	if id == "" {
		var buf [16]byte
		if _, err := rand.Read(buf[:]); err != nil {
			panic("router: crypto/rand.Read failed: " + err.Error())
		}
		id = hex.EncodeToString(buf[:])
	}
	parent := deps.Parent
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithCancel(parent)
	return &Subscriber{
		id:       id,
		kind:     cfg.Kind,
		schema:   cfg.Schema,
		table:    cfg.Table,
		pk:       cfg.PK,
		startLSN: cfg.StartLSN,
		ctx:      ctx,
		cancel:   cancel,
	}
}

func (s *Subscriber) ID() string { return s.id }

func (s *Subscriber) Kind() Kind { return s.kind }

func (s *Subscriber) KindString() string { return string(s.kind) }

func (s *Subscriber) Schema() string { return s.schema }

func (s *Subscriber) Table() string { return s.table }

func (s *Subscriber) PK() string { return s.pk }

func (s *Subscriber) StartLSN() pglogrepl.LSN { return s.startLSN }

func (s *Subscriber) WireSendFunc(fn func(frame []byte) bool) {
	s.sendFunc.Store(fn)
}

func (s *Subscriber) send(frame []byte) bool {
	v := s.sendFunc.Load()
	if v == nil {
		return false
	}
	return v.(func([]byte) bool)(frame)
}

func (s *Subscriber) Done() <-chan struct{} { return s.ctx.Done() }

func (s *Subscriber) Reason() string {
	p := s.reasonPtr.Load()
	if p == nil {
		return ""
	}
	return *p
}

func (s *Subscriber) Drop(reason string) {
	s.reasonOnce.Do(func() {
		r := reason
		s.reasonPtr.Store(&r)
		s.cancel()
	})
}
