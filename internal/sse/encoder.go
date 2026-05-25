package sse

import (
	"bytes"
	"encoding/json"
	"strconv"
	"sync"
	"time"

	"github.com/walera/walera/internal/router"
	"github.com/walera/walera/internal/wal"
)

var heartbeatBytes = []byte(":\n\n")

var shutdownBytes = []byte("event: shutdown\ndata: {\"reason\":\"service_restart\"}\n\n")

type txEvent struct {
	TxID     uint32        `json:"tx_id"`
	CommitTS string        `json:"commit_ts"`
	Changes  []changeEvent `json:"changes"`
}

type changeEvent struct {
	Op    string         `json:"op"`
	Table string         `json:"table"`
	PK    string         `json:"pk"`
	Data  map[string]any `json:"data,omitempty"`
}

func mkChangeEvent(ch wal.Change) changeEvent {
	data := ch.Data
	if data == nil {
		data = ch.Changed
	}
	return changeEvent{
		Op:    string(ch.Op),
		Table: ch.Table,
		PK:    ch.PK,
		Data:  data,
	}
}

func txToEvent(tx wal.Tx, matched []int) txEvent {
	var changes []changeEvent
	if matched == nil {
		changes = make([]changeEvent, 0, len(tx.Changes))
		for _, ch := range tx.Changes {
			changes = append(changes, mkChangeEvent(ch))
		}
	} else {
		changes = make([]changeEvent, 0, len(matched))
		for _, idx := range matched {
			if idx < 0 || idx >= len(tx.Changes) {
				continue
			}
			changes = append(changes, mkChangeEvent(tx.Changes[idx]))
		}
	}
	return txEvent{
		TxID:     tx.ID,
		CommitTS: tx.CommitTS.UTC().Format(time.RFC3339Nano),
		Changes:  changes,
	}
}

type Encoder struct {
	bufPool         sync.Pool
	maxPayloadBytes int
}

func NewEncoder(maxPayloadBytes int) *Encoder {
	return &Encoder{
		bufPool: sync.Pool{
			New: func() any { return new(bytes.Buffer) },
		},
		maxPayloadBytes: maxPayloadBytes,
	}
}

func (e *Encoder) Encode(ev router.Event) ([]byte, bool) {
	buf := e.bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		e.bufPool.Put(buf)
	}()

	buf.WriteString("event: tx\n")
	buf.WriteString("id: ")
	buf.WriteString(strconv.FormatUint(uint64(ev.Tx.ID), 10))
	buf.WriteString("\ndata: ")

	payload, err := json.Marshal(txToEvent(ev.Tx, ev.MatchedIndices))
	if err != nil {

		payload = []byte(`{"tx_id":0,"commit_ts":"","changes":[]}`)
	}
	buf.Write(payload)
	buf.WriteString("\n\n")

	if e.maxPayloadBytes > 0 && buf.Len() > e.maxPayloadBytes {
		return nil, true
	}

	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out, false
}

func (e *Encoder) EncodeError(reason string) []byte {
	buf := e.bufPool.Get().(*bytes.Buffer)
	defer func() {
		buf.Reset()
		e.bufPool.Put(buf)
	}()

	reasonJSON, err := json.Marshal(reason)
	if err != nil {

		reasonJSON = []byte(`"unknown"`)
	}

	buf.WriteString("event: error\ndata: {\"reason\":")
	buf.Write(reasonJSON)
	buf.WriteString("}\n\n")

	out := make([]byte, buf.Len())
	copy(out, buf.Bytes())
	return out
}

func (e *Encoder) EncodeHeartbeat() []byte {
	return heartbeatBytes
}

func (e *Encoder) EncodeShutdown() []byte {
	return shutdownBytes
}
