package router

import "github.com/walera/walera/internal/wal"

type Event struct {
	Tx wal.Tx

	MatchedIndices []int
}
