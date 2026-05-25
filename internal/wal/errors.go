package wal

import "errors"

var ErrNotConnected = errors.New("wal: replication connection not established")
