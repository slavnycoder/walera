package sse

import "errors"

var errPoolClosed = errors.New("sse: writer pool is closed")
