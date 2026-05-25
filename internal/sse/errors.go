package sse

import "errors"

var errPoolClosed = errors.New("sse: writer pool is closed")

var errHijackedConnNotTCP = errors.New("sse: hijacked conn is not *net.TCPConn")
