// Package sse — package-level sentinel errors.
package sse

import "errors"

// errPoolClosed is returned by Attach after Shutdown has been called.
var errPoolClosed = errors.New("sse: writer pool is closed")

// errHijackedConnNotTCP is returned by hijackTCPConn when the hijacked
// conn cannot be asserted to *net.TCPConn.
var errHijackedConnNotTCP = errors.New("sse: hijacked conn is not *net.TCPConn")
