package app

import "time"

// ShutdownDeadline is the hard cap on the entire graceful-shutdown sequence
// (default 10s). Named-type discipline distinguishes it from DrainDeadline
// at every call site.
type ShutdownDeadline time.Duration

// DrainDeadline is the inner deadline for broadcaster.Shutdown's fan-out
// (default 8s; must be <= ShutdownDeadline).
type DrainDeadline time.Duration

// Duration returns the underlying time.Duration value.
func (d ShutdownDeadline) Duration() time.Duration { return time.Duration(d) }

// Duration returns the underlying time.Duration value.
func (d DrainDeadline) Duration() time.Duration { return time.Duration(d) }
