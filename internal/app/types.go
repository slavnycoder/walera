package app

import "time"

type ShutdownDeadline time.Duration

func (d ShutdownDeadline) Duration() time.Duration { return time.Duration(d) }
