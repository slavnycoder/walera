package app

import "time"

type ShutdownDeadline time.Duration

type DrainDeadline time.Duration

func (d ShutdownDeadline) Duration() time.Duration { return time.Duration(d) }

func (d DrainDeadline) Duration() time.Duration { return time.Duration(d) }
