//go:build !race

// Package sse — race_off_test.go: non-race timing-bound flag.
// See race_on_test.go for the full design note. This file is selected
// when `go test` is invoked WITHOUT the race detector.
package sse

const raceEnabled = false
