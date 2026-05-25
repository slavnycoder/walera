//go:build race

// Package sse — race_on_test.go: race-detector-aware test bound.
// A `raceEnabled` boolean populated by build tags lets race-aware tests
// apply a looser timing bound under
// `go test -race` without sacrificing the strict bound on production
// builds.
// Build-tag pair: `race_on_test.go` (this file, `//go:build race`) sets
// `raceEnabled = true`; `race_off_test.go` (`//go:build !race`) sets it
// false. The Go toolchain selects exactly one at build time.
package sse

const raceEnabled = true
