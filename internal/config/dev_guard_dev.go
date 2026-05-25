//go:build dev

// Package config — dev_guard_dev.go is the no-op companion to dev_guard.go.
// It is in scope only when building with `-tags dev`; production builds
// always link the strict refusal in dev_guard.go.
//
// Build with the dev tag to enable EXPERIMENTAL_/DEBUG_FORCE_/PLAN_ env
// vars intentionally:
//
//	go build -tags dev ./...
//	go test  -tags dev ./internal/config/...
package config

// refuseDevEnv is a no-op under the `dev` build tag — dev builds may set
// WALERA_EXPERIMENTAL_*, WALERA_DEBUG_FORCE_*, and WALERA_PLAN_* env vars.
// The production build's strict refusal lives in dev_guard.go.
func refuseDevEnv(_ []string) error { return nil }
