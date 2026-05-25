//go:build dev

package config

func refuseDevEnv(_ []string) error { return nil }
