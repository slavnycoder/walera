package main

import "testing"

func TestHealthcheckTarget(t *testing.T) {
	for addr, want := range map[string]string{
		"":               "127.0.0.1:8080",
		":9000":          "127.0.0.1:9000",
		"0.0.0.0:9000":   "127.0.0.1:9000",
		"[::]:9000":      "127.0.0.1:9000",
		"10.1.2.3:9000":  "10.1.2.3:9000",
		"[fd00::1]:9000": "[fd00::1]:9000",
		"localhost:9000": "localhost:9000",
	} {
		if got := healthcheckTarget(addr); got != want {
			t.Errorf("healthcheckTarget(%q) = %q; want %q", addr, got, want)
		}
	}
}
