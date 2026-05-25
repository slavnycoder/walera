package main

import (
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

// runHealthcheckProbe performs the in-process /healthz probe used by
// distroless container HEALTHCHECKs (no shell, wget, or curl available). The
// function exits the process directly (0 on 200, 1 otherwise). It is called
// from main() BEFORE any logger/config/DB initialisation because the
// container HEALTHCHECK may execute in a partially-configured environment.
//
// The probe URL targets 127.0.0.1 (the in-process listener is always
// reachable on loopback regardless of the bind address). The port is derived
// from the WALERA_HTTP_ADDR env var when set so an operator override
// propagates to the probe; we read os.Getenv directly rather than calling
// config.Load to keep this fast-path cheap (no YAML parse, no koanf init).
// Accepted address formats: ":8080", "0.0.0.0:8080", "127.0.0.1:8080". On
// empty/unparseable values we fall back to 8080, matching the production
// EXPOSE 8080 and the testbench port mapping.
//
// Lives in cmd/cdc-sse/ (not internal/app/) because it runs BEFORE any
// package-app code executes — the binary's distroless HEALTHCHECK invokes
// it with --healthcheck and the function exits the process directly. An
// earlier refactor relocated the rest of bootstrap.go (verifyPGPrereqs /
// bootstrapPublication / checkSlotHeadroom) to internal/app/bootstrap.go;
// this file retains only the cmd-package fast-path probe.
func runHealthcheckProbe() {
	port := "8080"
	if addr := os.Getenv("WALERA_HTTP_ADDR"); addr != "" {
		if i := strings.LastIndex(addr, ":"); i >= 0 && i+1 < len(addr) {
			port = addr[i+1:]
		}
	}
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://127.0.0.1:" + port + "/healthz")
	if err != nil {
		os.Exit(1)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
	os.Exit(0)
}
