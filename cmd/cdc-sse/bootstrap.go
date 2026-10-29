package main

import (
	"io"
	"net"
	"net/http"
	"os"
	"time"
)

// healthcheckTarget is the host:port the probe dials for WALERA_ADDR: the
// configured host when it names one, loopback when it is a wildcard.
func healthcheckTarget(addr string) string {
	if addr == "" {
		addr = ":8080"
	}
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return "127.0.0.1:8080"
	}
	switch host {
	case "", "0.0.0.0", "::":
		host = "127.0.0.1"
	}
	return net.JoinHostPort(host, port)
}

func runHealthcheckProbe() {
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get("http://" + healthcheckTarget(os.Getenv("WALERA_ADDR")) + "/healthz") //nolint:gosec // G704: self-probe, address from our own config
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
