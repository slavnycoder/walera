package main

import (
	"io"
	"net/http"
	"os"
	"strings"
	"time"
)

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
