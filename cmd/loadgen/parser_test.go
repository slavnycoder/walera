package main

import "testing"

func TestParseFrame_HappyPath(t *testing.T) {
	lines := []string{
		"event: tx",
		"id: 42",
		"data: {\"tx_id\":42}",
	}
	event, data, ok := ParseFrame(lines)
	if !ok {
		t.Fatalf("ParseFrame ok=false; want true (lines=%v)", lines)
	}
	if event != "tx" {
		t.Errorf("event = %q; want %q", event, "tx")
	}
	if data != `{"tx_id":42}` {
		t.Errorf("data = %q; want %q", data, `{"tx_id":42}`)
	}
}

func TestParseFrame_ErrorFrame(t *testing.T) {
	lines := []string{
		"event: error",
		"data: {\"reason\":\"unauthorized\"}",
	}
	event, data, ok := ParseFrame(lines)
	if !ok {
		t.Fatalf("ParseFrame ok=false; want true")
	}
	if event != "error" {
		t.Errorf("event = %q; want %q", event, "error")
	}
	if data != `{"reason":"unauthorized"}` {
		t.Errorf("data = %q; want %q", data, `{"reason":"unauthorized"}`)
	}
}

func TestParseFrame_Heartbeat(t *testing.T) {

	_, _, ok := ParseFrame([]string{":"})
	if ok {
		t.Fatal("ParseFrame ok=true for heartbeat; want false (heartbeats are not delivered events)")
	}
}

func TestParseFrame_Shutdown(t *testing.T) {
	lines := []string{
		"event: shutdown",
		"data: {\"reason\":\"service_restart\"}",
	}
	event, data, ok := ParseFrame(lines)
	if !ok {
		t.Fatalf("ParseFrame ok=false; want true")
	}
	if event != "shutdown" {
		t.Errorf("event = %q; want %q", event, "shutdown")
	}
	if data != `{"reason":"service_restart"}` {
		t.Errorf("data = %q; want %q", data, `{"reason":"service_restart"}`)
	}
}

func TestParseFrame_MalformedNoData(t *testing.T) {

	_, _, ok := ParseFrame([]string{"event: tx"})
	if ok {
		t.Fatal("ParseFrame ok=true for event-only frame; want false")
	}
}

func TestParseFrame_IgnoresIDLine(t *testing.T) {

	_, _, ok := ParseFrame([]string{"event: tx", "id: 1"})
	if ok {
		t.Fatal("ParseFrame ok=true for event+id without data; want false")
	}
}
