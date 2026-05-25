// Package sse — tls_smoke_test.go gates VAL-05: the SSE wire format on a
// real TLS connection is byte-identical to the TCP golden fixture, AND
// the writev(2) fast-path (`(*net.Buffers).WriteTo` on `*net.TCPConn`) is
// demonstrably bypassed on the TLS path.
// Two assertions live here:
//  1. Wire-byte parity. After a single subscriber attaches over TLS and
//     receives prelude + tx frame + heartbeat + shutdown frame, the bytes
//     read from the client side match `scripts/golden/sse_v13_handshake.txt`
//     exactly. The fixture is the same file `golden_parity_test.go`'s
//     `TestGoldenParity_RespWriterPath` and `TestGoldenParity_HijackPath`
//     compare against — the TLS path is a third route through the same
//     contract.
//  2. Negative-writev assertion. The accepted net.Conn is wrapped with
//     `recordingConn` BEFORE the TLS handshake, so every encrypted Write
//     to the underlying TCP conn increments a counter. The drainSub fast
//     path (`(*net.Buffers).WriteTo`) is gated on `st.conn != nil`, and
//     `st.conn` is typed `*net.TCPConn` (pool.go:607) — the TLS handler
//     passes `conn=nil` to Attach, so the writev branch is unreachable by
//     construction. The recordingConn provides defense-in-depth: a future
//     refactor that loosened the conn type or wrapped a *tls.Conn into a
//     TCPConn shim would silently re-enable writev on the TLS path; this
//     test catches it by asserting the per-frame Write counter is at least
//     the per-frame frame count (≥ 3 writes for prelude / tx / heartbeat /
//     shutdown). A single writev would collapse the counter to 1.
//
// Cert is generated in-test via crypto/ecdsa + crypto/x509 stdlib helpers
// (CN="walera-test", IP SAN 127.0.0.1, 1 h validity). No testdata cert
// file is written; no new go.mod dependencies are introduced.
package sse

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"errors"
	"io"
	"math/big"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"

	"github.com/walera/walera/internal/router"
)

// recordingConn wraps a net.Conn and records every Write call. The pool's
// `(*net.Buffers).WriteTo` writev fast path is gated on the wrapped conn
// being a *net.TCPConn; recordingConn is not one, so the iovec branch is
// structurally unreachable here (defense-in-depth — the pool also gates
// on st.conn != nil and the TLS path passes conn=nil).
// Counter is mutex-guarded because tls.Conn may invoke Write from a
// goroutine distinct from the one that reads WriteCount in the test body.
type recordingConn struct {
	net.Conn
	mu         sync.Mutex
	writeCount int
}

func (r *recordingConn) Write(p []byte) (int, error) {
	r.mu.Lock()
	r.writeCount++
	r.mu.Unlock()
	return r.Conn.Write(p)
}

func (r *recordingConn) WriteCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.writeCount
}

// recordingListener wraps a net.Listener and records every accepted conn
// as a *recordingConn before returning it. The recorded conn is stored in
// recorders so the test can inspect WriteCount after the connection
// closes.
type recordingListener struct {
	base      net.Listener
	mu        sync.Mutex
	recorders []*recordingConn
}

func (l *recordingListener) Accept() (net.Conn, error) {
	c, err := l.base.Accept()
	if err != nil {
		return nil, err
	}
	rec := &recordingConn{Conn: c}
	l.mu.Lock()
	l.recorders = append(l.recorders, rec)
	l.mu.Unlock()
	return rec, nil
}

func (l *recordingListener) Close() error   { return l.base.Close() }
func (l *recordingListener) Addr() net.Addr { return l.base.Addr() }

func (l *recordingListener) firstRecorder() *recordingConn {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.recorders) == 0 {
		return nil
	}
	return l.recorders[0]
}

// generateSelfSignedCert builds an ephemeral ECDSA P-256 self-signed cert
// (CN="walera-test", IP SAN 127.0.0.1, 1 h validity). Returns the cert
// and the parsed leaf so the client can pin it as the sole trust root.
func generateSelfSignedCert(t *testing.T) (tls.Certificate, *x509.Certificate) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatalf("ecdsa.GenerateKey: %v", err)
	}
	template := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "walera-test"},
		NotBefore:    time.Now().Add(-time.Minute),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		// Modern Go x509 verifiers ignore CN and require the verified name
		// to appear in SAN. List both the DNS name the client will set as
		// ServerName and the loopback IP we dial.
		DNSNames:    []string{"walera-test"},
		IPAddresses: []net.IP{net.ParseIP("127.0.0.1")},
	}
	derBytes, err := x509.CreateCertificate(rand.Reader, &template, &template, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("x509.CreateCertificate: %v", err)
	}
	leaf, err := x509.ParseCertificate(derBytes)
	if err != nil {
		t.Fatalf("x509.ParseCertificate: %v", err)
	}
	cert := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  key,
		Leaf:        leaf,
	}
	return cert, leaf
}

// TestTLSSmoke_SingleSub_NoWriteV drives the deterministic golden tx
// through a *tls.Conn round-trip and asserts:
//   - wire bytes match the TCP golden fixture byte-for-byte
//   - the recording wrapper observed multiple per-frame Write calls
//     (i.e. the (*net.Buffers).WriteTo writev fast path did NOT fire)
//
// VAL-05.
func TestTLSSmoke_SingleSub_NoWriteV(t *testing.T) {
	t.Parallel()

	// --- 1. Ephemeral self-signed cert ----------------------------------
	cert, leaf := generateSelfSignedCert(t)

	// --- 2. Listener stack: TCP -> recording wrapper -> TLS -------------
	baseListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("net.Listen: %v", err)
	}
	defer baseListener.Close()

	recListener := &recordingListener{base: baseListener}
	tlsListener := tls.NewListener(recListener, &tls.Config{
		Certificates: []tls.Certificate{cert},
		MinVersion:   tls.VersionTLS12,
	})

	// --- 3. Pool + encoder shared between handler and shutdown driver ---
	enc := NewEncoder(10 * 1024 * 1024)
	// : fixturePoolConfig() sets HeartbeatInterval=250ms, which is
	// fine for the loopback-TCP golden fixtures (their read windows are
	// well under 10ms) but a latent flake on the TLS path: handshake +
	// kernel-stack round-trips for the tx read can exceed 250ms on slow
	// CI under -race, at which point the worker's heartbeat sweep would
	// enqueue an autonomous `:\n\n` frame in addition to the test's
	// explicit SendForTest(hb). Two heartbeats on the wire mismatch the
	// golden fixture and assertGoldenEqual fails. Override to time.Hour
	// so the explicit heartbeat is the only one — same defensive choice
	// goroutine_bound_test.go made for the same reason.
	tlsCfg := fixturePoolConfig()
	tlsCfg.HeartbeatInterval = time.Hour
	p := NewPool(tlsCfg, PoolDeps{Encoder: enc, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})
	// Defensive: if the test fails before the planned shutdown sequence,
	// still tear the pool down so the worker goroutines exit.
	defer func() { _ = p.Shutdown(context.Background()) }()

	// handlerReady signals the test-body that the HTTP handler has wired
	// the subscriber and read the request — i.e. it is now safe for the
	// test body to drive sends through subRef and to call pool.Shutdown.
	handlerReady := make(chan struct{})
	subRef := make(chan *fixtureSub, 1)
	var handlerErr error
	var handlerErrMu sync.Mutex
	setHandlerErr := func(e error) {
		handlerErrMu.Lock()
		if handlerErr == nil {
			handlerErr = e
		}
		handlerErrMu.Unlock()
	}

	// --- 4. HTTP handler — Attach the sub, expose it to the test body, -
	//        block on doneCh. All wire-traffic sequencing happens in -
	//        the test body so we can interleave reads from resp.Body -
	//        with sends to keep the wire deterministic (mirrors the -
	//        TCP-path fixture helper's read-then-write sequencing). -
	mux := http.NewServeMux()
	mux.HandleFunc("/sse", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		rc := http.NewResponseController(w)
		sub := newFixtureSub("tls-sub-001", "exact")
		doneCh, attachErr := p.Attach(sub, nil /* conn=nil forces respWriter path */, w, rc)
		if attachErr != nil {
			setHandlerErr(attachErr)
			close(handlerReady)
			return
		}
		// Hand the sub to the test body so it can drive SendForTest in
		// the correct order relative to client reads.
		subRef <- sub
		close(handlerReady)
		<-doneCh
	})

	srv := &http.Server{Handler: mux}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(tlsListener) }()
	defer srv.Close()

	// --- 5. Client — pin the test cert as the sole trust root -----------
	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,
				// "walera-test" is the cert's CN; with an IP SAN of
				// 127.0.0.1, the standard verifier accepts either.
				// ServerName drives SNI + verification name.
				ServerName: "walera-test",
			},
			// The cert template lists 127.0.0.1 as an IP SAN; dial via
			// the literal IP and have ServerName override the host check.
			DialTLSContext: nil, // default — Transport handles it
		},
	}
	url := "https://" + tlsListener.Addr().String() + "/sse"
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		t.Fatalf("http.NewRequest: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("client.Do: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK; got %d", resp.StatusCode)
	}

	// : Snapshot the per-conn Write counter after the TLS handshake
	// has completed (client.Do returned 200, so the server has finished
	// its handshake flight and written response headers) but before we
	// drive the data-frame wire sequence. The handshake alone produces
	// multiple ciphertext writes on the underlying conn (typically ≥ 3
	// for TLS 1.2/1.3 flights and the response-header flush), so a
	// cumulative-count lower bound cannot distinguish "handshake fired"
	// from "data path used per-frame writes". Compare the post-shutdown
	// counter against this baseline to assert specifically on the
	// data-frame portion of the conversation.
	rec := recListener.firstRecorder()
	if rec == nil {
		t.Fatalf("recordingListener observed zero accepted connections after client.Do")
	}
	handshakeBaseline := rec.WriteCount()

	// --- 6. Drive the wire sequence: tx -> read tx -> heartbeat -> read -
	//        heartbeat -> shutdown -> read shutdown -> EOF. -
	select {
	case <-handlerReady:
	case <-time.After(2 * time.Second):
		t.Fatalf("handler did not reach handlerReady within 2s")
	}
	handlerErrMu.Lock()
	hErr := handlerErr
	handlerErrMu.Unlock()
	if hErr != nil {
		t.Fatalf("handler error: %v", hErr)
	}
	var sub *fixtureSub
	select {
	case sub = <-subRef:
	case <-time.After(2 * time.Second):
		t.Fatalf("handler did not publish sub within 2s")
	}

	// Encode the deterministic synthetic tx (same buildGoldenTx the TCP
	// fixture helper uses). MatchedIndices [0,1,2] = all 3 changes.
	ev := router.Event{
		Tx:             buildGoldenTx(),
		MatchedIndices: []int{0, 1, 2},
	}
	frame, overflow := enc.Encode(ev)
	if overflow {
		t.Fatalf("synthetic tx overflowed encoder cap")
	}

	// Step 1: enqueue the tx frame and wait for it to land on the wire.
	if !sub.SendForTest(frame) {
		t.Fatalf("sub.SendForTest: queue full")
	}
	collected := make([]byte, 0, 1024)
	readBuf := make([]byte, 4096)
	// Expected first read: prelude (14 bytes) + tx frame.
	expectedPreludeAndTxLen := len("retry: 15000\n\n") + len(frame)
	deadline := time.Now().Add(2 * time.Second)
	for len(collected) < expectedPreludeAndTxLen {
		if time.Now().After(deadline) {
			t.Fatalf("timed out reading prelude+tx; have %d/%d bytes:\n%q", len(collected), expectedPreludeAndTxLen, collected)
		}
		n, rerr := resp.Body.Read(readBuf)
		if n > 0 {
			collected = append(collected, readBuf[:n]...)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			t.Fatalf("resp.Body.Read (tx): %v", rerr)
		}
	}

	// Step 2: enqueue the heartbeat through the same per-sub queue so
	// the worker drains it in FIFO order behind the tx frame.
	hb := enc.EncodeHeartbeat()
	if !sub.SendForTest(hb) {
		t.Fatalf("sub.SendForTest (heartbeat): queue full")
	}
	expectedAfterHb := expectedPreludeAndTxLen + len(hb)
	for len(collected) < expectedAfterHb {
		if time.Now().After(deadline) {
			t.Fatalf("timed out reading heartbeat; have %d/%d bytes", len(collected), expectedAfterHb)
		}
		n, rerr := resp.Body.Read(readBuf)
		if n > 0 {
			collected = append(collected, readBuf[:n]...)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			t.Fatalf("resp.Body.Read (hb): %v", rerr)
		}
	}

	// Step 3: trigger Shutdown — the worker emits the §3.5 shutdown
	// frame to the still-attached sub via the respWriter path, then the
	// handler's <-doneCh unblocks, http.Server closes the response, and
	// the client reads the shutdown bytes followed by EOF.
	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.Shutdown(context.Background()) }()

	// Drain the remaining bytes (shutdown frame + EOF).
	for {
		n, rerr := resp.Body.Read(readBuf)
		if n > 0 {
			collected = append(collected, readBuf[:n]...)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}
			// Any other read error after shutdown is unexpected.
			t.Fatalf("resp.Body.Read (shutdown tail): %v", rerr)
		}
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("pool.Shutdown: %v", err)
	}
	got := collected

	// --- 8. Wire-byte parity with the TCP golden fixture ----------------
	assertGoldenEqual(t, got)

	// --- 9. Negative-writev assertion -----------------------------------
	// Frames emitted on the wire post-handshake: prelude + tx + heartbeat +
	// shutdown = 4 frames. The respWriter per-frame fallback path produces
	// one plaintext Write per frame at the TLS layer, each yielding ≥ 1
	// ciphertext Write to the underlying recordingConn. A writev fast path
	// (`(*net.Buffers).WriteTo` on `*net.TCPConn`) would collapse the data
	// portion to a single underlying Write.
	// : assert against the DELTA from the handshake baseline rather
	// than the cumulative counter. The cumulative count is dominated by
	// TLS-handshake records (typically ≥ 3 on its own) which makes a
	// `>= 3` lower bound trivially satisfied even if the data path
	// collapsed to a single writev. The delta isolates the data-frame
	// portion and is the assertion that actually proves the contract.
	// Per-frame fallback (4 frames) → delta ≥ 3. We use 3 (not 4) for two
	// reasons: (a) the prelude write may race into the handshake baseline
	// snapshot if Attach's flush completes before client.Do returns 200,
	// shifting one count out of the delta; (b) record batching inside
	// crypto/tls could fold two adjacent plaintext writes into one
	// ciphertext record. A delta of 3 is well above a hypothetical
	// "single writev" regression (delta = 1) and tolerant of those edges.
	const minDataWrites = 3
	totalWrites := rec.WriteCount()
	dataWrites := totalWrites - handshakeBaseline
	if dataWrites < minDataWrites {
		t.Errorf("expected >= %d post-handshake Write calls on underlying conn (per-frame TLS fallback); got delta=%d (total=%d, handshake_baseline=%d) — writev fast-path may have leaked onto TLS path",
			minDataWrites, dataWrites, totalWrites, handshakeBaseline)
	}
	t.Logf("recordingConn observed total=%d Write calls; handshake_baseline=%d; data_delta=%d (≥ %d expected; writev fast-path bypassed)",
		totalWrites, handshakeBaseline, dataWrites, minDataWrites)
}
