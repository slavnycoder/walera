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

func TestTLSSmoke_SingleSub_NoWriteV(t *testing.T) {
	t.Parallel()

	cert, leaf := generateSelfSignedCert(t)

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

	enc := NewEncoder(10 * 1024 * 1024)

	tlsCfg := fixturePoolConfig()
	tlsCfg.HeartbeatInterval = time.Hour
	p := NewPool(tlsCfg, PoolDeps{Encoder: enc, Metrics: newFakeMetrics(), Logger: zerolog.Nop()})

	defer func() { _ = p.Shutdown(context.Background()) }()

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

	mux := http.NewServeMux()
	mux.HandleFunc("/sse", func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		rc := http.NewResponseController(w)
		sub := newFixtureSub("tls-sub-001", "exact")
		doneCh, attachErr := p.Attach(sub, nil, w, rc)
		if attachErr != nil {
			setHandlerErr(attachErr)
			close(handlerReady)
			return
		}

		subRef <- sub
		close(handlerReady)
		<-doneCh
	})

	srv := &http.Server{Handler: mux}
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(tlsListener) }()
	defer srv.Close()

	pool := x509.NewCertPool()
	pool.AddCert(leaf)
	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig: &tls.Config{
				RootCAs:    pool,
				MinVersion: tls.VersionTLS12,

				ServerName: "walera-test",
			},

			DialTLSContext: nil,
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

	rec := recListener.firstRecorder()
	if rec == nil {
		t.Fatalf("recordingListener observed zero accepted connections after client.Do")
	}
	handshakeBaseline := rec.WriteCount()

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

	ev := router.Event{
		Tx:             buildGoldenTx(),
		MatchedIndices: []int{0, 1, 2},
	}
	frame, overflow := enc.Encode(ev)
	if overflow {
		t.Fatalf("synthetic tx overflowed encoder cap")
	}

	if !sub.SendForTest(frame) {
		t.Fatalf("sub.SendForTest: queue full")
	}
	collected := make([]byte, 0, 1024)
	readBuf := make([]byte, 4096)

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

	shutdownDone := make(chan error, 1)
	go func() { shutdownDone <- p.Shutdown(context.Background()) }()

	for {
		n, rerr := resp.Body.Read(readBuf)
		if n > 0 {
			collected = append(collected, readBuf[:n]...)
		}
		if rerr != nil {
			if errors.Is(rerr, io.EOF) {
				break
			}

			t.Fatalf("resp.Body.Read (shutdown tail): %v", rerr)
		}
	}
	if err := <-shutdownDone; err != nil {
		t.Fatalf("pool.Shutdown: %v", err)
	}
	got := collected

	assertGoldenEqual(t, got)

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
