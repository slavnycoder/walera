package auth_test

import (
	"bytes"
	"strings"
	"testing"

	"github.com/walera/walera/internal/auth"
)

func mkSecret(n int) []byte {
	b := make([]byte, n)
	for i := range b {
		b[i] = byte('a' + (i % 26))
	}
	return b
}

func TestNewSigner_RejectsShortSecret(t *testing.T) {
	_, err := auth.NewSigner(mkSecret(auth.MinSigningSecretBytes-1), "v1")
	if err == nil {
		t.Fatal("NewSigner: err = nil; want too-short error")
	}
	if !strings.Contains(err.Error(), "32 bytes") {
		t.Errorf("err = %q; want mention of 32 bytes", err.Error())
	}
}

func TestNewSigner_RejectsEmptyKid(t *testing.T) {
	_, err := auth.NewSigner(mkSecret(64), "")
	if err == nil {
		t.Fatal("NewSigner: err = nil; want empty-kid error")
	}
}

func TestNewSigner_CopiesSecret(t *testing.T) {
	src := mkSecret(64)
	s, err := auth.NewSigner(src, "v1")
	if err != nil {
		t.Fatalf("NewSigner: %v", err)
	}
	sig1 := s.Sign("u1", "users:1", 1700000000, "n1")

	for i := range src {
		src[i] = 0
	}
	sig2 := s.Sign("u1", "users:1", 1700000000, "n1")

	if sig1 != sig2 {
		t.Error("Signer captured caller's slice — mutation changed sig")
	}
}

func TestSigner_SignDeterministic(t *testing.T) {
	s, _ := auth.NewSigner(mkSecret(64), "v1")
	a := s.Sign("u1", "users:1", 1700000000, "n1")
	b := s.Sign("u1", "users:1", 1700000000, "n1")
	if a != b {
		t.Errorf("sign not deterministic: %q vs %q", a, b)
	}
}

func TestSigner_VerifyMatches(t *testing.T) {
	s, _ := auth.NewSigner(mkSecret(64), "v1")
	sig := s.Sign("u1", "users:1", 1700000000, "n1")
	if !s.Verify("u1", "users:1", 1700000000, "n1", sig) {
		t.Error("Verify returned false for correct sig")
	}
}

func TestSigner_VerifyRejectsTampering(t *testing.T) {
	s, _ := auth.NewSigner(mkSecret(64), "v1")
	good := s.Sign("u1", "users:1", 1700000000, "n1")

	cases := []struct {
		name                       string
		userID, channel, nonce, sig string
		ts                         int64
	}{
		{"diff user", "u2", "users:1", "n1", good, 1700000000},
		{"diff channel", "u1", "users:2", "n1", good, 1700000000},
		{"diff ts", "u1", "users:1", "n1", good, 1700000001},
		{"diff nonce", "u1", "users:1", "n2", good, 1700000000},
		{"bad sig", "u1", "users:1", "n1", "deadbeef", 1700000000},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if s.Verify(tc.userID, tc.channel, tc.ts, tc.nonce, tc.sig) {
				t.Error("Verify returned true for tampered input")
			}
		})
	}
}

func TestSigner_DifferentSecretsDifferentSigs(t *testing.T) {
	s1, _ := auth.NewSigner(bytes.Repeat([]byte{'a'}, 64), "v1")
	s2, _ := auth.NewSigner(bytes.Repeat([]byte{'b'}, 64), "v1")
	if s1.Sign("u", "c", 0, "n") == s2.Sign("u", "c", 0, "n") {
		t.Error("different secrets produced identical sigs")
	}
}

func TestSigner_FieldSeparatorPreventsCollisions(t *testing.T) {
	s, _ := auth.NewSigner(mkSecret(64), "v1")

	a := s.Sign("ab", "c", 0, "n")
	b := s.Sign("a", "bc", 0, "n")
	if a == b {
		t.Error("ambiguous concatenation: 'ab'+'c' collided with 'a'+'bc'")
	}
}
