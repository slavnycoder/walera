package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
)

const (
	MinSigningSecretBytes = 32

	HealthSentinelUserID = "_health"
)

var ErrSigningSecretTooShort = fmt.Errorf("signing secret must be at least %d bytes", MinSigningSecretBytes)

type Signer struct {
	secret []byte
	kid    string
}

func NewSigner(secret []byte, kid string) (*Signer, error) {
	if len(secret) < MinSigningSecretBytes {
		return nil, ErrSigningSecretTooShort
	}
	if kid == "" {
		return nil, errors.New("signing kid is required")
	}
	cp := make([]byte, len(secret))
	copy(cp, secret)
	return &Signer{secret: cp, kid: kid}, nil
}

func (s *Signer) Kid() string { return s.kid }

func (s *Signer) Sign(userID, channel string, ts int64, nonce string) string {
	mac := hmac.New(sha256.New, s.secret)
	mac.Write([]byte(userID))
	mac.Write([]byte{'\n'})
	mac.Write([]byte(channel))
	mac.Write([]byte{'\n'})
	mac.Write([]byte(fmt.Sprintf("%d", ts)))
	mac.Write([]byte{'\n'})
	mac.Write([]byte(nonce))
	return hex.EncodeToString(mac.Sum(nil))
}

func (s *Signer) Verify(userID, channel string, ts int64, nonce, sig string) bool {
	want := s.Sign(userID, channel, ts, nonce)
	return hmac.Equal([]byte(want), []byte(sig))
}
