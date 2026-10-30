// Package secrets seals secret environment values at rest.
//
// AES-256-GCM with a random nonce per value. The ciphertext is bound to the
// row it belongs to through the additional data, so a database administrator
// (or a bug) cannot swap one secret into another variable's slot undetected.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
)

// KeySize is the required key length in bytes.
const KeySize = 32

// Box seals and opens values under one key.
type Box struct {
	aead   cipher.AEAD
	macKey []byte // derived from the key, for Revision
}

// NewBox returns a Box for a 32-byte key.
func NewBox(key []byte) (*Box, error) {
	if len(key) != KeySize {
		return nil, fmt.Errorf("secrets key must be %d bytes, got %d", KeySize, len(key))
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	aead, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	// A separate key for revisions, so the one that seals is never used to MAC.
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("edgerun/revision/v1"))
	return &Box{aead: aead, macKey: mac.Sum(nil)}, nil
}

// Seal returns nonce || ciphertext for plaintext, bound to aad.
func (b *Box) Seal(plaintext, aad []byte) []byte {
	nonce := make([]byte, b.aead.NonceSize(), b.aead.NonceSize()+len(plaintext)+b.aead.Overhead())
	if _, err := rand.Read(nonce); err != nil {
		panic("secrets: no entropy: " + err.Error()) // cannot continue safely
	}
	return b.aead.Seal(nonce, nonce, plaintext, aad)
}

// Open reverses Seal. It fails if the data was modified or if aad differs from
// the value used to seal it.
func (b *Box) Open(sealed, aad []byte) ([]byte, error) {
	n := b.aead.NonceSize()
	if len(sealed) < n+b.aead.Overhead() {
		return nil, errors.New("secrets: sealed value is too short")
	}
	return b.aead.Open(nil, sealed[:n], sealed[n:], aad)
}

// Revision returns a token that identifies parts without revealing them: the
// same parts always give the same token, any other give another, and it cannot
// be recomputed or guessed against without the key. Parts are length-prefixed,
// so ("ab", "c") and ("a", "bc") differ.
func (b *Box) Revision(parts ...[]byte) string {
	mac := hmac.New(sha256.New, b.macKey)
	for _, p := range parts {
		var n [8]byte
		binary.BigEndian.PutUint64(n[:], uint64(len(p)))
		mac.Write(n[:])
		mac.Write(p)
	}
	return hex.EncodeToString(mac.Sum(nil)[:16])
}
