// Package secrets seals secret environment values at rest.
//
// AES-256-GCM with a random nonce per value. The ciphertext is bound to the
// row it belongs to through the additional data, so a database administrator
// (or a bug) cannot swap one secret into another variable's slot undetected.
package secrets

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"errors"
	"fmt"
)

// KeySize is the required key length in bytes.
const KeySize = 32

// Box seals and opens values under one key.
type Box struct{ aead cipher.AEAD }

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
	return &Box{aead: aead}, nil
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
