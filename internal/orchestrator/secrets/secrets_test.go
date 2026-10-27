package secrets

import (
	"bytes"
	"testing"
)

func box(t *testing.T) *Box {
	b, err := NewBox(bytes.Repeat([]byte{7}, KeySize))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestRoundTrip(t *testing.T) {
	b := box(t)
	sealed := b.Seal([]byte("hunter2"), []byte("fn/API_KEY"))
	if bytes.Contains(sealed, []byte("hunter2")) {
		t.Fatal("plaintext visible in the sealed value")
	}
	got, err := b.Open(sealed, []byte("fn/API_KEY"))
	if err != nil || string(got) != "hunter2" {
		t.Fatalf("%q %v", got, err)
	}
}

func TestBoundToItsSlot(t *testing.T) {
	b := box(t)
	sealed := b.Seal([]byte("v"), []byte("fn/A"))
	if _, err := b.Open(sealed, []byte("fn/B")); err == nil {
		t.Error("a ciphertext moved to another variable's slot must not open")
	}
}

func TestTamperingAndWrongKeyAreDetected(t *testing.T) {
	b := box(t)
	sealed := b.Seal([]byte("value"), nil)
	for i := range sealed {
		mut := append([]byte{}, sealed...)
		mut[i] ^= 1
		if _, err := b.Open(mut, nil); err == nil {
			t.Fatalf("flipping bit in byte %d went unnoticed", i)
		}
	}
	other, _ := NewBox(bytes.Repeat([]byte{8}, KeySize))
	if _, err := other.Open(sealed, nil); err == nil {
		t.Error("opened under the wrong key")
	}
	if _, err := b.Open([]byte("short"), nil); err == nil {
		t.Error("short input accepted")
	}
}

func TestNoncesAreFresh(t *testing.T) {
	b := box(t)
	a, c := b.Seal([]byte("same"), nil), b.Seal([]byte("same"), nil)
	if bytes.Equal(a, c) {
		t.Error("sealing the same value twice produced identical output: nonce reuse")
	}
}

func TestKeyLength(t *testing.T) {
	for _, n := range []int{0, 16, 31, 33, 64} {
		if _, err := NewBox(make([]byte, n)); err == nil {
			t.Errorf("%d-byte key accepted", n)
		}
	}
}
