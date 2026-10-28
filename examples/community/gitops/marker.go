package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
)

// The control protocol has no labels, annotations or owner on any resource, and
// secret environment values cannot be read back. Both problems are solved here
// with the only per-function key/value store there is: one ordinary (non-secret)
// environment variable, GITOPS_STATE, holding
//
//	{"owner":"<--owner>","secrets":{"API_KEY":"<hmac>"}}
//
// owner is the ownership claim ("this function was created by this service");
// secrets holds a keyed fingerprint of every secret value this service wrote, so
// that "has the secret changed?" is decidable although the value is write-only.
//
// Costs, all recorded in FINDINGS.md: the guest can see this variable; it counts
// against the 64-variable limit; it can be edited by anyone with an admin token
// (which is acceptable: they could delete the function too); and nothing ties it
// to routes, which have no environment at all.
type marker struct {
	Owner   string            `json:"owner"`
	Secrets map[string]string `json:"secrets,omitempty"`
}

func (m marker) encode() string {
	b, _ := json.Marshal(m) // map keys are sorted: the encoding is canonical
	return string(b)
}

func parseMarker(s string) (marker, bool) {
	var m marker
	if json.Unmarshal([]byte(s), &m) != nil || m.Owner == "" {
		return marker{}, false
	}
	return m, true
}

// fingerprint is HMAC-SHA256 under a key only this service holds. A bare
// SHA-256 of a low-entropy secret stored in a readable variable would be an
// offline guessing oracle.
func fingerprint(key []byte, owner, fn, name, value string) string {
	h := hmac.New(sha256.New, key)
	for _, p := range []string{owner, fn, name, value} {
		h.Write([]byte(p))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))[:32]
}
