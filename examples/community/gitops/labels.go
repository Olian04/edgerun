package main

import (
	"maps"
	"sort"
	"strconv"
	"strings"

	"github.com/Olian04/edgerun"
)

// Everything this service writes into edgerun's labels lives under one
// namespace; a label outside it belongs to someone else and is carried over
// untouched whenever the service has to replace a function's label set
// (SetLabels replaces the whole map).
//
//	function  gitops.example.com/owner=<--owner>
//	          gitops.example.com/secrets.0 .. .N   revisions of the secrets last written (below)
//	version   gitops.example.com/owner=<--owner>
//	          gitops.example.com/commit=<git commit the version was first uploaded from>
//	route     gitops.example.com/owner=<--owner>
const (
	labelNS     = "gitops.example.com/"
	labelOwner  = labelNS + "owner"
	labelCommit = labelNS + "commit"
	labelSecret = labelNS + "secrets."
)

// ownerLabels is the filter and the stamp of one instance.
func ownerLabels(owner string) map[string]string { return map[string]string{labelOwner: owner} }

// The record of secret revisions.
//
// A secret cannot be read back, but EnvEntry.revision identifies the stored
// value, so the service remembers the revision its last write returned and
// compares it with what ListEnv says now. Where? A label of the function is the
// natural place, with three constraints: at most 32 labels per function, a value
// of at most 253 printable ASCII characters, and a key of [a-z0-9._/-]. An
// environment variable name may be upper case and any length, and a function may
// hold 64 variables, so one label per secret does not fit. Each secret becomes a
// fixed-width entry of 24 hex characters
//
//	nameTag(8) valueTag(16)
//
// nameTag is a hash of the variable name (so no name has to be representable as
// a label key), valueTag a hash of the name and the revision (the revision is
// opaque: it is compared, not interpreted). Ten entries fill a label, in order of
// nameTag, so 64 secrets need 7 labels. The tags are not secret: the revision is
// already visible to anyone who can list the function, and it cannot be used to
// guess a value.
const (
	entryLen = 24
	perLabel = 10
)

func nameTag(name string) string {
	return edgerun.Digest([]byte(name))[len("sha256:") : len("sha256:")+8]
}

func valueTag(name, revision string) string {
	return edgerun.Digest([]byte(name + "\x00" + revision))[len("sha256:") : len("sha256:")+16]
}

// encodeSecrets turns name -> revision into the labels that record them.
func encodeSecrets(revs map[string]string) map[string]string {
	entries := make([]string, 0, len(revs))
	for n, r := range revs {
		entries = append(entries, nameTag(n)+valueTag(n, r))
	}
	sort.Strings(entries)
	out := map[string]string{}
	for i := 0; i < len(entries); i += perLabel {
		out[labelSecret+strconv.Itoa(i/perLabel)] = strings.Join(entries[i:min(i+perLabel, len(entries))], "")
	}
	return out
}

// decodeSecrets returns nameTag -> valueTag of what the labels record.
func decodeSecrets(labels map[string]string) map[string]string {
	out := map[string]string{}
	for k, v := range labels {
		if !strings.HasPrefix(k, labelSecret) {
			continue
		}
		for len(v) >= entryLen {
			out[v[:8]] = v[8:entryLen]
			v = v[entryLen:]
		}
	}
	return out
}

// foreignLabels is a label set without anything this service writes for the
// secrets: the part of a function's labels it must preserve.
func foreignLabels(labels map[string]string) map[string]string {
	out := maps.Clone(labels)
	if out == nil {
		out = map[string]string{}
	}
	for k := range out {
		if strings.HasPrefix(k, labelSecret) {
			delete(out, k)
		}
	}
	return out
}

// functionLabels is the label set of a function this service manages: whatever
// others put there, our owner, and the record of the secrets' revisions.
func functionLabels(base map[string]string, owner string, revs map[string]string) map[string]string {
	out := foreignLabels(base)
	out[labelOwner] = owner
	maps.Copy(out, encodeSecrets(revs))
	return out
}
