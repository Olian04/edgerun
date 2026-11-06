package main

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
)

// Source is a git repository mirrored into a bare clone. Everything is read
// from one commit (a Snapshot), never from a working tree, so the YAML and the
// modules it names always come from the same revision, even if a fetch happens
// in between.
type Source struct {
	URL    string // anything git accepts: https://github.com/o/r.git, file:///path
	Branch string
	Dir    string // where the bare clone lives
}

func (s *Source) git(ctx context.Context, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Env = append(os.Environ(), "GIT_TERMINAL_PROMPT=0", "LC_ALL=C")
	var out, errb bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &errb
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(errb.String()))
	}
	return out.Bytes(), nil
}

// Sync clones on first use, fetches afterwards, and returns the commit the
// branch points at.
func (s *Source) Sync(ctx context.Context) (*Snapshot, error) {
	if _, err := os.Stat(filepath.Join(s.Dir, "HEAD")); err != nil {
		if err := os.MkdirAll(filepath.Dir(s.Dir), 0o755); err != nil {
			return nil, err
		}
		if _, err := s.git(ctx, "clone", "--quiet", "--bare", "--", s.URL, s.Dir); err != nil {
			return nil, err
		}
	}
	ref := "refs/gitops/" + s.Branch
	if _, err := s.git(ctx, "-C", s.Dir, "fetch", "--quiet", "--force", "origin", "refs/heads/"+s.Branch+":"+ref); err != nil {
		return nil, err
	}
	out, err := s.git(ctx, "-C", s.Dir, "rev-parse", "--verify", ref+"^{commit}")
	if err != nil {
		return nil, err
	}
	return &Snapshot{src: s, Rev: strings.TrimSpace(string(out))}, nil
}

type Snapshot struct {
	src *Source
	Rev string
}

// ReadFile returns a regular file of the commit. Symlinks are refused (they
// would be read as their target text), and so is a Git LFS pointer, which is
// what a .wasm tracked by LFS looks like to plain git and which would
// otherwise be uploaded as a "module".
func (sn *Snapshot) ReadFile(ctx context.Context, p string) ([]byte, error) {
	if err := checkRepoPath(p); err != nil {
		return nil, fmt.Errorf("%s: %w", p, err)
	}
	p = path.Clean(p)
	out, err := sn.src.git(ctx, "-C", sn.src.Dir, "ls-tree", "-z", sn.Rev, "--", p)
	if err != nil {
		return nil, err
	}
	meta, _, ok := strings.Cut(strings.TrimSuffix(string(out), "\x00"), "\t")
	f := strings.Fields(meta)
	if !ok || len(f) != 3 {
		return nil, fmt.Errorf("%s: not in %s", p, sn.Rev[:10])
	}
	mode, typ, sha := f[0], f[1], f[2]
	if typ != "blob" || (mode != "100644" && mode != "100755") {
		return nil, fmt.Errorf("%s: not a regular file (mode %s, %s)", p, mode, typ)
	}
	data, err := sn.src.git(ctx, "-C", sn.src.Dir, "cat-file", "blob", sha)
	if err != nil {
		return nil, err
	}
	if bytes.HasPrefix(data, []byte("version https://git-lfs.github.com/spec")) {
		return nil, fmt.Errorf("%s: is a Git LFS pointer, not the file (this tool does not fetch LFS objects)", p)
	}
	return data, nil
}
