package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

const functionCols = `id::text, name, coalesce(active_version_id::text, ''), created_at`

func scanFunction(row pgx.Row) (*controlv1.Function, error) {
	var f controlv1.Function
	var created time.Time
	if err := row.Scan(&f.Id, &f.Name, &f.ActiveVersionId, &created); err != nil {
		return nil, err
	}
	f.CreateTime = timestamppb.New(created)
	return &f, nil
}

// CreateFunction creates a function with no versions.
func (s *Store) CreateFunction(ctx context.Context, name string) (*controlv1.Function, error) {
	f, err := scanFunction(s.pool.QueryRow(ctx,
		`INSERT INTO functions (name) VALUES ($1) RETURNING `+functionCols, name))
	switch pgCode(err) {
	case "":
		return f, err
	case pgUniqueViolation:
		return nil, errAlready("function %q", name)
	case pgCheckViolation:
		return nil, invalid("function name %q: want 1-63 lowercase letters, digits or '-', not starting or ending with '-'", name)
	}
	return nil, err
}

// GetFunction returns one function.
func (s *Store) GetFunction(ctx context.Context, id string) (*controlv1.Function, error) {
	id, err := parseID("function", id)
	if err != nil {
		return nil, err
	}
	f, err := scanFunction(s.pool.QueryRow(ctx, `SELECT `+functionCols+` FROM functions WHERE id = $1`, id))
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("function %s", id)
	}
	return f, err
}

// ListFunctions pages through functions ordered by name.
func (s *Store) ListFunctions(ctx context.Context, size uint32, token string) ([]*controlv1.Function, string, error) {
	after, err := decodeToken(token, 1)
	if err != nil {
		return nil, "", err
	}
	n := pageSize(size)
	last := ""
	if after != nil {
		last = after[0]
	}
	rows, err := s.pool.Query(ctx,
		`SELECT `+functionCols+` FROM functions WHERE name > $1 ORDER BY name LIMIT $2`, last, n+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*controlv1.Function
	for rows.Next() {
		f, err := scanFunction(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, f)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > n {
		out = out[:n]
		next = encodeToken(out[n-1].Name)
	}
	return out, next, nil
}

// DeleteFunction removes a function with its versions, routes and
// environment. Invocation records and artifacts are kept.
func (s *Store) DeleteFunction(ctx context.Context, id string) error {
	id, err := parseID("function", id)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM functions WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return notFound("function %s", id)
	}
	return nil
}

// --- versions --------------------------------------------------------------

// NewVersion describes a version to create.
type NewVersion struct {
	FunctionID string
	Module     []byte
	ABI        string
	Limits     *commonv1.Limits
	// Activate deploys the version in the same transaction.
	Activate bool
}

// Digest is the content address of module bytes.
func Digest(b []byte) string {
	sum := sha256.Sum256(b)
	return "sha256:" + hex.EncodeToString(sum[:])
}

const versionCols = `id::text, function_id::text, number, artifact_digest, abi,
	(SELECT size_bytes FROM artifacts a WHERE a.digest = artifact_digest),
	memory_limit_bytes, max_response_bytes, created_at`

func scanVersion(row pgx.Row) (*controlv1.FunctionVersion, error) {
	var v controlv1.FunctionVersion
	var mem, resp, size int64
	var created time.Time
	if err := row.Scan(&v.Id, &v.FunctionId, &v.Number, &v.ArtifactDigest, &v.Abi, &size, &mem, &resp, &created); err != nil {
		return nil, err
	}
	v.SizeBytes = uint64(size)
	v.Limits = &commonv1.Limits{MemoryLimitBytes: uint64(mem), MaxResponseBytes: uint64(resp)}
	v.CreateTime = timestamppb.New(created)
	return &v, nil
}

// CreateVersion stores a new immutable version, numbered one above the
// function's highest. Concurrent calls for one function are serialised on the
// function's row, so numbers are unique and gapless.
func (s *Store) CreateVersion(ctx context.Context, nv NewVersion) (*controlv1.FunctionVersion, error) {
	fid, err := parseID("function", nv.FunctionID)
	if err != nil {
		return nil, err
	}
	switch {
	case len(nv.Module) == 0:
		return nil, invalid("module is empty")
	case nv.ABI == "":
		return nil, invalid("abi is required")
	}
	mem, resp := nv.Limits.GetMemoryLimitBytes(), nv.Limits.GetMaxResponseBytes()
	if mem > math.MaxInt64 || resp > math.MaxInt64 {
		return nil, invalid("limits out of range")
	}
	digest := Digest(nv.Module)

	var out *controlv1.FunctionVersion
	err = s.inTx(ctx, func(tx pgx.Tx) error {
		var locked string
		if err := tx.QueryRow(ctx, `SELECT id::text FROM functions WHERE id = $1 FOR UPDATE`, fid).Scan(&locked); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return notFound("function %s", fid)
			}
			return err
		}
		// Same digest means same bytes: nothing to compare, nothing to replace.
		if _, err := tx.Exec(ctx,
			`INSERT INTO artifacts (digest, size_bytes, data) VALUES ($1, $2, $3) ON CONFLICT (digest) DO NOTHING`,
			digest, len(nv.Module), nv.Module); err != nil {
			return err
		}
		var vid string
		if err := tx.QueryRow(ctx, `INSERT INTO function_versions
			(function_id, number, artifact_digest, abi, memory_limit_bytes, max_response_bytes)
			VALUES ($1, (SELECT coalesce(max(number), 0) + 1 FROM function_versions WHERE function_id = $1), $2, $3, $4, $5)
			RETURNING id::text`, fid, digest, nv.ABI, int64(mem), int64(resp)).Scan(&vid); err != nil {
			return err
		}
		if nv.Activate {
			if _, err := tx.Exec(ctx, `UPDATE functions SET active_version_id = $2 WHERE id = $1`, fid, vid); err != nil {
				return err
			}
		}
		v, err := scanVersion(tx.QueryRow(ctx, `SELECT `+versionCols+` FROM function_versions WHERE id = $1`, vid))
		out = v
		return err
	})
	return out, err
}

// ListVersions pages through a function's versions, newest first.
func (s *Store) ListVersions(ctx context.Context, functionID string, size uint32, token string) ([]*controlv1.FunctionVersion, string, error) {
	fid, err := parseID("function", functionID)
	if err != nil {
		return nil, "", err
	}
	after, err := decodeToken(token, 1)
	if err != nil {
		return nil, "", err
	}
	below := int64(math.MaxInt64)
	if after != nil {
		if below, err = strconv.ParseInt(after[0], 10, 64); err != nil {
			return nil, "", invalid("malformed page token")
		}
	}
	if _, err := s.GetFunction(ctx, fid); err != nil {
		return nil, "", err
	}
	n := pageSize(size)
	rows, err := s.pool.Query(ctx, `SELECT `+versionCols+` FROM function_versions
		WHERE function_id = $1 AND number < $2 ORDER BY number DESC LIMIT $3`, fid, below, n+1)
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*controlv1.FunctionVersion
	for rows.Next() {
		v, err := scanVersion(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, v)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > n {
		out = out[:n]
		next = encodeToken(strconv.FormatInt(out[n-1].Number, 10))
	}
	return out, next, nil
}

// ActivateVersion points a function at one of its own versions: deploy, and
// rollback. The schema rejects a version that belongs to another function.
func (s *Store) ActivateVersion(ctx context.Context, functionID, versionID string) (*controlv1.Function, error) {
	fid, err := parseID("function", functionID)
	if err != nil {
		return nil, err
	}
	vid, err := parseID("version", versionID)
	if err != nil {
		return nil, err
	}
	f, err := scanFunction(s.pool.QueryRow(ctx,
		`UPDATE functions SET active_version_id = $2 WHERE id = $1 RETURNING `+functionCols, fid, vid))
	switch {
	case errors.Is(err, pgx.ErrNoRows):
		return nil, notFound("function %s", fid)
	case pgCode(err) == pgFKViolation:
		return nil, notFound("version %s of function %s", vid, fid)
	}
	return f, err
}

// GetArtifact returns module bytes by digest.
func (s *Store) GetArtifact(ctx context.Context, digest string) ([]byte, error) {
	var data []byte
	err := s.pool.QueryRow(ctx, `SELECT data FROM artifacts WHERE digest = $1`, digest).Scan(&data)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, notFound("artifact %s", digest)
	}
	return data, err
}
