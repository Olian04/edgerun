package store

import (
	"context"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
)

// VersionRef is the deployed version of a routed function: what a worker needs
// to run it.
type VersionRef struct {
	ID     string
	Digest string
	ABI    string
	Limits *commonv1.Limits
}

// RouteEntry is one route with everything needed to invoke its function.
type RouteEntry struct {
	RouteID      string
	HostPattern  string
	PathPrefix   string
	FunctionID   string
	FunctionName string
	// Version is nil when the function has no active version.
	Version *VersionRef
	// Env holds decrypted values: only the orchestrator ever sees them.
	Env []*commonv1.EnvVar
	// EnvErr is set if a secret could not be opened. Only this function is
	// affected: one corrupt secret must not take every route down.
	EnvErr error
}

// LoadRouting reads every route with its function's deployed version and
// environment, as one consistent snapshot for the invoke path to cache.
func (s *Store) LoadRouting(ctx context.Context) ([]RouteEntry, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback(ctx)
	// Repeatable read: routes, versions and env come from the same instant.
	if _, err := tx.Exec(ctx, `SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY`); err != nil {
		return nil, err
	}

	rows, err := tx.Query(ctx, `SELECT r.id::text, r.host_pattern, r.path_prefix, f.id::text, f.name,
			coalesce(v.id::text, ''), coalesce(v.artifact_digest, ''), coalesce(v.abi, ''),
			coalesce(v.memory_limit_bytes, 0), coalesce(v.max_response_bytes, 0)
		FROM routes r
		JOIN functions f ON f.id = r.function_id
		LEFT JOIN function_versions v ON v.id = f.active_version_id
		ORDER BY r.host_pattern, r.path_prefix`)
	if err != nil {
		return nil, err
	}
	var out []RouteEntry
	byFunction := map[string][]int{} // function id -> indexes into out
	for rows.Next() {
		var e RouteEntry
		var vid, digest, abi string
		var mem, resp int64
		if err := rows.Scan(&e.RouteID, &e.HostPattern, &e.PathPrefix, &e.FunctionID, &e.FunctionName, &vid, &digest, &abi, &mem, &resp); err != nil {
			rows.Close()
			return nil, err
		}
		if vid != "" {
			e.Version = &VersionRef{ID: vid, Digest: digest, ABI: abi,
				Limits: &commonv1.Limits{MemoryLimitBytes: uint64(mem), MaxResponseBytes: uint64(resp)}}
		}
		byFunction[e.FunctionID] = append(byFunction[e.FunctionID], len(out))
		out = append(out, e)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}

	envRows, err := tx.Query(ctx, `SELECT function_id::text, name, value, secret FROM env_vars
		WHERE function_id IN (SELECT function_id FROM routes) ORDER BY function_id, name`)
	if err != nil {
		return nil, err
	}
	defer envRows.Close()
	for envRows.Next() {
		var fid, name string
		var value []byte
		var secret bool
		if err := envRows.Scan(&fid, &name, &value, &secret); err != nil {
			return nil, err
		}
		plain := string(value)
		var openErr error
		if secret {
			b, err := s.box.Open(value, envAAD(fid, name))
			if err != nil {
				openErr = err
			}
			plain = string(b)
		}
		for _, i := range byFunction[fid] {
			if openErr != nil {
				out[i].EnvErr = openErr
				continue
			}
			out[i].Env = append(out[i].Env, &commonv1.EnvVar{Name: name, Value: plain})
		}
	}
	return out, envRows.Err()
}
