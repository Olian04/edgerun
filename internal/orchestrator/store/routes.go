package store

import (
	"context"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

const routeCols = `id::text, host_pattern, path_prefix, function_id::text, created_at`

func scanRoute(row pgx.Row) (*controlv1.Route, error) {
	var r controlv1.Route
	var created time.Time
	if err := row.Scan(&r.Id, &r.HostPattern, &r.PathPrefix, &r.FunctionId, &created); err != nil {
		return nil, err
	}
	r.CreateTime = timestamppb.New(created)
	return &r, nil
}

// NormalizeRoute returns the canonical form of a route: the host lower-cased,
// the path prefix without a trailing slash (except the root "/"). Prefixes
// match on whole path segments, so "/api" and "/api/" are the same route.
func NormalizeRoute(host, prefix string) (string, string, error) {
	host = strings.ToLower(strings.TrimSpace(host))
	if prefix == "" || prefix[0] != '/' {
		return "", "", invalid("path prefix %q must start with '/'", prefix)
	}
	if strings.Contains(prefix, "//") || strings.ContainsAny(prefix, "?# \t\r\n\x00") {
		return "", "", invalid("path prefix %q must not contain '//', '?', '#' or whitespace", prefix)
	}
	if len(prefix) > 1 {
		prefix = strings.TrimSuffix(prefix, "/")
	}
	return host, prefix, nil
}

// CreateRoute routes (host, prefix) to a function.
func (s *Store) CreateRoute(ctx context.Context, host, prefix, functionID string) (*controlv1.Route, error) {
	host, prefix, err := NormalizeRoute(host, prefix)
	if err != nil {
		return nil, err
	}
	fid, err := parseID("function", functionID)
	if err != nil {
		return nil, err
	}
	r, err := scanRoute(s.pool.QueryRow(ctx,
		`INSERT INTO routes (host_pattern, path_prefix, function_id) VALUES ($1, $2, $3) RETURNING `+routeCols,
		host, prefix, fid))
	switch pgCode(err) {
	case "":
		return r, err
	case pgUniqueViolation:
		return nil, errAlready("route %s%s", host, prefix)
	case pgFKViolation:
		return nil, notFound("function %s", fid)
	case pgCheckViolation:
		return nil, invalid("host pattern %q: want a lower-case host, \"*.suffix\", or \"*\"", host)
	}
	return nil, err
}

// ListRoutes pages through routes ordered by (host, path prefix).
func (s *Store) ListRoutes(ctx context.Context, size uint32, token string) ([]*controlv1.Route, string, error) {
	after, err := decodeToken(token, 2)
	if err != nil {
		return nil, "", err
	}
	n := pageSize(size)
	var rows pgx.Rows
	if after == nil {
		rows, err = s.pool.Query(ctx, `SELECT `+routeCols+` FROM routes ORDER BY host_pattern, path_prefix LIMIT $1`, n+1)
	} else {
		rows, err = s.pool.Query(ctx, `SELECT `+routeCols+` FROM routes
			WHERE (host_pattern, path_prefix) > ($1, $2) ORDER BY host_pattern, path_prefix LIMIT $3`, after[0], after[1], n+1)
	}
	if err != nil {
		return nil, "", err
	}
	defer rows.Close()
	var out []*controlv1.Route
	for rows.Next() {
		r, err := scanRoute(rows)
		if err != nil {
			return nil, "", err
		}
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, "", err
	}
	next := ""
	if len(out) > n {
		out = out[:n]
		next = encodeToken(out[n-1].HostPattern, out[n-1].PathPrefix)
	}
	return out, next, nil
}

// DeleteRoute removes a route.
func (s *Store) DeleteRoute(ctx context.Context, id string) error {
	id, err := parseID("route", id)
	if err != nil {
		return err
	}
	tag, err := s.pool.Exec(ctx, `DELETE FROM routes WHERE id = $1`, id)
	if err != nil {
		return err
	}
	if tag.RowsAffected() == 0 {
		return notFound("route %s", id)
	}
	return nil
}
