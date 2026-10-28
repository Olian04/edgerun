package store

import (
	"context"
	"strconv"
	"time"

	"github.com/jackc/pgx/v5"
	"google.golang.org/protobuf/types/known/timestamppb"

	commonv1 "github.com/Olian04/edgerun/gen/edgerun/common/v1"
	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
)

const (
	maxPathBytes   = 2048
	maxStderrBytes = 64 << 10
)

// InsertInvocations stores invocation records in one round trip. Ids must be
// UUIDs chosen by the caller (the id is returned to clients before the record
// is written). Oversized fields are truncated rather than rejected: losing a
// log tail is better than losing the record.
func (s *Store) InsertInvocations(ctx context.Context, invs []*controlv1.Invocation) error {
	if len(invs) == 0 {
		return nil
	}
	batch := &pgx.Batch{}
	for _, v := range invs {
		path := v.GetPath()
		if len(path) > maxPathBytes {
			path = path[:maxPathBytes]
		}
		stderr := v.GetStderr()
		if stderr == nil {
			stderr = []byte{} // a nil slice would be sent as NULL; the column is NOT NULL
		}
		if len(stderr) > maxStderrBytes {
			stderr = stderr[len(stderr)-maxStderrBytes:]
		}
		batch.Queue(`INSERT INTO invocations
			(id, request_id, function_id, version_id, worker_id, method, path, status, error_reason,
			 duration_micros, peak_memory_bytes, stderr, start_time)
			VALUES ($1::uuid, $2, NULLIF($3, '')::uuid, NULLIF($4, '')::uuid, NULLIF($5, '')::uuid, $6, $7, $8, $9, $10, $11, $12, $13)
			ON CONFLICT (id) DO NOTHING`,
			v.GetId(), v.GetRequestId(), v.GetFunctionId(), v.GetVersionId(), v.GetWorkerId(), v.GetMethod(), path,
			int32(v.GetStatus()), int32(v.GetErrorReason()), int64(v.GetDurationMicros()), int64(v.GetPeakMemoryBytes()),
			stderr, v.GetStartTime().AsTime())
	}
	br := s.pool.SendBatch(ctx, batch)
	defer br.Close()
	for range invs {
		if _, err := br.Exec(); err != nil {
			return err
		}
	}
	return nil
}

const invocationCols = `id::text, request_id, coalesce(function_id::text, ''), coalesce(version_id::text, ''),
	coalesce(worker_id::text, ''), method, path, status, error_reason, duration_micros, peak_memory_bytes, stderr, start_time`

// ListInvocations pages through records, newest first, optionally for one
// function.
func (s *Store) ListInvocations(ctx context.Context, functionID string, size uint32, token string) ([]*controlv1.Invocation, string, error) {
	after, err := decodeToken(token, 2)
	if err != nil {
		return nil, "", err
	}
	n := pageSize(size)

	where, args := "TRUE", []any{}
	if functionID != "" {
		fid, err := parseID("function", functionID)
		if err != nil {
			return nil, "", nil // no function has this id, so no records
		}
		args = append(args, fid)
		where = "function_id = $" + strconv.Itoa(len(args))
	}
	if after != nil {
		micros, err := strconv.ParseInt(after[0], 10, 64)
		if err != nil {
			return nil, "", invalid("malformed page token")
		}
		args = append(args, time.UnixMicro(micros).UTC(), after[1])
		where += " AND (start_time, id) < ($" + strconv.Itoa(len(args)-1) + "::timestamptz, $" + strconv.Itoa(len(args)) + "::uuid)"
	}
	args = append(args, n+1)
	rows, err := s.q(ctx).Query(ctx, `SELECT `+invocationCols+` FROM invocations WHERE `+where+
		` ORDER BY start_time DESC, id DESC LIMIT $`+strconv.Itoa(len(args)), args...)
	if err != nil {
		if pgCode(err) == "22P02" { // invalid_text_representation: a forged token id
			return nil, "", invalid("malformed page token")
		}
		return nil, "", err
	}
	defer rows.Close()
	var out []*controlv1.Invocation
	for rows.Next() {
		var v controlv1.Invocation
		var status, reason int32
		var dur, peak int64
		var start time.Time
		if err := rows.Scan(&v.Id, &v.RequestId, &v.FunctionId, &v.VersionId, &v.WorkerId, &v.Method, &v.Path,
			&status, &reason, &dur, &peak, &v.Stderr, &start); err != nil {
			return nil, "", err
		}
		v.Status, v.ErrorReason = uint32(status), commonv1.ErrorReason(reason)
		v.DurationMicros, v.PeakMemoryBytes = uint64(dur), uint64(peak)
		v.StartTime = timestamppb.New(start)
		out = append(out, &v)
	}
	if err := rows.Err(); err != nil {
		if pgCode(err) == "22P02" {
			return nil, "", invalid("malformed page token")
		}
		return nil, "", err
	}
	next := ""
	if len(out) > n {
		out = out[:n]
		last := out[n-1]
		next = encodeToken(strconv.FormatInt(last.StartTime.AsTime().UnixMicro(), 10), last.Id)
	}
	return out, next, nil
}

// DeleteInvocationsBefore removes up to limit records older than t and returns
// how many it removed; call it until it returns less than limit.
func (s *Store) DeleteInvocationsBefore(ctx context.Context, t time.Time, limit int) (int, error) {
	tag, err := s.q(ctx).Exec(ctx, `DELETE FROM invocations WHERE id IN
		(SELECT id FROM invocations WHERE start_time < $1 ORDER BY start_time LIMIT $2)`, t, limit)
	return int(tag.RowsAffected()), err
}
