package store

import (
	"context"
	"net/url"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	workerv1 "github.com/Olian04/edgerun/gen/edgerun/worker/v1"
)

// RegisterWorker records a worker and returns its fresh id. Registering an
// endpoint that is already known replaces the old entry (the worker restarted).
func (s *Store) RegisterWorker(ctx context.Context, endpoint string, info *workerv1.WorkerInfo) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", invalid("worker endpoint %q: want an http(s) URL", endpoint)
	}
	if info == nil || info.GetMaxConcurrency() == 0 {
		return "", invalid("worker info with a positive max_concurrency is required")
	}
	if len(info.GetAbis()) == 0 {
		return "", invalid("a worker must support at least one ABI")
	}
	doc, err := protojson.Marshal(info)
	if err != nil {
		return "", err
	}
	var id string
	err = s.pool.QueryRow(ctx, `INSERT INTO workers (endpoint, info, abis, max_concurrency, state)
		VALUES ($1, $2, $3, $4, $5)
		ON CONFLICT (endpoint) DO UPDATE SET
			id = gen_random_uuid(), info = EXCLUDED.info, abis = EXCLUDED.abis,
			max_concurrency = EXCLUDED.max_concurrency, state = EXCLUDED.state,
			in_flight = 0, registered_at = now(), last_heartbeat = now()
		RETURNING id::text`,
		endpoint, doc, info.GetAbis(), int32(info.GetMaxConcurrency()), int16(workerv1.WorkerState_WORKER_STATE_READY)).Scan(&id)
	return id, err
}

// Heartbeat records liveness and load. known is false if no such worker is
// registered (it was deregistered or replaced), in which case the worker must
// register again.
func (s *Store) Heartbeat(ctx context.Context, id string, state workerv1.WorkerState, inFlight uint32) (known bool, err error) {
	wid, perr := parseID("worker", id)
	if perr != nil {
		return false, nil
	}
	tag, err := s.pool.Exec(ctx, `UPDATE workers SET state = $2, in_flight = $3, last_heartbeat = now() WHERE id = $1`,
		wid, int16(state), int32(inFlight))
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// DeregisterWorker removes a worker. Removing an unknown worker is not an error.
func (s *Store) DeregisterWorker(ctx context.Context, id string) error {
	wid, err := parseID("worker", id)
	if err != nil {
		return nil
	}
	_, err = s.pool.Exec(ctx, `DELETE FROM workers WHERE id = $1`, wid)
	return err
}

// PruneWorkers deletes workers that have not heartbeated for longer than
// olderThan, and returns how many it removed.
func (s *Store) PruneWorkers(ctx context.Context, olderThan time.Duration) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM workers WHERE last_heartbeat < now() - make_interval(secs => $1)`, olderThan.Seconds())
	return int(tag.RowsAffected()), err
}

// ListWorkers returns every registered worker. A worker is alive if it
// heartbeated within aliveWithin, by the database's clock.
func (s *Store) ListWorkers(ctx context.Context, aliveWithin time.Duration) ([]*controlv1.WorkerStatus, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text, endpoint, info, state, in_flight, last_heartbeat,
			last_heartbeat > now() - make_interval(secs => $1)
		FROM workers ORDER BY endpoint`, aliveWithin.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*controlv1.WorkerStatus
	for rows.Next() {
		var w controlv1.WorkerStatus
		var doc []byte
		var state int16
		var inFlight int32
		var hb time.Time
		if err := rows.Scan(&w.Id, &w.Endpoint, &doc, &state, &inFlight, &hb, &w.Alive); err != nil {
			return nil, err
		}
		w.Info = &workerv1.WorkerInfo{}
		if err := protojson.Unmarshal(doc, w.Info); err != nil {
			return nil, err
		}
		w.State, w.InFlight = workerv1.WorkerState(state), uint32(inFlight)
		w.LastHeartbeatTime = timestamppb.New(hb)
		out = append(out, &w)
	}
	return out, rows.Err()
}

// Worker is what the scheduler needs to know about a candidate.
type Worker struct {
	ID             string
	Endpoint       string
	ABIs           []string
	MaxConcurrency uint32
	InFlight       uint32
}

// Supports reports whether the worker can run modules built for abi.
func (w Worker) Supports(abi string) bool {
	for _, a := range w.ABIs {
		if a == abi {
			return true
		}
	}
	return false
}

// ReadyWorkers returns the workers that are alive and READY, with their ABIs.
// Filtering by ABI is the scheduler's job: it needs the whole set to tell "no
// worker supports this ABI" from "no worker is alive".
func (s *Store) ReadyWorkers(ctx context.Context, aliveWithin time.Duration) ([]Worker, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text, endpoint, abis, max_concurrency, in_flight FROM workers
		WHERE state = $1 AND last_heartbeat > now() - make_interval(secs => $2)
		ORDER BY endpoint`,
		int16(workerv1.WorkerState_WORKER_STATE_READY), aliveWithin.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Worker
	for rows.Next() {
		var w Worker
		var maxC, inF int32
		if err := rows.Scan(&w.ID, &w.Endpoint, &w.ABIs, &maxC, &inF); err != nil {
			return nil, err
		}
		w.MaxConcurrency, w.InFlight = uint32(maxC), uint32(inF)
		out = append(out, w)
	}
	return out, rows.Err()
}
