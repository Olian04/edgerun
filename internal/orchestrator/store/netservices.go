package store

import (
	"context"
	"net/url"
	"time"

	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/timestamppb"

	controlv1 "github.com/Olian04/edgerun/gen/edgerun/control/v1"
	egressv1 "github.com/Olian04/edgerun/gen/edgerun/egress/v1"
	"github.com/Olian04/edgerun/internal/egress/policy"
)

// RegisterNetworkService records a network service and returns its fresh id.
// Registering an endpoint that is already known replaces the old entry (the
// service restarted). The coverage is stored in canonical form.
func (s *Store) RegisterNetworkService(ctx context.Context, endpoint string, info *egressv1.ServiceInfo) (string, error) {
	u, err := url.Parse(endpoint)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return "", invalid("network service endpoint %q: want an http(s) URL", endpoint)
	}
	covers, err := policy.CanonicalCovers(info.GetCovers())
	if err != nil {
		return "", invalid("%v", err)
	}
	stored := &egressv1.ServiceInfo{Implementation: info.GetImplementation(), Covers: covers}
	doc, err := protojson.Marshal(stored)
	if err != nil {
		return "", err
	}
	var id string
	err = s.pool.QueryRow(ctx, `INSERT INTO network_services (endpoint, info, covers, state)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (endpoint) DO UPDATE SET
			id = gen_random_uuid(), info = EXCLUDED.info, covers = EXCLUDED.covers, state = EXCLUDED.state,
			registered_at = now(), last_heartbeat = now()
		RETURNING id::text`,
		endpoint, doc, covers, int16(egressv1.ServiceState_SERVICE_STATE_READY)).Scan(&id)
	return id, err
}

// HeartbeatNetworkService records liveness and state. known is false if no
// such service is registered, in which case it must register again.
func (s *Store) HeartbeatNetworkService(ctx context.Context, id string, state egressv1.ServiceState) (known bool, err error) {
	sid, perr := parseID("network service", id)
	if perr != nil {
		return false, nil
	}
	tag, err := s.pool.Exec(ctx, `UPDATE network_services SET state = $2, last_heartbeat = now() WHERE id = $1`, sid, int16(state))
	if err != nil {
		return false, err
	}
	return tag.RowsAffected() == 1, nil
}

// DeregisterNetworkService removes a service. Removing an unknown one is not an error.
func (s *Store) DeregisterNetworkService(ctx context.Context, id string) error {
	sid, err := parseID("network service", id)
	if err != nil {
		return nil
	}
	_, err = s.pool.Exec(ctx, `DELETE FROM network_services WHERE id = $1`, sid)
	return err
}

// PruneNetworkServices deletes services that have not heartbeated for longer
// than olderThan, and returns how many it removed.
func (s *Store) PruneNetworkServices(ctx context.Context, olderThan time.Duration) (int, error) {
	tag, err := s.pool.Exec(ctx, `DELETE FROM network_services WHERE last_heartbeat < now() - make_interval(secs => $1)`, olderThan.Seconds())
	return int(tag.RowsAffected()), err
}

// ListNetworkServices returns every registered service. One is alive if it
// heartbeated within aliveWithin, by the database's clock.
func (s *Store) ListNetworkServices(ctx context.Context, aliveWithin time.Duration) ([]*controlv1.NetworkServiceStatus, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text, endpoint, info, state, last_heartbeat,
			last_heartbeat > now() - make_interval(secs => $1)
		FROM network_services ORDER BY endpoint`, aliveWithin.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*controlv1.NetworkServiceStatus
	for rows.Next() {
		var n controlv1.NetworkServiceStatus
		var doc []byte
		var state int16
		var hb time.Time
		if err := rows.Scan(&n.Id, &n.Endpoint, &doc, &state, &hb, &n.Alive); err != nil {
			return nil, err
		}
		n.Info = &egressv1.ServiceInfo{}
		if err := protojson.Unmarshal(doc, n.Info); err != nil {
			return nil, err
		}
		n.State = egressv1.ServiceState(state)
		n.LastHeartbeatTime = timestamppb.New(hb)
		out = append(out, &n)
	}
	return out, rows.Err()
}

// NetworkService is what routing needs to know about a registered service.
type NetworkService struct {
	ID       string
	Endpoint string
	Covers   policy.Covers
}

// ReadyNetworkServices returns the services that are alive and READY.
func (s *Store) ReadyNetworkServices(ctx context.Context, aliveWithin time.Duration) ([]NetworkService, error) {
	rows, err := s.pool.Query(ctx, `SELECT id::text, endpoint, covers FROM network_services
		WHERE state = $1 AND last_heartbeat > now() - make_interval(secs => $2)
		ORDER BY endpoint`,
		int16(egressv1.ServiceState_SERVICE_STATE_READY), aliveWithin.Seconds())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []NetworkService
	for rows.Next() {
		var n NetworkService
		var covers []string
		if err := rows.Scan(&n.ID, &n.Endpoint, &covers); err != nil {
			return nil, err
		}
		if n.Covers, err = policy.ParseCovers(covers); err != nil {
			continue // stored by an older grammar: never route to what cannot be read
		}
		out = append(out, n)
	}
	return out, rows.Err()
}
