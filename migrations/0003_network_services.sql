-- Network services that registered with an orchestrator, and what each carries.
-- Like workers they dial in and heartbeat; liveness is judged by the database's
-- clock. The coverage grammar is checked by the application
-- (internal/egress/policy); the schema only bounds the size.
CREATE TABLE network_services (
    id             uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint       text        NOT NULL UNIQUE,
    info           jsonb       NOT NULL,   -- protojson of egress.v1.ServiceInfo
    covers         text[]      NOT NULL CHECK (cardinality(covers) BETWEEN 1 AND 64),
    state          smallint    NOT NULL,   -- egress.v1.ServiceState
    registered_at  timestamptz NOT NULL DEFAULT now(),
    last_heartbeat timestamptz NOT NULL DEFAULT now()
);
