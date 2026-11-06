-- edgerun schema, version 1.
--
-- Only the orchestrator touches this database. Invariants that the code relies
-- on are enforced here, not just in Go, so that several orchestrator instances
-- (and mistakes) cannot violate them.

-- Module bytes, content-addressed. Immutable: a digest never maps to other bytes.
CREATE TABLE artifacts (
    digest      text        PRIMARY KEY CHECK (digest ~ '^sha256:[0-9a-f]{64}$'),
    size_bytes  bigint      NOT NULL,
    data        bytea       NOT NULL,
    created_at  timestamptz NOT NULL DEFAULT now(),
    CHECK (size_bytes = octet_length(data))
);

CREATE TABLE functions (
    id                uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    name              text        NOT NULL UNIQUE
                                  CHECK (name ~ '^[a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?$'),
    active_version_id uuid,
    created_at        timestamptz NOT NULL DEFAULT now()
);

-- Immutable once created. Rolling back is moving functions.active_version_id.
CREATE TABLE function_versions (
    id                 uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    function_id        uuid        NOT NULL REFERENCES functions (id) ON DELETE CASCADE,
    number             bigint      NOT NULL CHECK (number >= 1),
    artifact_digest    text        NOT NULL REFERENCES artifacts (digest),
    abi                text        NOT NULL CHECK (abi <> ''),
    memory_limit_bytes bigint      NOT NULL DEFAULT 0 CHECK (memory_limit_bytes >= 0),
    max_response_bytes bigint      NOT NULL DEFAULT 0 CHECK (max_response_bytes >= 0),
    created_at         timestamptz NOT NULL DEFAULT now(),
    UNIQUE (function_id, number),
    UNIQUE (id, function_id)
);

-- A function can only be pointed at one of its own versions. Deleting the
-- active version clears the pointer instead of the function's identity.
ALTER TABLE functions
    ADD CONSTRAINT functions_active_version_belongs_to_function
    FOREIGN KEY (active_version_id, id) REFERENCES function_versions (id, function_id)
    ON DELETE SET NULL (active_version_id);

CREATE TABLE routes (
    id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    -- lower-case host, "*.suffix", or "*"
    host_pattern text        NOT NULL
                             CHECK (host_pattern ~ '^(\*|(\*\.)?[a-z0-9]([a-z0-9.-]*[a-z0-9])?)$'),
    path_prefix  text        NOT NULL CHECK (path_prefix LIKE '/%'),
    function_id  uuid        NOT NULL REFERENCES functions (id) ON DELETE CASCADE,
    created_at   timestamptz NOT NULL DEFAULT now(),
    UNIQUE (host_pattern, path_prefix)
);
CREATE INDEX routes_by_function ON routes (function_id);

-- Plain values are stored as UTF-8 bytes; secret values as AES-GCM ciphertext.
CREATE TABLE env_vars (
    function_id uuid    NOT NULL REFERENCES functions (id) ON DELETE CASCADE,
    name        text    NOT NULL CHECK (name ~ '^[A-Za-z_][A-Za-z0-9_]*$'),
    value       bytea   NOT NULL,
    secret      boolean NOT NULL,
    PRIMARY KEY (function_id, name)
);

-- The fleet. last_heartbeat is set with the database clock, so orchestrator
-- instances never need synchronised clocks to agree on who is alive.
CREATE TABLE workers (
    id              uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
    endpoint        text        NOT NULL UNIQUE,
    info            jsonb       NOT NULL,   -- protojson of worker.v1.WorkerInfo
    abis            text[]      NOT NULL,   -- denormalised from info, for scheduling
    max_concurrency integer     NOT NULL CHECK (max_concurrency > 0),
    state           smallint    NOT NULL,   -- worker.v1.WorkerState
    in_flight       integer     NOT NULL DEFAULT 0,
    registered_at   timestamptz NOT NULL DEFAULT now(),
    last_heartbeat  timestamptz NOT NULL DEFAULT now()
);

-- Deliberately no foreign keys: records must outlive the things they describe.
CREATE TABLE invocations (
    id                uuid        PRIMARY KEY,
    request_id        text        NOT NULL,
    function_id       uuid,
    version_id        uuid,
    worker_id         uuid,
    method            text        NOT NULL,
    path              text        NOT NULL,
    status            integer     NOT NULL DEFAULT 0,
    error_reason      integer     NOT NULL DEFAULT 0,   -- common.v1.ErrorReason
    duration_micros   bigint      NOT NULL DEFAULT 0,
    peak_memory_bytes bigint      NOT NULL DEFAULT 0,
    stderr            bytea       NOT NULL DEFAULT ''::bytea,
    start_time        timestamptz NOT NULL
);
CREATE INDEX invocations_recent ON invocations (start_time DESC, id DESC);
CREATE INDEX invocations_by_function ON invocations (function_id, start_time DESC, id DESC);
