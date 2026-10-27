-- The network destinations a function may reach, as the host patterns its
-- operator listed. Empty means none: functions are closed off by default. The
-- grammar is checked by the application (internal/egress/policy); the schema
-- only bounds the size.
ALTER TABLE functions
    ADD COLUMN egress_allow text[] NOT NULL DEFAULT '{}'
        CONSTRAINT functions_egress_allow_size CHECK (cardinality(egress_allow) <= 64);
