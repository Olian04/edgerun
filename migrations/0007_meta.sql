-- Facts about the installation itself. 'secret-key-check' is a known text sealed with the
-- orchestrators' secret key, so that an orchestrator started with another key is refused
-- instead of failing on every secret.
CREATE TABLE orchestrator_meta (
    key   text  PRIMARY KEY,
    value bytea NOT NULL
);
