-- Labels: free-form key/value metadata that whoever manages a resource sets,
-- and that nothing in edgerun interprets. Their form (keys, values, counts) is
-- checked by the application; the schema keeps them to a JSON object of strings.
-- The containment index serves the match_labels filter of the list calls.
ALTER TABLE functions
    ADD COLUMN labels jsonb NOT NULL DEFAULT '{}'
        CONSTRAINT functions_labels_object CHECK (jsonb_typeof(labels) = 'object');
ALTER TABLE function_versions
    ADD COLUMN labels jsonb NOT NULL DEFAULT '{}'
        CONSTRAINT function_versions_labels_object CHECK (jsonb_typeof(labels) = 'object');
ALTER TABLE routes
    ADD COLUMN labels jsonb NOT NULL DEFAULT '{}'
        CONSTRAINT routes_labels_object CHECK (jsonb_typeof(labels) = 'object');

CREATE INDEX functions_labels ON functions USING gin (labels jsonb_path_ops);
CREATE INDEX routes_labels ON routes USING gin (labels jsonb_path_ops);
