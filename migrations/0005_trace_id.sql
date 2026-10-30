-- The trace an invocation belongs to (the W3C trace id as 32 hex digits), so a
-- record can be followed to its trace. Empty when the request carried none.
ALTER TABLE invocations ADD COLUMN trace_id text NOT NULL DEFAULT '';
