-- ListInvocations can be asked for the invocations of one request id: without an
-- index that would read every record there is.
CREATE INDEX invocations_request_id ON invocations (request_id);
