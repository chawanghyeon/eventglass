-- Existing occurrences have no recorded receive timestamp in SQLite. Retain
-- their previous event-time activity until they age out; new occurrences use
-- the durable server receive timestamp for clock-skew-resistant triage.
ALTER TABLE issue_occurrences ADD COLUMN received_at_us INTEGER;
UPDATE issue_occurrences SET received_at_us=occurred_at_us;
CREATE INDEX issue_activity_received
    ON issue_occurrences(issue_id, received_at_us DESC);
