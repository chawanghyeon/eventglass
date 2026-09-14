-- Only durable record boundaries are tracked. An empty Replay/Feedback envelope does
-- not claim that searchable records were accepted.
CREATE TABLE project_ingest_state (
    project_id INTEGER PRIMARY KEY REFERENCES projects(id) ON DELETE CASCADE,
    last_accepted_at_us INTEGER NOT NULL,
    last_accepted_ingest_seq INTEGER NOT NULL,
    accepted_records INTEGER NOT NULL,
    last_searchable_at_us INTEGER,
    last_searchable_ingest_seq INTEGER,
    searchable_records INTEGER NOT NULL DEFAULT 0
);

-- Existing pending Inbox chunks must be counted before new accepts arrive;
-- completed historical records have no per-project receipt boundary.
INSERT INTO project_ingest_state(
    project_id,last_accepted_at_us,last_accepted_ingest_seq,accepted_records
)
SELECT i.project_id,
       (SELECT latest.received_at_us FROM inbox latest
        WHERE latest.project_id=i.project_id ORDER BY latest.id DESC LIMIT 1),
       max(i.last_ingest_seq),sum(i.record_count)
FROM inbox i GROUP BY i.project_id;
