CREATE TABLE replays (
    project_id INTEGER NOT NULL REFERENCES projects(id),
    replay_id TEXT NOT NULL CHECK(length(replay_id)=32),
    started_at_ms INTEGER NOT NULL,
    finished_at_ms INTEGER NOT NULL,
    expires_at_us INTEGER NOT NULL,
    metadata TEXT NOT NULL,
    segment_count INTEGER NOT NULL DEFAULT 0,
    max_segment_id INTEGER NOT NULL DEFAULT 0,
    recording_bytes INTEGER NOT NULL DEFAULT 0,
    slow_count INTEGER NOT NULL DEFAULT 0,
    dead_count INTEGER NOT NULL DEFAULT 0,
    rage_count INTEGER NOT NULL DEFAULT 0,
    multi_count INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY(project_id,replay_id)
);
CREATE INDEX replays_started ON replays(project_id,started_at_ms DESC,replay_id);
CREATE INDEX replays_expiration ON replays(expires_at_us);
CREATE TABLE replay_segments (
    project_id INTEGER NOT NULL,
    replay_id TEXT NOT NULL,
    segment_id INTEGER NOT NULL,
    blob_sha256 TEXT NOT NULL CHECK(length(blob_sha256)=64),
    blob_size INTEGER NOT NULL,
    metadata_sha256 TEXT NOT NULL CHECK(length(metadata_sha256)=64),
    PRIMARY KEY(project_id,replay_id,segment_id),
    FOREIGN KEY(project_id,replay_id) REFERENCES replays(project_id,replay_id) ON DELETE CASCADE
);
CREATE TABLE feedback (
    project_id INTEGER NOT NULL REFERENCES projects(id),
    event_id TEXT NOT NULL CHECK(length(event_id)=32),
    replay_id TEXT,
    timestamp_ms INTEGER NOT NULL,
    expires_at_us INTEGER NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY(project_id,event_id)
);
CREATE INDEX feedback_replay ON feedback(project_id,replay_id,timestamp_ms);
