-- +eventglass Up
CREATE TABLE installations (
    singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
    installation_id UUID NOT NULL,
    storage_generation BIGINT NOT NULL DEFAULT 1 CHECK (storage_generation > 0),
    schema_version INTEGER NOT NULL,
    topology_version INTEGER NOT NULL DEFAULT 1,
    lane_count INTEGER NOT NULL DEFAULT 16 CHECK (lane_count = 16),
    storage_identity TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
);

-- +eventglass Down
DROP TABLE installations;
