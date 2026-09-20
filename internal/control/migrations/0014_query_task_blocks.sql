-- +eventglass Up
CREATE TABLE query_task_blocks (
    tenant_id BIGINT NOT NULL,
    query_id UUID NOT NULL,
    stage TEXT NOT NULL,
    level INTEGER NOT NULL,
    partition_id INTEGER NOT NULL,
    block_index INTEGER NOT NULL CHECK (block_index >= 0),
    sha256 CHAR(64) NOT NULL CHECK (sha256 ~ '^[0-9a-f]{64}$'),
    PRIMARY KEY (query_id,stage,level,partition_id,block_index),
    FOREIGN KEY (tenant_id,query_id,stage,level,partition_id)
        REFERENCES query_tasks(tenant_id,query_id,stage,level,partition_id) ON DELETE CASCADE
);
ALTER TABLE query_jobs ADD COLUMN cache_bytes BIGINT NOT NULL DEFAULT 0 CHECK (cache_bytes >= 0);

-- +eventglass Down
ALTER TABLE query_jobs DROP COLUMN cache_bytes;
DROP TABLE query_task_blocks;
