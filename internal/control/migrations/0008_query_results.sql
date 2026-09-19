-- +eventglass Up
ALTER TABLE query_jobs
    ADD COLUMN plan_input_bytes BIGINT NOT NULL DEFAULT 0 CHECK (plan_input_bytes >= 0);

-- +eventglass Down
ALTER TABLE query_jobs DROP COLUMN plan_input_bytes;
