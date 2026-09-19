-- +eventglass Up
ALTER TABLE query_tasks
    ADD CONSTRAINT query_tasks_stage_level_check CHECK (
        (stage='scan' AND level=0) OR (stage='reduce' AND level>0)
    ),
    ADD CONSTRAINT query_tasks_result_identity_check CHECK (
        (result_intent_id IS NULL) = (result_sha256 IS NULL)
        AND (result_intent_id IS NULL) = (result_rows IS NULL)
        AND (result_intent_id IS NULL) = (result_bytes IS NULL)
    );

ALTER TABLE query_task_inputs
    ADD CONSTRAINT query_task_inputs_stage_check CHECK (
        consumer_stage='reduce' AND consumer_level>0
        AND ((producer_stage='scan' AND producer_level=0)
            OR (producer_stage='reduce' AND producer_level>0))
    );
CREATE INDEX query_task_inputs_producer_idx
    ON query_task_inputs(tenant_id,query_id,producer_stage,producer_level,producer_partition_id);

ALTER TABLE query_jobs
    ADD CONSTRAINT query_jobs_result_identity_check CHECK (
        (result_intent_id IS NULL) = (result_sha256 IS NULL)
        AND (result_intent_id IS NULL) = (result_bytes IS NULL)
    ),
    ADD CONSTRAINT query_jobs_coordinator_state_check CHECK (
        coordinator_owner IS NULL OR state IN ('planning','queued','running')
    );

ALTER TABLE object_intents DROP CONSTRAINT object_intents_producer_job_check;
ALTER TABLE object_intents
    ADD COLUMN query_id UUID,
    ADD COLUMN query_stage TEXT,
    ADD COLUMN query_level INTEGER,
    ADD COLUMN query_partition_id INTEGER,
    ADD CONSTRAINT object_intents_query_identity_check CHECK (
        (query_id IS NULL) = (query_stage IS NULL)
        AND (query_id IS NULL) = (query_level IS NULL)
        AND (query_id IS NULL) = (query_partition_id IS NULL)
    ),
    ADD CONSTRAINT object_intents_query_values_check CHECK (
        query_id IS NULL OR (query_stage IN ('scan','reduce') AND query_level>=0 AND query_partition_id>=0)
    ),
    ADD CONSTRAINT object_intents_producer_family_check CHECK (
        (conversion_job_id IS NOT NULL)::integer + (query_id IS NOT NULL)::integer <= 1
    ),
    ADD CONSTRAINT object_intents_producer_authority_check CHECK (
        (conversion_job_id IS NOT NULL OR query_id IS NOT NULL) = (producer_generation IS NOT NULL)
    ),
    ADD CONSTRAINT object_intents_query_task_fk
        FOREIGN KEY(tenant_id,query_id,query_stage,query_level,query_partition_id)
        REFERENCES query_tasks(tenant_id,query_id,stage,level,partition_id);
CREATE INDEX object_intents_query_producer_idx
    ON object_intents(tenant_id,query_id,query_stage,query_level,query_partition_id)
    WHERE query_id IS NOT NULL;

-- +eventglass Down
DROP INDEX object_intents_query_producer_idx;
ALTER TABLE object_intents
    DROP CONSTRAINT object_intents_query_task_fk,
    DROP CONSTRAINT object_intents_producer_authority_check,
    DROP CONSTRAINT object_intents_producer_family_check,
    DROP CONSTRAINT object_intents_query_values_check,
    DROP CONSTRAINT object_intents_query_identity_check;
UPDATE object_intents SET producer_generation=NULL,producer_fence=NULL WHERE query_id IS NOT NULL;
ALTER TABLE object_intents
    DROP COLUMN query_partition_id,
    DROP COLUMN query_level,
    DROP COLUMN query_stage,
    DROP COLUMN query_id,
    ADD CONSTRAINT object_intents_producer_job_check CHECK ((conversion_job_id IS NULL) = (producer_generation IS NULL));
ALTER TABLE query_jobs
    DROP CONSTRAINT query_jobs_coordinator_state_check,
    DROP CONSTRAINT query_jobs_result_identity_check;
DROP INDEX query_task_inputs_producer_idx;
ALTER TABLE query_task_inputs DROP CONSTRAINT query_task_inputs_stage_check;
ALTER TABLE query_tasks
    DROP CONSTRAINT query_tasks_result_identity_check,
    DROP CONSTRAINT query_tasks_stage_level_check;
