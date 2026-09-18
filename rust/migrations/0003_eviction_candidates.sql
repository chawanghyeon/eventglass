CREATE INDEX shards_eviction_candidates
ON shards(coalesce(last_accessed_at_us,sealed_at_us,created_at_us),id)
WHERE state='remote_verified' AND remote_archive_key IS NOT NULL
  AND archive_sha256 IS NOT NULL AND recovery_checkpoint_id IS NOT NULL;
