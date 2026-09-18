-- Preserve the latest actual post-resolve regression for triage. A manual
-- status edit does not fabricate or erase this event history.
ALTER TABLE issues ADD COLUMN last_regressed_at_us INTEGER;
