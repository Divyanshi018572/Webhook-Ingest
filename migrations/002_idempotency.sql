-- 002_idempotency.sql
-- Enforce database-level uniqueness on event_id to support atomic ON CONFLICT deduplication.
ALTER TABLE events ADD CONSTRAINT uq_events_event_id UNIQUE (event_id);
