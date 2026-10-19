-- The relay replaces the first model gate without rewriting historical audits.
-- Its separate stage also gives an interrupted legacy filter input a distinct
-- event identity when replayed by the relay. Consumer groups and offsets stay put.
BEGIN;
ALTER TABLE routing_events DROP CONSTRAINT IF EXISTS routing_events_stage_check;
ALTER TABLE routing_events ADD CONSTRAINT routing_events_stage_check
    CHECK (stage IN ('filter', 'fast', 'relay'));
COMMIT;
