-- Additive migration. For an existing Compose database, run:
-- docker compose exec -T postgres psql -v ON_ERROR_STOP=1 -U bluesky -d bluesky \
--   < postgres/init/002_evidence.sql
-- Init-directory scripts run automatically only when the data volume is empty.

BEGIN;

CREATE OR REPLACE FUNCTION reject_raw_message_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'raw_messages originals are immutable';
END;
$$;

DROP TRIGGER IF EXISTS raw_messages_immutable ON raw_messages;
CREATE TRIGGER raw_messages_immutable
BEFORE UPDATE OR DELETE ON raw_messages
FOR EACH ROW EXECUTE FUNCTION reject_raw_message_mutation();

CREATE OR REPLACE FUNCTION preserve_decision_evidence()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    IF OLD.payload ? 'evidence_snapshot' AND (
        (NEW.payload->'evidence_snapshot') IS DISTINCT FROM (OLD.payload->'evidence_snapshot')
        OR (NEW.payload->'related_annotations') IS DISTINCT FROM (OLD.payload->'related_annotations')
    ) THEN
        RAISE EXCEPTION 'published decision evidence and annotation references are immutable';
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS decision_evidence_immutable ON decision_packets;
CREATE TRIGGER decision_evidence_immutable
BEFORE UPDATE ON decision_packets
FOR EACH ROW EXECUTE FUNCTION preserve_decision_evidence();

COMMIT;
