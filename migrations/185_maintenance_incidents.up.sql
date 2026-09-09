-- Durable sanitized outbox for user-facing terminal routing incidents. Source
-- correlation fields stay private to the storage adapter and are never part of
-- the serialized incident contract. There is intentionally no foreign key to
-- llm_routing_events because its retention worker must not erase this outbox.
CREATE TABLE maintenance_incidents (
    id                  BIGSERIAL PRIMARY KEY,
    source_event_id     BIGINT NOT NULL UNIQUE,
    source_workflow_key TEXT NOT NULL,
    source_job_id       BIGINT,
    source_chat_id      BIGINT,
    source_message_id   INTEGER,
    source_created_at   TIMESTAMPTZ NOT NULL,
    signature           TEXT NOT NULL,
    first_seen          TIMESTAMPTZ NOT NULL,
    last_seen           TIMESTAMPTZ NOT NULL,
    snapshot            JSONB NOT NULL,
    captured_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    CONSTRAINT maintenance_incidents_signature_check
        CHECK (signature ~ '^[0-9a-f]{64}$'),
    CONSTRAINT maintenance_incidents_seen_order_check
        CHECK (first_seen <= last_seen),
    CONSTRAINT maintenance_incidents_snapshot_object_check
        CHECK (jsonb_typeof(snapshot) = 'object'),
    CONSTRAINT maintenance_incidents_snapshot_keys_check
        CHECK ((snapshot - ARRAY['event_type', 'workflow', 'route', 'queue', 'reason']) = '{}'::jsonb),
    CONSTRAINT maintenance_incidents_snapshot_required_check
        CHECK (snapshot ? 'event_type' AND snapshot ? 'workflow' AND snapshot ? 'route'),
    CONSTRAINT maintenance_incidents_route_shape_check
        CHECK (
            jsonb_typeof(snapshot->'route') = 'object'
            AND ((snapshot->'route') - ARRAY['provider', 'model']) = '{}'::jsonb
        )
);

CREATE FUNCTION reject_maintenance_incident_mutation()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    RAISE EXCEPTION 'maintenance incident outbox rows are immutable';
END;
$$;

CREATE TRIGGER maintenance_incidents_immutable
BEFORE UPDATE OR DELETE ON maintenance_incidents
FOR EACH ROW EXECUTE FUNCTION reject_maintenance_incident_mutation();
