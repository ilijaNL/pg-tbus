CREATE TABLE {{schema}}."cursors" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "svc" text not null,
  "l_p" bigint not null default 0,
  "created_at" timestamptz NOT NULL DEFAULT now(), 
  PRIMARY KEY ("id"),
  UNIQUE ("svc")
);

CREATE TABLE {{schema}}."events" (
  "id" uuid NOT NULL DEFAULT gen_random_uuid(),
  "event_name" text NOT NULL,
  "event_data" jsonb NOT NULL,
  "pos" bigint not null default 0,
  "created_at" timestamptz NOT NULL DEFAULT now(), 
  PRIMARY KEY ("id") 
);

CREATE INDEX idx_events_pos ON {{schema}}."events" (pos) WHERE pos > 0;

CREATE SEQUENCE {{schema}}.event_order as bigint start 1;

CREATE FUNCTION {{schema}}.proc_set_position()
    RETURNS TRIGGER
    LANGUAGE plpgsql
AS
$$
BEGIN
    PERFORM pg_advisory_xact_lock(1723683380);
    update {{schema}}."events" set pos = NEXTVAL('{{schema}}.event_order') where id = new.id;
    RETURN NULL;
END;
$$;

CREATE CONSTRAINT TRIGGER set_commit_order
    AFTER INSERT ON {{schema}}."events"
    DEFERRABLE INITIALLY DEFERRED
    FOR EACH ROW
EXECUTE PROCEDURE {{schema}}.proc_set_position();
