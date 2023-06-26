ALTER TABLE {{schema}}.events ADD COLUMN expire_at date not null default now() + interval '30 days';

CREATE INDEX idx_events_expire_at ON {{schema}}."events" (expire_at);

CREATE OR REPLACE FUNCTION {{schema}}.create_bus_events(events jsonb)
	RETURNS SETOF {{schema}}.events
  AS $$
BEGIN
  INSERT INTO {{schema}}.events (
    event_name,
    event_data,
    expire_at
  ) 
  SELECT
    e_n,
    d as event_data,
    (now()::date + COALESCE("rid", 30) * interval '1 day') as expire_at
  FROM jsonb_to_recordset(events) as x(
    e_n text,
    d jsonb,
    rid int
  );
  RETURN;
END;
$$ LANGUAGE 'plpgsql' VOLATILE;