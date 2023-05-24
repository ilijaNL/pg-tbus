CREATE OR REPLACE FUNCTION {{schema}}.create_bus_tasks(tasks jsonb)
	RETURNS SETOF {{schema}}.tasks
  AS $$
BEGIN
  INSERT INTO {{schema}}.tasks (
    id,
    queue,
    data,
    retryLimit,
    retryDelay,
    retryBackoff,
    singleton_key,
    startAfter,
    expireIn,
    keepUntil
  )
  SELECT
    gen_random_uuid() as id,
    "queue",
    "data",
    "retryLimit",
    "retryDelay",
    "retryBackoff",
    "singletonKey" as singleton_key,
    (now() + ("startAfterSeconds" * interval '1s'))::timestamptz as startAfter,
    "expireInSeconds" * interval '1s' as expireIn,
    (now() + ("startAfterSeconds" * interval '1s') + ("keepInSeconds" * interval '1s'))::timestamptz as keepUntil
  FROM jsonb_to_recordset(tasks) as x(
    "queue" text,
    "data" jsonb,
    "retryLimit" integer,
    "retryDelay" integer,
    "retryBackoff" boolean,
    "startAfterSeconds" integer,
    "expireInSeconds" integer,
    "keepInSeconds" integer,
    "singletonKey" text
  )
  ON CONFLICT DO NOTHING;
  RETURN;
END;
$$ LANGUAGE 'plpgsql' VOLATILE;

CREATE OR REPLACE FUNCTION {{schema}}.create_bus_events(events jsonb)
	RETURNS SETOF {{schema}}.events
  AS $$
BEGIN
  INSERT INTO {{schema}}.events (
    event_name,
    event_data
  ) 
  SELECT
    event_name,
    data as event_data
  FROM jsonb_to_recordset(events) as x(
    event_name text,
    data jsonb
  );
  RETURN;
END;
$$ LANGUAGE 'plpgsql' VOLATILE;