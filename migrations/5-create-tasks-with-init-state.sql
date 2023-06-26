ALTER TABLE {{schema}}.tasks DROP COLUMN "id";
ALTER TABLE {{schema}}.events DROP COLUMN "id";

ALTER TABLE {{schema}}.events ADD COLUMN id BIGSERIAL PRIMARY KEY;
ALTER TABLE {{schema}}.tasks ADD COLUMN id BIGSERIAL PRIMARY KEY;

CREATE OR REPLACE FUNCTION {{schema}}.create_bus_tasks(tasks jsonb)
	RETURNS SETOF {{schema}}.tasks
  AS $$
BEGIN
  INSERT INTO {{schema}}.tasks (
    "queue",
    "data",
    "state",
    retryLimit,
    retryDelay,
    retryBackoff,
    singleton_key,
    startAfter,
    expireIn,
    keepUntil
  )
  SELECT
    "q" as "queue",
    "d" as "data",
    COALESCE("s", 0) as "state",
    "r_l" as "retryLimit",
    "r_d" as "retryDelay",
    "r_b" as "retryBackoff",
    "skey" as singleton_key,
    (now() + ("saf" * interval '1s'))::timestamptz as startAfter,
    "eis" * interval '1s' as expireIn,
    (now() + ("saf" * interval '1s') + ("kis" * interval '1s'))::timestamptz as keepUntil
  FROM jsonb_to_recordset(tasks) as x(
    "q" text,
    "d" jsonb,
    "s" smallint,
    "r_l" integer,
    "r_d" integer,
    "r_b" boolean,
    "saf" integer,
    "eis" integer,
    "kis" integer,
    "skey" text
  )
  ON CONFLICT DO NOTHING;
  RETURN;
END;
$$ LANGUAGE 'plpgsql' VOLATILE;
