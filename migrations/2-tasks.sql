CREATE TABLE {{schema}}.tasks (
  id uuid primary key not null default gen_random_uuid(),
  queue text not null,
  data jsonb,
  state smallint not null default(0),
  retryLimit integer not null default(0),
  retryCount integer not null default(0),
  retryDelay integer not null default(0),
  retryBackoff boolean not null default false,
  startAfter timestamp with time zone not null default now(),
  startedOn timestamp with time zone,
  expireIn interval not null default interval '2 minutes',
  createdOn timestamp with time zone not null default now(),
  completedOn timestamp with time zone,
  keepUntil timestamp with time zone NOT NULL default now() + interval '14 days',
  output jsonb
) -- https://www.cybertec-postgresql.com/en/what-is-fillfactor-and-how-does-it-affect-postgresql-performance/
WITH (fillfactor=80);

-- 0: create, 1: retry, 2: active, 3 >= all completed/failed
CREATE INDEX idx_get_tasks ON {{schema}}."tasks" ("queue", startAfter) WHERE state < 2;
CREATE INDEX idx_expire_tasks ON {{schema}}."tasks" ("state") WHERE state = 2;
CREATE INDEX idx_purge_tasks ON {{schema}}."tasks" (keepUntil) WHERE state >= 3;