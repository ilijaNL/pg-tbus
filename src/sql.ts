import { JobInsert } from 'pg-boss';
import fastJSON from 'fast-json-stable-stringify';
import { Event } from './definitions';
import { Pool, PoolClient } from 'pg';

export interface QueryResultRow {
  [column: string]: any;
}

export const schemaRE = new RegExp('{{schema}}', 'g');

export type PGClient = {
  query: <T = any>(props: {
    text: string;
    values: any[];
    name?: string;
  }) => Promise<{
    rows: T[];
    rowCount: number;
  }>;
};

type QueryCommand<Result> = {
  text: string;
  values: unknown[];
  // used to keep the type definition and is always undefined
  __result?: Result;
};

/**
 * Helper function to convert string literal to parameterized postgres query
 * @param sqlFragments
 * @param parameters
 * @returns
 */
export function createSql(schema: string) {
  return function sql<Result extends QueryResultRow>(
    sqlFragments: TemplateStringsArray,
    ...parameters: unknown[]
  ): QueryCommand<Result> {
    const text: string = sqlFragments.reduce((prev, curr, i) => prev + '$' + i + curr);

    return {
      text: text.replace(schemaRE, schema),
      values: parameters,
    };
  };
}

export async function query<Result extends QueryResultRow>(
  client: PGClient,
  command: QueryCommand<Result>,
  opts?: Partial<{ name: string }>
) {
  return client
    .query<Result>({
      text: command.text,
      values: command.values,
      name: opts?.name,
    })
    .then((d) => d.rows);
}

export type TaskDTO<T> = { tn: string; data: T };
export type Job<T = {}> = Omit<JobInsert<TaskDTO<T>>, 'id' | 'onComplete' | 'priority'>;

export const createPlans = (schema: string) => {
  const sql = createSql(schema);
  function createTasks(tasks: Job[]) {
    return sql<{}>`
      INSERT INTO {{schema}}.job (
        id,
        name,
        data,
        priority,
        startAfter,
        expireIn,
        retryLimit,
        retryDelay,
        retryBackoff,
        singletonKey,
        keepUntil,
        on_complete
      )
      SELECT
        COALESCE(id, gen_random_uuid()) as id,
        name,
        data,
        COALESCE(priority, 0) as priority,
        COALESCE("startAfter", now()) as startAfter,
        COALESCE("expireInSeconds", 15 * 60) * interval '1s' as expireIn,
        COALESCE("retryLimit", 0) as retryLimit,
        COALESCE("retryDelay", 0) as retryDelay,
        COALESCE("retryBackoff", false) as retryBackoff,
        "singletonKey",
        COALESCE("keepUntil", now() + interval '14 days') as keepUntil,
        COALESCE("onComplete", false) as onComplete
      FROM json_to_recordset(${fastJSON(tasks)}) as x(
        id uuid,
        name text,
        priority integer,
        data jsonb,
        "retryLimit" integer,
        "retryDelay" integer,
        "retryBackoff" boolean,
        "startAfter" timestamp with time zone,
        "singletonKey" text,
        "expireInSeconds" integer,
        "keepUntil" timestamp with time zone,
        "onComplete" boolean
      )
      ON CONFLICT DO NOTHING
    `;
  }

  function getEvents(service_name: string, options = { limit: 100 }) {
    const events = sql<{ id: string; event_name: string; event_data: any; position: number }>`
    WITH cursor AS (
      SELECT 
        l_p 
      FROM {{schema}}.cursors 
      WHERE svc = ${service_name}
      LIMIT 1
      FOR UPDATE
    ) SELECT 
        id, 
        event_name, 
        event_data,
        pos as position
      FROM {{schema}}.events, cursor
      WHERE pos > cursor.l_p
      ORDER BY pos ASC
      LIMIT ${options.limit}`;

    return events;
  }

  function updateCursor(service_name: string, last_position: number) {
    return sql`
      UPDATE {{schema}}.cursors 
      SET l_p = ${last_position} 
      WHERE svc = ${service_name}`;
  }

  function ensureServicePointer(service_name: string) {
    return sql`
      INSERT INTO {{schema}}.cursors (svc, l_p) 
      VALUES (${service_name}, 0) 
      ON CONFLICT DO NOTHING`;
  }

  function tableExists(table: string) {
    return sql<{ exists: boolean }>`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE  table_schema = '{{schema}}'
        AND    table_name   = ${table}
      );  
    `;
  }

  function createEvents(events: Event[]) {
    return sql`
      INSERT INTO {{schema}}.events (
        event_name,
        event_data
      ) 
      SELECT
        event_name,
        data as event_data
      FROM json_to_recordset(${fastJSON(events)}) as x(
        event_name text,
        data jsonb
      )
    `;
  }

  function getMigrations() {
    return sql<{ id: number; name: string; hash: string }>`
      SELECT * FROM {{schema}}.tbus_migrations ORDER BY id
    `;
  }

  function insertMigration(migration: { id: number; hash: string; name: string }) {
    return sql`
      INSERT INTO 
        {{schema}}.tbus_migrations (id, name, hash) 
      VALUES (${migration.id}, ${migration.name}, ${migration.hash})
    `;
  }

  return {
    createEvents,
    ensureServicePointer,
    updateCursor,
    getEvents,
    tableExists,
    createTasks,
    getMigrations,
    insertMigration,
  };
};

export async function withTransaction(pool: Pool, handler: (client: PoolClient) => Promise<void>) {
  const client = await pool.connect();
  try {
    await client.query('BEGIN');
    await handler(client);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }
}
