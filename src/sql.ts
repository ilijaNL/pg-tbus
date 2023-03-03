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

export type QueryCommand<Result> = {
  text: string;
  values: unknown[];
  frags: ReadonlyArray<string>;
  // used to keep the type definition and is always undefined
  __result?: Result;
};

/**
 * Helper function to convert string literal to parameterized postgres query
 */
export function createSql(schema: string) {
  return function sql<Result extends QueryResultRow>(
    sqlFragments: ReadonlyArray<string>,
    ...parameters: unknown[]
  ): QueryCommand<Result> {
    const text: string = sqlFragments.reduce((prev, curr, i) => prev + '$' + i + curr);
    const result = {
      frags: sqlFragments,
      text: text.replace(schemaRE, schema),
      values: parameters,
    };

    return result;
  };
}

export function combineSQL(f: ReadonlyArray<string>, ...parameters: QueryCommand<any>[]) {
  const sqlFragments = [f[0] ?? ''];

  for (let i = 0; i < f.length - 1; ++i) {
    sqlFragments[sqlFragments.length - 1] += parameters[i]?.frags[0] ?? '';
    sqlFragments.push(...(parameters[i]?.frags ?? []).slice(1));
    sqlFragments[sqlFragments.length - 1] += f[i + 1] ?? '';
  }

  const values = [...parameters.flatMap((c) => c.values)];
  return {
    sqlFragments: sqlFragments,
    parameters: values,
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
      ...opts,
    })
    .then((d) => d.rows);
}

export async function withTransaction<T>(pool: Pool, handler: (client: PoolClient) => Promise<T>) {
  const client = await pool.connect();
  let result: T;
  try {
    await client.query('BEGIN');
    result = await handler(client);
    await client.query('COMMIT');
  } catch (e) {
    await client.query('ROLLBACK');
    throw e;
  } finally {
    client.release();
  }

  return result;
}
