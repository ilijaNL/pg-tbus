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
