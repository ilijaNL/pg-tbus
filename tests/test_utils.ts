import { Pool } from 'pg';

export async function cleanupSchema(pool: Pool, schema: string) {
  await pool.query(`DROP SCHEMA ${schema} CASCADE`);
}

export function createRandomSchema() {
  const schema = 'schema_' + (Math.random() + 1).toString(36).substring(5);
  return schema;
}
