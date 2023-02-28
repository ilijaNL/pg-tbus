import { readFile, readdir } from 'fs/promises';
import * as path from 'path';
import crypto from 'crypto';
import { createSql, query, schemaRE, withTransaction } from './sql';
import { Pool } from 'pg';

const isValidFile = (fileName: string) => /\.(sql)$/gi.test(fileName);

const getFileName = (filePath: string) => path.basename(filePath);

const hashString = (s: string) => crypto.createHash('sha1').update(s, 'utf8').digest('hex');

const parseId = (id: string) => {
  const parsed = parseInt(id, 10);
  if (isNaN(parsed)) {
    throw new Error(`Migration file name should begin with an integer ID.'`);
  }

  return parsed;
};

export interface FileInfo {
  id: number;
  name: string;
}

const parseFileName = (fileName: string): FileInfo => {
  const result = /^(-?\d+)[-_]?(.*).(sql)$/gi.exec(fileName);

  if (!result) {
    throw new Error(`Invalid file name: '${fileName}'.`);
  }

  const [, id, name, _type] = result;

  return {
    id: parseId(id!),
    name: name == null || name === '' ? fileName : name,
  };
};

type Migration = {
  id: number;
  name: string;
  contents: string;
  fileName: string;
  hash: string;
  sql: string;
};

const loadMigrationFile = async (filePath: string, schema: string) => {
  const fileName = getFileName(filePath);

  try {
    const { id, name } = parseFileName(fileName);
    const contents = await readFile(filePath, { encoding: 'utf8' });

    const sql = contents.replace(schemaRE, schema);
    const hash = hashString(fileName + sql);

    return {
      id,
      name,
      contents,
      fileName,
      hash,
      sql,
    };
  } catch (err: any) {
    throw new Error(`${err.message} - Offending file: '${fileName}'.`);
  }
};

const loadMigrationFiles = async (directory: string, schema: string) => {
  const fileNames = await readdir(directory);

  if (fileNames == null) {
    return [];
  }

  const migrationFiles = fileNames.map((fileName) => path.resolve(directory, fileName)).filter(isValidFile);

  const unorderedMigrations = await Promise.all(migrationFiles.map((path) => loadMigrationFile(path, schema)));

  // Arrange in ID order
  const orderedMigrations = unorderedMigrations.sort((a, b) => a.id - b.id);

  return orderedMigrations;
};

function filterMigrations(migrations: Array<Migration>, appliedMigrations: Set<number>) {
  const notAppliedMigration = (migration: Migration) => !appliedMigrations.has(migration.id);

  return migrations.filter(notAppliedMigration);
}

function validateMigrationHashes(
  migrations: Array<Migration>,
  appliedMigrations: Array<{
    id: number;
    name: string;
    hash: string;
  }>
) {
  const invalidHash = (migration: Migration) => {
    const appliedMigration = appliedMigrations.find((m) => m.id === migration.id);
    return !!appliedMigration && appliedMigration.hash !== migration.hash;
  };

  // Assert migration hashes are still same
  const invalidHashes = migrations.filter(invalidHash);
  if (invalidHashes.length > 0) {
    // Someone has altered one or more migrations which has already run - gasp!
    const invalidFiles = invalidHashes.map(({ fileName }) => fileName);
    throw new Error(`Hashes don't match for migrations '${invalidFiles}'.
This means that the scripts have changed since it was applied.`);
  }
}

export const createMigrationPlans = (schema: string) => {
  const sql = createSql(schema);

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

  function tableExists(table: string) {
    return sql<{ exists: boolean }>`
      SELECT EXISTS (
        SELECT FROM information_schema.tables 
        WHERE  table_schema = '{{schema}}'
        AND    table_name   = ${table}
      );  
    `;
  }

  return {
    tableExists,
    getMigrations,
    insertMigration,
  };
};

export async function migrate(pool: Pool, schema: string, directory: string) {
  const allMigrations = await loadMigrationFiles(directory, schema);
  let toApply = [...allMigrations];
  // check if table exists
  const plans = createMigrationPlans(schema);

  await withTransaction(pool, async (client) => {
    // acquire lock
    await client.query(`
      SELECT pg_advisory_xact_lock( ('x' || md5(current_database() || '.tbus.${schema}'))::bit(64)::bigint )
    `);

    const rows = await query(client, plans.tableExists('tbus_migrations'));
    const migTableExists = rows[0]?.exists;

    // fetch latest migration
    if (migTableExists) {
      const appliedMigrations = await query(client, plans.getMigrations());
      validateMigrationHashes(allMigrations, appliedMigrations);
      toApply = filterMigrations(allMigrations, new Set(appliedMigrations.map((m) => m.id)));
    }

    for (const migration of toApply) {
      await client.query(migration.sql);
      await query(client, plans.insertMigration(migration));
    }
  });
}
