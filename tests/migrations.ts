import { Pool } from 'pg';
import { test } from 'tap';
import { copy, remove, appendFile, readFile, writeFile } from 'fs-extra';
import createTBus, { createPlans, query } from '../src';
import { cleanupSchema, createRandomSchema } from './utils';
import { migrate } from '../src/migrations';
import path from 'path';

const connectionString = 'postgres://postgres:postgres@localhost:5432/app';

async function createMigrationDir(schema: string) {
  const migrationPath = path.join(__dirname, '.' + schema);
  await copy(path.join(__dirname, '..', 'migrations'), migrationPath, { overwrite: true });
  return migrationPath;
}

test('happy path', async ({ teardown, equal }) => {
  const schema = createRandomSchema();
  const pool = new Pool({
    connectionString: connectionString,
    max: 3,
  });

  const plans = createPlans(schema);
  const bus = createTBus('test', { db: pool, schema: schema });
  await bus.start();
  const hasMigrations = await query(pool, plans.tableExists('tbus_migrations'));

  equal(hasMigrations[0]!.exists, true);

  teardown(async () => {
    await bus.stop();
    await cleanupSchema(pool, schema);
    await pool.end();
  });
});

test('concurrently startup', async ({ teardown }) => {
  const schema = createRandomSchema();
  const bus = createTBus('test', { db: { connectionString }, schema: schema });
  const bus2 = createTBus('test', { db: { connectionString }, schema: schema });

  await Promise.all([bus.start(), bus2.start()]);

  const pool = new Pool({
    connectionString: connectionString,
    max: 1,
  });

  teardown(async () => {
    await Promise.all([bus.stop(), bus2.stop()]);
    await cleanupSchema(pool, schema);
    await pool.end();
  });
});

test('applies new migration', async ({ teardown, equal }) => {
  const schema = createRandomSchema();

  const pool = new Pool({
    connectionString: connectionString,
    max: 2,
  });

  const migrationPath = await createMigrationDir(schema);
  await migrate(pool, schema, migrationPath);

  teardown(async () => {
    await cleanupSchema(pool, schema);
    await remove(migrationPath);
    await pool.end();
  });

  const allMigrations = await query(pool, createPlans(schema).getMigrations());

  const lastId = allMigrations[allMigrations.length - 1]!.id;

  // write new file
  const newFileContent = `SELECT * FROM ${schema}.tbus_migrations`;
  const migrationName = `${lastId + 1}-test.sql`;
  const migrationFile = path.join(migrationPath, migrationName);
  await writeFile(migrationFile, newFileContent, { encoding: 'utf8' });

  await migrate(pool, schema, migrationPath);

  const newMigrations = await query(pool, createPlans(schema).getMigrations());

  equal(allMigrations.length + 1, newMigrations.length);

  const lastAppliedMig = newMigrations[newMigrations.length - 1]!;

  equal(lastAppliedMig.name, lastAppliedMig.name);
  equal(lastAppliedMig.id, lastId + 1);
});

test('throws when not valid name', async ({ teardown, rejects }) => {
  const schema = createRandomSchema();

  const pool = new Pool({
    connectionString: connectionString,
    max: 2,
  });

  const migrationPath = await createMigrationDir(schema);
  await migrate(pool, schema, migrationPath);

  teardown(async () => {
    await cleanupSchema(pool, schema);
    await remove(migrationPath);
    await pool.end();
  });

  // write new file
  const newFileContent = `SELECT * FROM ${schema}.tbus_migrations`;
  const migrationName = `awdawd-test.sql`;
  const migrationFile = path.join(migrationPath, migrationName);
  await writeFile(migrationFile, newFileContent, { encoding: 'utf8' });

  rejects(migrate(pool, schema, migrationPath));
});

test('throws when migration is changed', async ({ teardown, equal, rejects }) => {
  const schema = createRandomSchema();

  const pool = new Pool({
    connectionString: connectionString,
    max: 2,
  });

  const migrationPath = await createMigrationDir(schema);

  await migrate(pool, schema, migrationPath);

  const migrationFile = path.join(migrationPath, '0-create-migration-table.sql');
  // change migration file
  await appendFile(migrationFile, `\n--- schema: ${schema}`);

  const mig = await readFile(migrationFile, { encoding: 'utf8' });

  equal(mig.includes(`schema: ${schema}`), true);

  rejects(migrate(pool, schema, migrationPath));

  teardown(async () => {
    await cleanupSchema(pool, schema);
    await remove(migrationPath);
    await pool.end();
  });
});
