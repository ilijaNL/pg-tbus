import { Type } from '@sinclair/typebox';
import { once } from 'events';
import path from 'path';
import { Pool } from 'pg';
import { EventEmitter } from 'stream';
import tap from 'tap';
import { defineTask, query } from '../src';
import { createMessagePlans, createTaskFactory, TASK_STATES } from '../src/messages';
import { migrate } from '../src/migrations';
import { createBaseWorker } from '../src/workers/base';
import { createMaintainceWorker } from '../src/workers/maintaince';
import { createTaskWorker } from '../src/workers/task';
import { cleanupSchema } from './helpers';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

tap.test('baseworker', async (tap) => {
  tap.test('loops', async (t) => {
    t.plan(1);
    let called = 0;
    const worker = createBaseWorker(
      async () => {
        called += 1;
        return false;
      },
      { loopInterval: 100 }
    );

    worker.start();

    await new Promise((resole) => setTimeout(resole, 290));

    // 3 because start calls immedialtly
    t.equal(called, 3);
    await worker.stop();
  });

  tap.test('long run', async (t) => {
    t.plan(2);
    const worker = createBaseWorker(
      async () => {
        // longer than interval
        await new Promise((resolve) => setTimeout(resolve, 200));
        t.pass('loop');
      },
      { loopInterval: 100 }
    );

    worker.start();
    await new Promise((resolve) => setTimeout(resolve, 240));
    await worker.stop();
  });

  tap.test('continues', async (t) => {
    t.plan(2);
    const ee = new EventEmitter();
    const worker = createBaseWorker(
      async () => {
        ee.emit('loop');
        t.pass('loop');
        return true;
      },
      { loopInterval: 10000000 }
    );

    worker.start();
    await once(ee, 'loop');
    await worker.stop();
  });

  tap.test('notifies', async (t) => {
    t.plan(3);
    const ee = new EventEmitter();
    const worker = createBaseWorker(
      async () => {
        ee.emit('loop');
        t.pass('loop');
        return true;
      },
      { loopInterval: 10000000 }
    );

    worker.start();
    await once(ee, 'loop');
    worker.notify();
    await once(ee, 'loop');
    await worker.stop();
  });

  tap.test('multiple starts', async (t) => {
    t.plan(1);
    let called = 0;
    const worker = createBaseWorker(
      async () => {
        called += 1;
        return false;
      },
      { loopInterval: 100 }
    );

    worker.start();
    worker.start();
    worker.start();

    await new Promise((resolve) => setTimeout(resolve, 230));

    t.equal(called, 3);
    await worker.stop();
  });
});

tap.test('task worker', async (t) => {
  t.jobs = 5;

  const schema = 'taskworker';

  const sqlPool = new Pool({
    connectionString: connectionString,
  });

  await migrate(sqlPool, schema, path.join(process.cwd(), './migrations'));

  t.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  t.test('happy path', async (t) => {
    const queue = 'happy';
    const taskFactory = createTaskFactory({
      queue: queue,
      taskConfig: {},
    });

    const worker = createTaskWorker({
      client: sqlPool,
      async handler(event) {
        return {
          works: event.data.tn,
        };
      },
      maxConcurrency: 10,
      poolInternvalInMs: 100,
      queue: queue,
      schema: schema,
    });

    worker.start();
    t.teardown(() => worker.stop());

    // start stop
    const plans = createMessagePlans(schema);
    const simpleTask = defineTask({
      schema: Type.Object({}),
      task_name: 'happy-task',
      config: {
        expireInSeconds: 10,
        keepInSeconds: 10,
        retryBackoff: false,
        retryLimit: 1,
        retryDelay: 1,
      },
      handler: async () => {},
    });
    const task = simpleTask.from({});

    const insertTask = taskFactory(task, { type: 'direct' });
    await query(sqlPool, plans.createTasks([insertTask]));
    await new Promise((resolve) => setTimeout(resolve, 1000));

    const result = await sqlPool
      .query(
        `SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${simpleTask.task_name}' LIMIT 1`
      )
      .then((r) => r.rows[0]);

    t.equal(result.output.works, 'happy-task');
    t.equal(result.state, TASK_STATES.completed);
  });

  t.test('retries', async (t) => {
    const queue = 'retries';
    let called = 0;
    const taskFactory = createTaskFactory({
      queue: queue,
      taskConfig: {},
    });

    const worker = createTaskWorker({
      client: sqlPool,
      async handler(event) {
        called += 1;
        const item = {} as any;
        // throw with stack trace
        item.balbala.run();
      },
      maxConcurrency: 10,
      poolInternvalInMs: 200,
      queue: queue,
      schema: schema,
    });

    worker.start();
    t.teardown(() => worker.stop());

    // start stop
    const plans = createMessagePlans(schema);
    const simpleTask = defineTask({
      schema: Type.Object({}),
      task_name: 'task',
      config: {
        expireInSeconds: 10,
        keepInSeconds: 10,
        retryBackoff: false,
        retryLimit: 1,
        retryDelay: 1,
      },
      handler: async () => {},
    });
    const task = simpleTask.from({});

    const insertTask = taskFactory(task, { type: 'direct' });

    t.equal(insertTask.retryLimit, 1);

    await query(sqlPool, plans.createTasks([insertTask]));
    await new Promise((resolve) => setTimeout(resolve, 3000));

    t.equal(called, 2);

    const result = await sqlPool
      .query(
        `SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${simpleTask.task_name}' LIMIT 1`
      )
      .then((r) => r.rows[0]);

    t.equal(result.state, TASK_STATES.failed);
    t.equal(result.output.message, "Cannot read properties of undefined (reading 'run')");
  });

  t.test('expires', async (t) => {
    const queue = 'expires';
    let called = 0;
    const taskFactory = createTaskFactory({
      queue: queue,
      taskConfig: {},
    });

    const worker = createTaskWorker({
      client: sqlPool,
      async handler(event) {
        called += 1;
        await new Promise((resolve) => setTimeout(resolve, 2000));
      },
      maxConcurrency: 10,
      poolInternvalInMs: 100,
      queue: queue,
      schema: schema,
    });

    worker.start();
    t.teardown(() => worker.stop());

    // start stop
    const plans = createMessagePlans(schema);
    const insertTask = taskFactory(
      {
        config: {
          expireInSeconds: 1,
          keepInSeconds: 10,
          retryBackoff: false,
          retryLimit: 1,
          retryDelay: 1,
        },
        task_name: 'expired-task',
        data: {},
      },
      { type: 'direct' }
    );

    await query(sqlPool, plans.createTasks([insertTask]));
    await new Promise((resolve) => setTimeout(resolve, 5000));

    t.equal(called, 2);

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = 'expired-task' LIMIT 1`)
      .then((r) => r.rows[0]);

    t.equal(result.state, TASK_STATES.failed);
    t.pass(result.output.message.includes('handler execution exceeded'));
  });
});

tap.test('maintaince worker', async (t) => {
  t.jobs = 5;

  const schema = 'maintaince';
  const plans = createMessagePlans(schema);

  const sqlPool = new Pool({
    connectionString: connectionString,
  });

  await migrate(sqlPool, schema, path.join(process.cwd(), './migrations'));

  t.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  const queue = 'maintaince';

  const taskFactory = createTaskFactory({
    queue: queue,
    taskConfig: {},
  });

  t.test('expires', async (t) => {
    const worker = createMaintainceWorker({
      client: sqlPool,
      retentionInDays: 30,
      schema,
      intervalInMs: 200,
    });

    worker.start();
    t.teardown(() => worker.stop());

    const insertTask = taskFactory(
      {
        config: {
          expireInSeconds: 1,
          keepInSeconds: 120,
          retryBackoff: false,
          retryLimit: 0,
          retryDelay: 1,
        },
        task_name: 'expired-task',
        data: {},
      },
      { type: 'direct' }
    );

    await query(sqlPool, plans.createTasks([insertTask]));
    // mark the task as started
    await query(sqlPool, plans.getTasks({ queue, amount: 100 }));

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = 'expired-task' LIMIT 1`)
      .then((r) => r.rows[0]);

    t.equal(result.state, TASK_STATES.expired);
  });

  t.test('purges tasks', async (t) => {
    const worker = createMaintainceWorker({
      client: sqlPool,
      retentionInDays: 30,
      schema,
      intervalInMs: 200,
    });

    const plans = createMessagePlans(schema);
    const insertTask = taskFactory(
      {
        config: {
          expireInSeconds: 1,
          keepInSeconds: 0,
          retryBackoff: false,
          retryLimit: 0,
          retryDelay: 1,
        },
        task_name: 'expired-task-worker',
        data: {},
      },
      { type: 'direct' }
    );

    t.equal(insertTask.keepInSeconds, 0);
    t.equal(insertTask.startAfterSeconds, 0);

    await query(sqlPool, plans.createTasks([insertTask]));
    // mark the task as started
    await query(sqlPool, plans.getTasks({ queue, amount: 100 }));
    // complete
    const result = await sqlPool
      .query(
        `UPDATE ${schema}.tasks SET state = ${TASK_STATES.completed} WHERE queue = '${queue}' AND data->>'tn' = 'expired-task-worker' RETURNING *`
      )
      .then((r) => r.rows[0]);

    t.ok(result.id);

    worker.start();
    t.teardown(() => worker.stop());

    await new Promise((resolve) => setTimeout(resolve, 1000));

    const r = await sqlPool.query(`SELECT * FROM ${schema}.tasks WHERE id = '${result.id}'::uuid`);
    t.equal(r.rowCount, 0);
  });
});
