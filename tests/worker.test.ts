import { Type } from '@sinclair/typebox';
import { once } from 'events';
import path from 'path';
import { Pool } from 'pg';
import { EventEmitter } from 'stream';
import tap from 'tap';
import { PGClient, createTaskHandler, defineTask, query } from '../src';
import { createMessagePlans, createTaskFactory, SelectTask, TASK_STATES } from '../src/messages';
import { migrate } from '../src/migrations';
import { createBaseWorker } from '../src/workers/base';
import { createMaintainceWorker } from '../src/workers/maintaince';
import { createTaskWorker } from '../src/workers/task';
import { cleanupSchema } from './helpers';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

function resolvesInTime<T>(p1: Promise<T>, ms: number) {
  return Promise.race([p1, new Promise((_, reject) => setTimeout(reject, ms))]);
}

tap.jobs = 5;

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
      refillThresholdPct: 0.33,
    });

    worker.start();
    t.teardown(() => worker.stop());

    // start stop
    const plans = createMessagePlans(schema);
    const simpleTask = createTaskHandler({
      taskDef: defineTask({
        schema: Type.Object({}),
        task_name: 'happy-task',
        config: {
          expireInSeconds: 10,
          keepInSeconds: 10,
          retryBackoff: false,
          retryLimit: 1,
          retryDelay: 1,
        },
      }),
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

  t.test('skip fetching when maxConcurrency is reached', async (t) => {
    let handlerCalls = 0;
    let queryCalls = 0;
    const amountOfTasks = 50;
    const tasks: SelectTask[] = new Array<SelectTask>(amountOfTasks)
      .fill({
        state: 0,
        retrycount: 0,
        id: '0',
        data: {} as any,
        expire_in_seconds: 10,
      })
      .map((r, idx) => ({ ...r, id: `${idx}` }));
    const ee = new EventEmitter();

    const mockedPool: PGClient = {
      async query(props) {
        // this is to fetch tasks
        if (props.text.includes('FOR UPDATE SKIP LOCKED')) {
          queryCalls += 1;
          // $3 is amount of rows requested
          const rows = tasks.splice(0, props.values[2]);

          return {
            rowCount: rows.length,
            rows: rows as any,
          };
        }

        return {
          rowCount: 0,
          rows: [],
        };
      },
    };

    const worker = createTaskWorker({
      client: mockedPool,
      async handler(event) {
        handlerCalls += 1;

        const delay = 80;
        await new Promise((resolve) => setTimeout(resolve, delay));
        if (handlerCalls === amountOfTasks) {
          ee.emit('completed');
        }
        return {
          works: event.data.tn,
        };
      },
      // fetch all at once
      maxConcurrency: amountOfTasks,
      poolInternvalInMs: 20,
      refillThresholdPct: 0.33,
      queue: 'abc',
      schema: schema,
    });

    worker.start();

    await once(ee, 'completed');

    // wait for last item to be resolved
    await worker.stop();

    t.equal(queryCalls, 1);
    t.equal(handlerCalls, amountOfTasks);
  });

  t.test('refills', async (t) => {
    const queue = 'refills';
    let handlerCalls = 0;
    let queryCalls = 0;
    const ee = new EventEmitter();
    const tasks: SelectTask[] = new Array<SelectTask>(100)
      .fill({
        state: 0,
        retrycount: 0,
        id: '0',
        data: {} as any,
        expire_in_seconds: 10,
      })
      .map((r, idx) => ({ ...r, id: `${idx}` }));

    const mockedPool: PGClient = {
      async query(props) {
        // this is to fetch tasks
        if (props.text.includes('FOR UPDATE SKIP LOCKED')) {
          queryCalls += 1;
          // $3 is amount of rows
          const rows = tasks.splice(0, props.values[2]);

          if (tasks.length === 0) {
            ee.emit('drained');
          }

          return {
            rowCount: rows.length,
            rows: rows as any,
          };
        }

        return {
          rowCount: 0,
          rows: [],
        };
      },
    };
    const worker = createTaskWorker({
      client: mockedPool,
      async handler(event) {
        handlerCalls += 1;
        // resolves between 10-30ms
        const delay = 10 + Math.random() * 20;
        await new Promise((resolve) => setTimeout(resolve, delay));
        return {
          works: event.data.tn,
        };
      },
      maxConcurrency: 10,
      refillThresholdPct: 0.33,
      poolInternvalInMs: 100,
      queue: queue,
      schema: schema,
    });

    worker.start();

    // should be faster than 1000 because of refilling
    await t.resolves(resolvesInTime(once(ee, 'drained'), 600));

    // wait for last item to be resolved
    await worker.stop();

    // make some assumptions such that we dont fetch to much, aka threshold
    t.ok(queryCalls > 10);
    t.ok(queryCalls < 20);

    t.equal(handlerCalls, 100);
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
      refillThresholdPct: 0.33,
      queue: queue,
      schema: schema,
    });

    worker.start();
    t.teardown(() => worker.stop());

    // start stop
    const plans = createMessagePlans(schema);
    const simpleTask = createTaskHandler({
      taskDef: defineTask({
        schema: Type.Object({}),
        task_name: 'task',
        config: {
          expireInSeconds: 10,
          keepInSeconds: 10,
          retryBackoff: false,
          retryLimit: 1,
          retryDelay: 1,
        },
      }),
      handler: async () => {},
    });
    const task = simpleTask.from({});

    const insertTask = taskFactory(task, { type: 'direct' });

    t.equal(insertTask.r_l, 1);

    await query(sqlPool, plans.createTasks([insertTask]));

    await new Promise((resolve) => setTimeout(resolve, 2000));

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
      async handler() {
        called += 1;
        await new Promise((resolve) => setTimeout(resolve, 2000));
      },
      maxConcurrency: 10,
      poolInternvalInMs: 100,
      refillThresholdPct: 0.33,
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

    await new Promise((resolve) => setTimeout(resolve, 4000));

    t.equal(called, 2);

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = 'expired-task' LIMIT 1`)
      .then((r) => r.rows[0]);

    t.equal(result.state, TASK_STATES.failed);
    t.equal(result.output.message, 'handler execution exceeded 1000ms');
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

    t.equal(insertTask.kis, 0);
    t.equal(insertTask.saf, 0);

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

    const r = await sqlPool.query(`SELECT * FROM ${schema}.tasks WHERE id = ${result.id}`);
    t.equal(r.rowCount, 0);
  });

  t.test('event retention', async (t) => {
    const worker = createMaintainceWorker({
      client: sqlPool,
      schema,
      intervalInMs: 200,
    });

    const plans = createMessagePlans(schema);

    await query(
      sqlPool,
      plans.createEvents([
        { d: {}, e_n: 'event_retention_123', rid: -1 },
        { d: { exists: true }, e_n: 'event_retention_123' },
        { d: { exists: true }, e_n: 'event_retention_123', rid: 3 },
      ])
    );
    const r = await sqlPool.query(`SELECT * FROM ${schema}.events WHERE event_name = 'event_retention_123'`);
    t.equal(r.rowCount, 3);
    worker.start();
    t.teardown(() => worker.stop());

    await new Promise((resolve) => setTimeout(resolve, 500));
    const r2 = await sqlPool.query(
      `SELECT id, expire_at - now()::date as days, event_data FROM ${schema}.events WHERE event_name = 'event_retention_123' ORDER BY days desc`
    );
    t.equal(r2.rowCount, 2);
    t.equal(r2.rows[0].event_data.exists, true);
    // default retention
    t.equal(r2.rows[0].days, 30);
    t.equal(r2.rows[1].days, 3);
  });
});
