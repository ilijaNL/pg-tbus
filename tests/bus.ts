import { Type } from '@sinclair/typebox';
import EventEmitter, { once } from 'events';
import { Pool } from 'pg';
import { test } from 'tap';
import { createEventHandler, createTBus, defineEvent, defineTask } from '../src';
import { resolveWithinSeconds } from '../src/worker';
import { cleanupSchema } from './test_utils';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';
const schema = 'abc';

test('bus', async ({ teardown, test }) => {
  const cleanupPool = new Pool({
    connectionString: connectionString,
  });

  teardown(async () => {
    await cleanupSchema(cleanupPool, schema);
    await cleanupPool.end();
  });

  test('smoke test', async ({ teardown, pass }) => {
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });

    await bus.start();
    teardown(() => bus.stop());
    await new Promise((resolve) => setTimeout(resolve, 300));
    pass('passes');
  });

  test('emit task', async ({ teardown, equal, plan }) => {
    plan(2);
    const ee = new EventEmitter();
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });
    const taskDef = defineTask({
      task_name: 'task_abc',
      schema: Type.Object({ works: Type.String() }),
      handler: async (props) => {
        equal(props.input.works, 'abcd');
        equal(props.trigger.type, 'direct');
        ee.emit('handled');
      },
    });

    bus.registerTask(taskDef);

    await bus.start();

    teardown(() => bus.stop());

    await bus.send(taskDef.from({ works: 'abcd' }));

    await once(ee, 'handled');
  });

  test('emit event', async ({ teardown, equal, plan }) => {
    plan(6);

    const ee = new EventEmitter();
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });

    const event = defineEvent({
      event_name: 'test_event',
      schema: Type.Object({
        text: Type.String(),
      }),
    });

    const event2 = defineEvent({
      event_name: 'awdawd',
      schema: Type.Object({
        rrr: Type.String(),
      }),
    });

    const handler1 = createEventHandler({
      task_name: 'task_1',
      eventDef: event,
      handler: async (props) => {
        equal(props.input.text, 'text222');
        equal(props.trigger.type, 'event');
        ee.emit('handled1');
      },
    });

    const handler2 = createEventHandler({
      task_name: 'task_2',
      eventDef: event,
      handler: async (props) => {
        equal(props.input.text, 'text222');
        equal(props.trigger.type, 'event');
        ee.emit('handled2');
      },
    });

    const handler3 = createEventHandler({
      task_name: 'task_3',
      eventDef: event2,
      handler: async (props) => {
        equal(props.input.rrr, 'event2');
        equal(props.trigger.type, 'event');
        ee.emit('handled3');
      },
    });

    bus.registerHandler(handler1, handler2, handler3);

    await bus.start();

    teardown(() => bus.stop());

    await bus.publish([event.from({ text: 'text222' }), event2.from({ rrr: 'event2' })]);

    await resolveWithinSeconds(Promise.all([once(ee, 'handled1'), once(ee, 'handled2'), once(ee, 'handled3')]), 3);
  });

  test('throws when same task is registered', async ({ throws }) => {
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });
    const taskDef1 = defineTask({
      task_name: 'task_abc',
      schema: Type.Object({ works: Type.String() }),
      handler: async (props) => {},
    });
    const taskDef2 = defineTask({
      task_name: 'task_abc',
      schema: Type.Object({ abc: Type.String() }),
      handler: async (props) => {},
    });

    throws(() => bus.registerTask(taskDef1, taskDef2));
  });

  test('throws when same eventHandler is registered', async ({ throws }) => {
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });

    const event = defineEvent({
      event_name: 'test_event',
      schema: Type.Object({
        text: Type.String(),
      }),
    });

    const event2 = defineEvent({
      event_name: 'awdawd',
      schema: Type.Object({
        rrr: Type.String(),
      }),
    });

    const handler1 = createEventHandler({ task_name: 'task_2', eventDef: event, handler: async (props) => {} });
    const handler2 = createEventHandler({ task_name: 'task_2', eventDef: event2, handler: async (props) => {} });

    throws(() => bus.registerHandler(handler1, handler2));
  });

  test('task options', async ({ teardown, equal }) => {
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });
    const taskDef = defineTask({
      task_name: 'options_task',
      schema: Type.Object({ works: Type.String() }),
      options: {
        expireInSeconds: 5,
        retryBackoff: true,
        retryDelay: 45,
        retryLimit: 4,
        startAfterSeconds: 45,
        keepInSeconds: 6000,
      },
      handler: async () => {},
    });

    bus.registerTask(taskDef);

    await bus.start();

    teardown(() => bus.stop());

    await bus.send(taskDef.from({ works: 'abcd' }));

    const taskName = `svc--options_task`;

    const result = await cleanupPool
      .query(`SELECT * FROM ${schema}.job WHERE name = '${taskName}' LIMIT 1`)
      .then((r) => r.rows[0]);

    const startAfterInMs = 45000;

    equal(result.retrylimit, 4);
    equal(result.retrydelay, 45);
    equal(result.retrybackoff, true);
    equal(new Date(result.startafter).getTime() - new Date(result.createdon).getTime(), startAfterInMs);
    equal(new Date(result.keepuntil).getTime() - new Date(result.createdon).getTime(), startAfterInMs + 6000 * 1000);
  });
});

test('when registering new service, add last event as cursor', async ({ equal, teardown }) => {
  const event = defineEvent({
    event_name: 'test_event',
    schema: Type.Object({}),
  });

  const bus = createTBus('serviceA', { db: { connectionString }, schema: schema });
  await bus.start();

  teardown(() => bus.stop());

  await bus.publish([event.from({}), event.from({})]);

  const bus2 = createTBus('serviceB', { db: { connectionString }, schema: schema });
  await bus2.start();
  teardown(() => bus2.stop());

  const pool = new Pool({
    connectionString: connectionString,
    max: 1,
  });
  teardown(() => pool.end());

  const result = await pool.query<{ l_p: number }>(
    ` SELECT  l_p  FROM ${schema}.cursors  WHERE svc = 'serviceB' LIMIT 1`
  );

  equal(result.rows[0]?.l_p, '2');
});
