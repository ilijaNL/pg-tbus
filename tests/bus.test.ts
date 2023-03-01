import { Type } from '@sinclair/typebox';
import EventEmitter, { once } from 'events';
import { Pool } from 'pg';
import tap from 'tap';
import { createEventHandler, createTBus, defineEvent, defineTask } from '../src';
import { resolveWithinSeconds } from '../src/utils';
import { cleanupSchema, createRandomSchema } from './helpers';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

tap.jobs = 5;

tap.test('bus', async (t) => {
  const schema = 'abc';

  const sqlPool = new Pool({
    connectionString: connectionString,
  });

  t.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  t.test('smoke test', async ({ teardown, pass }) => {
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });

    await bus.start();
    teardown(() => bus.stop());
    await new Promise((resolve) => setTimeout(resolve, 300));
    pass('passes');
  });

  t.test('emit task', async ({ teardown, equal, plan }) => {
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

  t.test('emit event', async ({ teardown, equal, plan }) => {
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

  t.test('throws when same task is registered', async ({ throws }) => {
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

  t.test('throws when same eventHandler is registered', async ({ throws }) => {
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

  t.test('task options', async ({ teardown, equal }) => {
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });
    const taskDef = defineTask({
      task_name: 'options_task',
      schema: Type.Object({ works: Type.String() }),
      config: {
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

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.job WHERE name = '${taskName}' LIMIT 1`)
      .then((r) => r.rows[0]);

    const startAfterInMs = 45000;

    equal(result.retrylimit, 4);
    equal(result.retrydelay, 45);
    equal(result.retrybackoff, true);
    equal(new Date(result.startafter).getTime() - new Date(result.createdon).getTime(), startAfterInMs);
    equal(new Date(result.keepuntil).getTime() - new Date(result.createdon).getTime(), startAfterInMs + 6000 * 1000);
  });

  t.test('event handler options', async ({ teardown, equal }) => {
    const bus = createTBus('sss', { db: { connectionString: connectionString }, schema: schema });

    const event = defineEvent({
      event_name: 'test',
      schema: Type.Object({}),
    });

    const config = {
      expireInSeconds: 3,
      retryBackoff: true,
      retryDelay: 33,
      retryLimit: 5,
      startAfterSeconds: 2,
      keepInSeconds: 20,
    };

    const handler = createEventHandler({
      eventDef: event,
      task_name: 'handler_task_config',
      handler: async () => {},
      config: config,
    });

    bus.registerHandler(handler);

    await bus.start();

    teardown(() => bus.stop());

    await bus.publish(event.from({}));

    // can be short since it notifies internally
    await new Promise((resolve) => setTimeout(resolve, 1500));

    const taskName = `sss--handler_task_config`;

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.job WHERE name = '${taskName}' LIMIT 1`)
      .then((r) => r.rows[0]);

    const startAfterInMs = config.startAfterSeconds * 1000;

    equal(result.retrylimit, config.retryLimit);
    equal(result.retrydelay, config.retryDelay);
    equal(result.retrybackoff, config.retryBackoff);
    equal(new Date(result.startafter).getTime() - new Date(result.createdon).getTime(), startAfterInMs);
    equal(
      new Date(result.keepuntil).getTime() - new Date(result.createdon).getTime(),
      startAfterInMs + config.keepInSeconds * 1000
    );
  });

  t.test('when registering new service, add last event as cursor', async ({ equal, teardown }) => {
    const schema = createRandomSchema();
    const event = defineEvent({
      event_name: 'test_event',
      schema: Type.Object({}),
    });

    const bus = createTBus('serviceA', { db: { connectionString }, schema: schema });
    await bus.start();

    await bus.publish([event.from({}), event.from({})]);

    const bus2 = createTBus('serviceB', { db: { connectionString }, schema: schema });
    await bus2.start();

    teardown(async () => {
      await bus.stop();
      await bus2.stop();
      await cleanupSchema(sqlPool, schema);
    });

    const result = await sqlPool.query<{ l_p: string }>(
      `SELECT l_p FROM ${schema}.cursors WHERE svc = 'serviceB' LIMIT 1`
    );

    equal(result.rows[0]?.l_p, '2');
  });

  t.test('cursor', async ({ teardown, equal, plan }) => {
    plan(4);
    const ee = new EventEmitter();
    const schema = createRandomSchema();
    const bus = createTBus('cursorservice', { db: { connectionString: connectionString }, schema: schema });

    const event = defineEvent({
      event_name: 'test_event',
      schema: Type.Object({
        text: Type.String(),
      }),
    });

    let count = 0;

    const handler1 = createEventHandler({
      task_name: 'task_1',
      eventDef: event,
      handler: async (props) => {
        count++;
        equal(props.input.text, count.toString());

        if (props.trigger.type === 'event') {
          equal(props.trigger.e.p, count);
        }

        ee.emit('task_1');
      },
    });

    bus.registerHandler(handler1);

    await bus.start();

    teardown(async () => {
      await bus.stop();
      await cleanupSchema(sqlPool, schema);
    });

    await bus.publish(event.from({ text: '1' }));

    await once(ee, 'task_1');

    await bus.publish(event.from({ text: '2' }));

    await once(ee, 'task_1');
  });
});

tap.test('concurrency', async ({ teardown, plan, equal }) => {
  plan(1);

  const schema = createRandomSchema();
  const bus1 = createTBus('concurrent', {
    db: { connectionString: connectionString },
    schema: schema,
    worker: { intervalInMs: 500 },
  });
  const bus2 = createTBus('concurrent', {
    db: { connectionString: connectionString },
    schema: schema,
    worker: { intervalInMs: 500 },
  });

  const sqlPool = new Pool({
    connectionString: connectionString,
  });

  const event = defineEvent({
    event_name: 'test_event',
    schema: Type.Object({
      text: Type.String(),
    }),
  });

  const handler1 = createEventHandler({
    task_name: 'task',
    eventDef: event,
    handler: async (props) => {
      equal('123', props.input.text);
    },
  });

  const handler2 = createEventHandler({
    task_name: 'task',
    eventDef: event,
    handler: async (props) => {
      equal('123', props.input.text);
    },
  });

  bus1.registerHandler(handler1);
  bus2.registerHandler(handler2);

  await Promise.all([bus1.start(), bus2.start()]);

  teardown(async () => {
    await Promise.all([bus1.stop(), bus2.stop()]);
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  await bus1.publish(event.from({ text: '123' }));

  await new Promise((resolve) => setTimeout(resolve, 6000));
});
