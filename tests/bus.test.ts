import { Type } from '@sinclair/typebox';
import EventEmitter, { once } from 'events';
import { Pool } from 'pg';
import tap from 'tap';
import { createEventHandler, createTBus, declareTask, defineEvent, defineTask } from '../src';
import { resolveWithinSeconds } from '../src/utils';
import { cleanupSchema, createRandomSchema } from './helpers';
import stringify from 'safe-stable-stringify';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

tap.jobs = 5;

tap.test('bus', async (tap) => {
  tap.jobs = 5;
  const schema = createRandomSchema();

  const sqlPool = new Pool({
    connectionString: connectionString,
    max: 5,
  });

  tap.teardown(async () => {
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  tap.test('smoke test', async ({ teardown, pass }) => {
    const bus = createTBus('smoke_test', { db: sqlPool, schema: schema });

    await bus.start();
    teardown(() => bus.stop());
    await new Promise((resolve) => setTimeout(resolve, 300));
    pass('passes');
  });

  tap.test('emit task', async ({ teardown, equal }) => {
    const ee = new EventEmitter();
    const queue = 'emit_queue';
    const bus = createTBus(queue, { db: sqlPool, schema: schema });
    const task_name = 'emit_task';
    const taskDef = defineTask({
      task_name: task_name,
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

    const waitProm = once(ee, 'handled');

    await bus.send([taskDef.from({ works: 'abcd' }), taskDef.from({ works: 'abcd' })]);

    await waitProm;
  });

  tap.test('emit task to different queue', async ({ teardown, equal }) => {
    const ee = new EventEmitter();
    const bus = createTBus('emit_queue', { db: sqlPool, schema: schema });
    const task_name = 'emit_task';

    const taskDef = defineTask({
      task_name: task_name,
      queue: 'abc',
      schema: Type.Object({ works: Type.String() }),
      handler: async (props) => {
        equal(props.input.works, 'abcd');
        equal(props.trigger.type, 'direct');
        ee.emit('handled');
      },
    });

    const waitProm = once(ee, 'handled');

    const bus2 = createTBus('abc', { db: sqlPool, schema: schema });

    bus2.registerTask(taskDef);
    await bus2.start();

    teardown(() => bus2.stop());

    await bus.send(taskDef.from({ works: 'abcd' }));
    await waitProm;
  });

  tap.test('emit event', async (t) => {
    const ee = new EventEmitter();
    const bus = createTBus('emit_event_queue', { db: sqlPool, schema: schema });

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
        t.equal(props.input.text, 'text222');
        t.equal(props.trigger.type, 'event');
        ee.emit('handled1');
      },
    });

    const handler2 = createEventHandler({
      task_name: 'task_2',
      eventDef: event,
      handler: async (props) => {
        t.equal(props.input.text, 'text222');
        t.equal(props.trigger.type, 'event');
        ee.emit('handled2');
      },
    });

    const handler3 = createEventHandler({
      task_name: 'task_3',
      eventDef: event2,
      handler: async (props) => {
        t.equal(props.input.rrr, 'event2');
        t.equal(props.trigger.type, 'event');
        ee.emit('handled3');
      },
    });

    bus.registerHandler(handler1, handler2, handler3);

    await bus.start();

    t.teardown(() => bus.stop());

    await t.resolves(
      resolveWithinSeconds(
        Promise.all([
          bus.publish([event.from({ text: 'text222' }), event2.from({ rrr: 'event2' })]),
          once(ee, 'handled1'),
          once(ee, 'handled2'),
          once(ee, 'handled3'),
        ]),
        3
      )
    );
  });

  tap.test('throws when task with different queue is registered', async (t) => {
    const bus = createTBus('svc', { db: sqlPool, schema: schema });

    const validTask = defineTask({
      task_name: 'task_abc',
      queue: 'svc',
      schema: Type.Object({ works: Type.String() }),
      handler: async () => {},
    });

    const throwTask = defineTask({
      task_name: 'task_abc',
      queue: 'queuea',
      schema: Type.Object({ works: Type.String() }),
      handler: async () => {},
    });

    t.doesNotThrow(() => bus.registerTask(validTask));
    t.throws(() => bus.registerTask(throwTask));
  });

  tap.test('throws when same task is registered', async ({ throws }) => {
    const bus = createTBus('svc', { db: sqlPool, schema: schema });
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

  tap.test('throws when same eventHandler is registered', async ({ throws }) => {
    const bus = createTBus('svc', { db: sqlPool, schema: schema });

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

  tap.test('task options', async ({ teardown, equal }) => {
    const queue = `task_options`;

    const bus = createTBus(queue, { db: sqlPool, schema: schema });
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

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = 'options_task' LIMIT 1`)
      .then((r) => r.rows[0]);

    const startAfterInMs = 45000;

    equal(result.retrylimit, 4);
    equal(result.retrydelay, 45);
    equal(result.retrybackoff, true);
    equal(result.singleton_key, null);
    equal(new Date(result.startafter).getTime() - new Date(result.createdon).getTime(), startAfterInMs);
    equal(new Date(result.keepuntil).getTime() - new Date(result.createdon).getTime(), startAfterInMs + 6000 * 1000);
  });

  tap.test('event handler options', async ({ teardown, equal }) => {
    const queue = 'handler-options';
    const bus = createTBus(queue, { db: sqlPool, schema: schema });

    const event = defineEvent({
      event_name: 'test',
      schema: Type.Object({}),
    });

    const config = {
      expireInSeconds: 3,
      retryBackoff: true,
      retryDelay: 33,
      retryLimit: 5,
      startAfterSeconds: 1,
      keepInSeconds: 20,
    };

    const ee = new EventEmitter();

    const handler = createEventHandler({
      eventDef: event,
      task_name: 'handler_task_config',
      handler: async () => {
        ee.emit('handled');
      },
      config: config,
    });

    bus.registerHandler(handler);

    await bus.start();

    teardown(() => bus.stop());

    const waitP = once(ee, 'handled');

    await bus.publish(event.from({}));

    await waitP;

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = 'handler_task_config' LIMIT 1`)
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

  tap.test('event handler config from payload', async ({ teardown, equal }) => {
    const queue = `handler_config_p`;
    const bus = createTBus(queue, { db: sqlPool, schema: schema });
    const ee = new EventEmitter();
    const event = defineEvent({
      event_name: 'event_handler_p',
      schema: Type.Object({
        c: Type.Number(),
      }),
    });
    const taskName = 'handler_event_opt';

    const handler = createEventHandler({
      eventDef: event,
      task_name: taskName,
      handler: async () => {
        ee.emit('p');
      },
      config: (input) => {
        equal(input.c, 91);
        return {
          retryDelay: input.c + 2,
          singletonKey: 'singleton',
        };
      },
    });

    bus.registerHandler(handler);

    await bus.start();

    teardown(() => bus.stop());

    const waitP = once(ee, 'p');

    await bus.publish(event.from({ c: 91 }));

    await waitP;

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${taskName}' LIMIT 1`)
      .then((r) => r.rows[0]);

    equal(result.retrydelay, 91 + 2);
    equal(result.singleton_key, 'singleton');
  });

  tap.test('singleton task', async ({ teardown, equal }) => {
    const queue = `singleton_task`;
    const bus = createTBus(queue, { db: sqlPool, schema: schema });
    const taskName = 'singleton_task';
    const taskDef = defineTask({
      task_name: taskName,
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

    await bus.send([
      taskDef.from({ works: 'abcd' }, { singletonKey: 'single' }),
      taskDef.from({ works: 'abcd' }, { singletonKey: 'single' }),
    ]);

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${taskName}'`)
      .then((r) => r.rows);

    equal(result.length, 1);
  });

  tap.test('event handler singleton from payload', async ({ teardown, equal }) => {
    const queue = `singleton_queue_payload`;
    // need a schema to ensure no concurrent events are created which affect the cursor assert test
    const schema = createRandomSchema();
    const bus = createTBus(queue, { db: sqlPool, schema: schema, worker: { intervalInMs: 200 } });

    const event = defineEvent({
      event_name: 'event_handler_singleton_payload',
      schema: Type.Object({
        c: Type.Number(),
      }),
    });

    const task_name = 'event_singleton_task';

    const handler = createEventHandler({
      eventDef: event,
      task_name: task_name,
      handler: async () => {
        await new Promise((resolve) => setTimeout(resolve, 200));
      },
      config: ({ c }) => {
        return {
          singletonKey: 'event_singleton_task_' + c,
        };
      },
    });

    bus.registerHandler(handler);

    await bus.start();

    teardown(async () => {
      await bus.stop();
      await cleanupSchema(sqlPool, schema);
    });

    const cursor = await sqlPool
      .query(`SELECT * FROM ${schema}.cursors WHERE svc = '${queue}'`)
      .then((r) => +r.rows[0].l_p);

    await bus.publish([event.from({ c: 91 }), event.from({ c: 93 }), event.from({ c: 91 })]);
    await bus.publish(event.from({ c: 91 }));
    await bus.publish(event.from({ c: 93 }));

    await new Promise((resolve) => setTimeout(resolve, 2000));

    const result = await sqlPool
      .query(`SELECT * FROM ${schema}.tasks WHERE queue = '${queue}' AND data->>'tn' = '${task_name}'`)
      .then((r) => r.rows);

    equal(result.length, 2);

    // this means that all events are processed by the service
    equal(
      await sqlPool.query(`SELECT * FROM ${schema}.cursors WHERE svc = '${queue}'`).then((r) => +r.rows[0].l_p),
      cursor + 5
    );
  });

  tap.test('when registering new service, add last event as cursor', async ({ equal, teardown }) => {
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

  tap.test('cursor', async ({ teardown, equal }) => {
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

    const p1 = once(ee, 'task_1');
    await bus.publish(event.from({ text: '1' }));

    await p1;

    const p2 = once(ee, 'task_1');
    await bus.publish(event.from({ text: '2' }));

    await p2;
  });
});

tap.test('getState', async (t) => {
  const bus = createTBus('doesnot-matter', {
    db: { connectionString: connectionString },
    schema: 'schema',
  });

  const taskA = defineTask({
    task_name: 'task_a',
    schema: Type.Object({ works: Type.String() }),
    handler: async (props) => {},
  });

  const taskB = defineTask({
    task_name: 'task_b',
    schema: Type.Object({ works: Type.String() }),
    handler: async (props) => {},
  });

  const eventA = defineEvent({
    event_name: 'eventA',
    schema: Type.Object({
      c: Type.Number(),
    }),
  });

  const eventB = defineEvent({
    event_name: 'eventB',
    schema: Type.Object({
      d: Type.Number(),
    }),
  });

  const handlerA = createEventHandler({
    task_name: 'task_event_a',
    eventDef: eventA,
    handler: async () => {},
  });

  const handlerB = createEventHandler({
    task_name: 'task_event_b',
    eventDef: eventB,
    handler: async () => {},
  });

  bus.registerTask(taskA, taskB);
  bus.registerHandler(handlerA, handlerB);

  t.matchSnapshot(stringify(bus.getState(), null, 2));
  // 2 taskhandlers and 2 event handlers
  t.equal(bus.getState().tasks.length, 4);
});

tap.test('concurrency', async (t) => {
  let published = false;
  let handlerCalls = 0;
  const schema = 'concurrentschema';
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
      t.equal(published, false);
      published = true;
      t.equal('123', props.input.text);
      handlerCalls += 1;
    },
  });

  const handler2 = createEventHandler({
    task_name: 'task',
    eventDef: event,
    handler: async (props) => {
      t.equal(published, false);
      published = true;
      t.equal('123', props.input.text);
      handlerCalls += 1;
    },
  });

  bus1.registerHandler(handler1);
  bus2.registerHandler(handler2);

  await Promise.all([bus1.start(), bus2.start()]);

  t.teardown(async () => {
    await Promise.all([bus1.stop(), bus2.stop()]);
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  await bus1.publish(event.from({ text: '123' }));

  await new Promise((resolve) => setTimeout(resolve, 2000));
  t.equal(handlerCalls, 1);
});
