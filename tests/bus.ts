import { Type } from '@sinclair/typebox';
import EventEmitter, { once } from 'events';
import { Pool } from 'pg';
import { test } from 'tap';
import { createEventHandler, createTBus, defineEvent, defineTask } from '../src';
import { resolveWithinSeconds } from '../src/worker';
import { cleanupSchema } from './utils';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';
const schema = 'schema';

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
    plan(1);
    const ee = new EventEmitter();
    const bus = createTBus('svc', { db: { connectionString: connectionString }, schema: schema });
    const taskDef = defineTask({
      task_name: 'task_abc',
      schema: Type.Object({ works: Type.String() }),
      handler: async (props) => {
        equal(props.input.works, 'abcd');
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
    plan(3);

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
        ee.emit('handled1');
      },
    });

    const handler2 = createEventHandler({
      task_name: 'task_2',
      eventDef: event,
      handler: async (props) => {
        equal(props.input.text, 'text222');
        ee.emit('handled2');
      },
    });

    const handler3 = createEventHandler({
      task_name: 'task_3',
      eventDef: event2,
      handler: async (props) => {
        equal(props.input.rrr, 'event2');
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
});
