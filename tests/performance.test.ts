import { Type } from '@sinclair/typebox';
import EventEmitter, { once } from 'events';
import { Pool } from 'pg';
import tap from 'tap';
import { createEventHandler, createTBus, defineEvent } from '../src';
import { cleanupSchema, createRandomSchema } from './helpers';

const connectionString = process.env.PG ?? 'postgres://postgres:postgres@localhost:5432/app';

tap.test('latency', async (t) => {
  const schema = createRandomSchema();
  const ee = new EventEmitter();
  const bus = createTBus('perf', {
    db: { connectionString: connectionString },
    schema: schema,
    worker: { intervalInMs: 1000 },
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

  const handler = createEventHandler({
    task_name: 'task',
    eventDef: event,
    handler: async (props) => {
      ee.emit('published');
      t.equal('latency', props.input.text);
    },
  });

  bus.registerHandler(handler);

  await bus.start();

  t.teardown(async () => {
    await bus.stop();
    await cleanupSchema(sqlPool, schema);
    await sqlPool.end();
  });

  const time = Date.now();

  await bus.publish(event.from({ text: 'latency' }));

  await once(ee, 'published');
  t.equal(Date.now() - time < 1100, true);
});

// tap.test('bench', { timeout: 3000 }, async (t) => {
//   t.plan(1);

//   const schema = createRandomSchema();
//   const ee = new EventEmitter();

//   const bus = createTBus('perf', {
//     db: { connectionString: connectionString },
//     schema: schema,
//     worker: { intervalInMs: 2000 },
//   });

//   const sqlPool = new Pool({
//     connectionString: connectionString,
//   });

//   const event = defineEvent({
//     event_name: 'test_event',
//     schema: Type.Object({
//       text: Type.String(),
//     }),
//   });

//   const handler = createEventHandler({
//     task_name: 'task',
//     eventDef: event,
//     handler: async (props) => {
//       ee.emit('published');
//       t.equal('perf', props.input.text);
//     },
//   });

//   bus.registerHandler(handler);

//   await bus.start();

//   t.teardown(async () => {
//     await bus.stop();
//     await cleanupSchema(sqlPool, schema);
//     await sqlPool.end();
//   });

//   console.log('start inserting');
//   // insert 500000 events
//   await sqlPool.query(
//     `insert into ${schema}.events (event_name, event_data) (select 'e_' || generate_series(0, 500000) as event_name, json_build_object() as event_data);`
//   );

//   console.log('start inserting');

//   await bus.publish(event.from({ text: 'perf' }));
//   await resolveWithinSeconds(once(ee, 'published'), 10);
// });
