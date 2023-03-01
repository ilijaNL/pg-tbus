import tap from 'tap';
import { defineEvent, defineTask } from '../src';
import { Type } from '@sinclair/typebox';

tap.test('throws with invalid task data', async ({ throws }) => {
  const task = defineTask({
    task_name: 'task_abc',
    schema: Type.Object({ item: Type.String({ minLength: 5 }) }),
    handler: async () => [],
  });

  throws(() => task.from({ item: '2' }));
});

tap.test('throws with invalid event data', async ({ throws }) => {
  const event = defineEvent({
    event_name: 'abc',
    schema: Type.Object({ item: Type.String({ minLength: 5 }) }),
  });

  throws(() => event.from({ item: '2' }));
});
