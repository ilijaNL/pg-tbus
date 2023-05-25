import tap from 'tap';
import { declareTask, defineEvent, defineTask } from '../src';
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

tap.test('create task from declaredTask', async (t) => {
  const taskDecl = declareTask({
    schema: Type.Object({ item: Type.String({ minLength: 5 }) }),
    task_name: 'taskTest',
    config: {
      retryLimit: 10,
      retryBackoff: true,
    },
  });

  const task = taskDecl.from({ item: 'dddddd' }, { expireInSeconds: 10, retryBackoff: false });
  t.same(task.config, { expireInSeconds: 10, retryLimit: 10, retryBackoff: false });
  t.same(task.data, { item: 'dddddd' });

  t.throws(() => taskDecl.from({ item: '2' }));

  const definedTask = defineTask({
    ...taskDecl,
    handler: async (props) => {
      t.equal(props.input.item, 'abcdef');
      return 'works';
    },
  });

  const res = await definedTask.handler({ input: { item: 'abcdef' }, name: 'taskTest', trigger: { type: 'direct' } });

  t.equal(res, 'works');
});
