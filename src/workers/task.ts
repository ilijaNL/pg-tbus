import { createBaseWorker } from './base';
import { PGClient, query } from '../sql';
import { SelectTask, createMessagePlans } from '../messages';
import { resolveWithinSeconds } from '../utils';
import { createBatcher } from 'node-batcher';

type ResolveResponse = { task_id: string; success: boolean; payload: any };

function replaceErrors(value: any) {
  if (value instanceof Error) {
    var error = {} as any;

    Object.getOwnPropertyNames(value).forEach(function (propName) {
      error[propName] = (value as any)[propName];
    });

    return error;
  }

  return value;
}

function mapCompletionDataArg(data: any) {
  if (data === null || typeof data === 'undefined' || typeof data === 'function') {
    return null;
  }

  const result = typeof data === 'object' && !Array.isArray(data) ? data : { value: data };

  return replaceErrors(result);
}

export const createTaskWorker = (props: {
  client: PGClient;
  handler: (event: SelectTask) => Promise<any>;
  queue: string;
  schema: string;
  maxConcurrency: number;
  poolInternvalInMs: number;
  refillThresholdPct: number;
}) => {
  const activeTasks = new Map<string, Promise<any>>();
  const { maxConcurrency, client, queue, schema, handler, poolInternvalInMs, refillThresholdPct } = props;
  const plans = createMessagePlans(schema);
  // used to determine if we can refetch early
  let hasMoreTasks = false;

  const resolveTaskBatcher = createBatcher<ResolveResponse>({
    async onFlush(batch) {
      const q = plans.resolveTasks(batch.map(({ data: i }) => ({ p: i.payload, s: i.success, t: i.task_id })));
      await query(client, q);
    },
    // dont make to big since payload can be big
    maxSize: 100,
    // keep it low latency
    maxTimeInMs: 50,
  });

  function resolveTask(task: SelectTask, err: any, result?: any) {
    // if this throws, something went really wrong
    resolveTaskBatcher.add({ payload: mapCompletionDataArg(err ?? result), success: !err, task_id: task.id });

    activeTasks.delete(task.id);

    // if some treshhold is reached, we can refetch
    const threshHoldPct = refillThresholdPct;
    if (hasMoreTasks && activeTasks.size / maxConcurrency < threshHoldPct) {
      taskWorker.notify();
    }
  }

  const taskWorker = createBaseWorker(
    async () => {
      if (activeTasks.size >= maxConcurrency) {
        return;
      }

      const requestedAmount = maxConcurrency - activeTasks.size;
      const tasks = await query(client, plans.getTasks({ amount: requestedAmount, queue }));

      // high chance that there are more tasks when requested amount is same as fetched
      hasMoreTasks = tasks.length === requestedAmount;

      if (tasks.length === 0) {
        return;
      }

      tasks.forEach((task) => {
        const taskPromise = resolveWithinSeconds(handler(task), task.expire_in_seconds)
          .then((result) => {
            resolveTask(task, null, result);
          })
          .catch((err) => {
            resolveTask(task, err);
          });

        activeTasks.set(task.id, taskPromise);
      });
    },
    { loopInterval: poolInternvalInMs }
  );

  return {
    ...taskWorker,
    async stop() {
      await taskWorker.stop();
      await Promise.all(Array.from(activeTasks.values()));
      await resolveTaskBatcher.waitForAll();
    },
  };
};
