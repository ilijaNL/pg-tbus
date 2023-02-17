import { TSchema } from '@sinclair/typebox';
import { EventHandler, Event, Task, TaskHandler, TaskDefinition } from './definitions';
import { query, Job, withTransaction, TaskDTO, createPlans, PGClient, createTaskDTOFactory, getTaskName } from './sql';
import { createBaseWorker } from './worker';
import { Pool, PoolConfig } from 'pg';
import PgBoss from 'pg-boss';
import { migrate } from './migrations';
import path from 'path';

export type WorkerConfig = {
  /**
   * Amount of concurrent tasks that can be executed, default 25
   */
  concurrency: number;
  /**
   * Interval of pooling the database for new work, default 1500
   */
  intervalInMs: number;
};

export type TBusConfiguration = {
  db: Pool | PoolConfig;
  worker?: Partial<WorkerConfig>;
  schema: string;
};

const createTBus = (serviceName: string, configuration: TBusConfiguration) => {
  // K: task_name, should be unique
  const eventHandlersMap = new Map<string, EventHandler<string, TSchema>>();
  const taskHandlersMap = new Map<string, TaskHandler<any>>();
  const { schema, db, worker } = configuration;

  let taskWorker: string | null = null;

  const workerConfig = Object.assign<WorkerConfig, Partial<WorkerConfig>>(
    {
      concurrency: 25,
      intervalInMs: 1500,
    },
    worker ?? {}
  );

  const taskFactory = createTaskDTOFactory(serviceName);
  const plans = createPlans(configuration.schema);
  let pool: Pool;

  const remotePool = 'query' in db;

  if ('query' in db) {
    pool = db;
  } else {
    // create connection pool
    pool = new Pool({
      ...db,
    });
  }

  const boss = new PgBoss({
    db: {
      executeSql: (text, values) =>
        pool.query({
          text,
          values,
        }),
    },
    schema: schema,
    noScheduling: true,
    onComplete: false,
  });

  // worker which responsible for creating tasks from incoming integrations events
  const fanoutWorker = createBaseWorker(
    async () => {
      // start transaction
      const newTasks = await withTransaction(pool, async (client) => {
        const events = await query(client, plans.getEvents(serviceName), { name: 'getEvents' });

        if (events.length === 0) {
          return false;
        }

        const tasks = events
          .map((event) => {
            return Array.from(eventHandlersMap.values()).reduce((agg, curr) => {
              if (curr.def.event_name !== event.event_name) {
                return agg;
              }

              const task: Job = taskFactory({
                data: event.event_data,
                task_name: curr.task_name,
                options: curr.taskOptions,
              });

              return [...agg, task];
            }, [] as Job[]);
          })
          .flat();

        // commit tasks
        await query(client, plans.createTasks(tasks));

        // update pointer
        const lastPosition = events[events.length - 1]!.position;
        await query(client, plans.updateCursor(serviceName, lastPosition));

        return tasks.length > 0;
      });

      if (newTasks && taskWorker) {
        boss.notifyWorker(taskWorker);
      }

      return false;
    },
    { loopInterval: 1000 }
  );

  /**
   * Register multiple event handlers
   */
  function registerHandler(...handlers: EventHandler<string, any>[]) {
    handlers.forEach((handler) => {
      if (eventHandlersMap.has(handler.task_name)) {
        throw new Error(`task ${handler.task_name} already registered`);
      }

      taskHandlersMap.set(handler.task_name, handler.handler);
      eventHandlersMap.set(handler.task_name, handler);
    });
  }

  /**
   * Register multiple task definitions
   */
  function registerTask(...definitions: TaskDefinition<any>[]) {
    definitions.forEach((definition) => {
      if (taskHandlersMap.has(definition.task_name)) {
        throw new Error(`task ${definition.task_name} already registered`);
      }

      taskHandlersMap.set(definition.task_name, definition.handler);
    });
  }

  /**
   * Start workers of the pg-tbus
   */
  async function start() {
    if (taskWorker) {
      return;
    }

    await migrate(pool, schema, path.join(__dirname, '..', 'migrations'));

    const lastCursor = (await query(pool, plans.getLastEvent()))[0];
    await query(pool, plans.ensureServicePointer(serviceName, lastCursor?.position ?? 0));

    await boss.start();
    const taskListener = getTaskName(serviceName, '*');

    taskWorker = await boss.work<TaskDTO<any>, any>(
      taskListener,
      {
        teamSize: workerConfig.concurrency * 2,
        newJobCheckInterval: workerConfig.intervalInMs,
        teamConcurrency: workerConfig.concurrency,
      },
      async function handler({ data: { data, tn } }) {
        const taskHandler = taskHandlersMap.get(tn);

        // log
        if (!taskHandler) {
          return;
        }

        await taskHandler({ name: tn, input: data });
      }
    );

    fanoutWorker.start();
  }

  return {
    registerTask,
    registerHandler,
    start,
    publish: async (events: Event<string, any> | Event<string, any>[], client?: PGClient) => {
      await query(client ?? pool, plans.createEvents(Array.isArray(events) ? events : [events]));

      // optimize this by checking if this instance is affect (has event listeners)
      fanoutWorker.notify();
    },
    send: async (tasks: Task | Task[], client?: PGClient) => {
      const _tasks = Array.isArray(tasks) ? tasks : [tasks];
      await query(client ?? pool, plans.createTasks(_tasks.map((task) => taskFactory(task))));
      // optimize this by checking if this instance is affect (has task listeners)
      fanoutWorker.notify();
    },
    stop: async () => {
      taskWorker = null;
      await boss.stop({ graceful: true, timeout: 2000 });
      await fanoutWorker.stop();
      if (!remotePool) {
        await pool.end();
      }
    },
  };
};

export default createTBus;
