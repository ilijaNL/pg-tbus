import { TSchema } from '@sinclair/typebox';
import { EventHandler, Event, Task, TaskHandler, TaskDefinition } from './definitions';
import { query, Job, withTransaction, TaskDTO, createPlans, PGClient } from './sql';
import { createBaseWorker } from './worker';
import { Pool, PoolConfig } from 'pg';
import PgBoss from 'pg-boss';
import { migrate } from './migrations';
import path from 'path';

const getTaskName = (svc: string, task: string) => `${svc}--${task}`;

const createTaskDTOFactory =
  (svc: string) =>
  (task: Task): Job => {
    return {
      data: {
        data: task.data,
        tn: task.task_name,
      },
      name: getTaskName(svc, task.task_name),
    };
  };

type TBusConfiguration = {
  db: Pool | PoolConfig;
  schema: string;
};

const createTBus = (serviceName: string, configuration: TBusConfiguration) => {
  // K: task_name, should be unique
  const eventHandlersMap = new Map<string, EventHandler<string, TSchema>>();
  const taskHandlersMap = new Map<string, TaskHandler<any>>();
  const { schema, db } = configuration;

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
    application_name: 'tasks_mananger',
    schema: schema,
    noScheduling: true,
    onComplete: false,
  });

  // worker which responsible for creating tasks from incoming integrations events
  const fanoutWorker = createBaseWorker(
    async () => {
      // start transaction
      await withTransaction(pool, async (client) => {
        const events = await query(client, plans.getEvents(serviceName), { name: 'getEvents' });

        if (events.length === 0) {
          return;
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
      });

      return false;
    },
    { loopInterval: 1000 }
  );

  function registerHandler(...handlers: EventHandler<string, any>[]) {
    handlers.forEach((handler) => {
      if (eventHandlersMap.has(handler.task_name)) {
        throw new Error(`task ${handler.task_name} already registered`);
      }

      taskHandlersMap.set(handler.task_name, handler.handler);
      eventHandlersMap.set(handler.task_name, handler);
    });
  }

  function registerTask(...definitions: TaskDefinition<any>[]) {
    definitions.forEach((definition) => {
      if (taskHandlersMap.has(definition.task_name)) {
        throw new Error(`task ${definition.task_name} already registered`);
      }

      taskHandlersMap.set(definition.task_name, definition.handler);
    });
  }

  async function start() {
    await migrate(pool, schema, path.join(__dirname, '..', 'migrations'));
    await query(pool, plans.ensureServicePointer(serviceName));

    await boss.start();
    const taskListener = getTaskName(serviceName, '*');
    await boss.work<TaskDTO<any>, any>(
      taskListener,
      { teamSize: 50, newJobCheckIntervalSeconds: 2, teamConcurrency: 20 },
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
    },
    send: async (tasks: Task | Task[], client?: PGClient) => {
      const _tasks = Array.isArray(tasks) ? tasks : [tasks];
      await query(client ?? pool, plans.createTasks(_tasks.map((task) => taskFactory(task))));
    },
    stop: async () => {
      await boss.stop({ graceful: true, timeout: 2000 });
      await fanoutWorker.stop();
      if (!remotePool) {
        await pool.end();
      }
    },
  };
};

export default createTBus;
