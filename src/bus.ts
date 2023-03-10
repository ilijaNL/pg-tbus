import { TSchema } from '@sinclair/typebox';
import { EventHandler, Event, Task, TaskHandler, TaskDefinition, TaskTrigger, TaskConfig } from './definitions';
import { query, PGClient, createSql, QueryCommand } from './sql';
import { Pool, PoolConfig } from 'pg';
import PgBoss from 'pg-boss';
import { migrate } from './migrations';
import path from 'path';
import { debounce } from './utils';
import { createJobFactory, createMessagePlans, TaskDTO } from './messages';
import { createFanoutWorker } from './workers/fanout';
import { createMaintainceWorker } from './workers/maintaince';

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
  /**
   * Postgres database pool or existing pool
   */
  db: Pool | PoolConfig;
  /**
   * Task worker configuration
   */
  worker?: Partial<WorkerConfig>;
  /**
   * Postgres database schema to use
   * Note: changing schemas will result in event/task loss
   */
  schema: string;
  /**
   * Default configuration of eventHandlers/task handlers
   */
  handlerConfig?: Partial<TaskConfig>;
};

type TaskName = string;

const directTrigger: TaskTrigger = {
  type: 'direct',
} as const;

const createPlans = (schema: string) => {
  const sql = createSql(schema);

  function getLastEvent() {
    return sql<{ id: string; position: string }>`
      SELECT 
        id,
        pos as position
      FROM {{schema}}.events
      -- for index
      WHERE pos > 0
      ORDER BY pos DESC
      LIMIT 1`;
  }

  /**
   *  On new service entry, insert a cursor
   */
  function ensureServicePointer(service_name: string, position: number) {
    return sql`
      INSERT INTO {{schema}}.cursors (svc, l_p) 
      VALUES (${service_name}, ${position}) 
      ON CONFLICT DO NOTHING`;
  }

  return {
    ensureServicePointer,
    getLastEvent,
  };
};

/**
 * Create a pg-tbus instance.
 * @serviceName should be unique for the service. Using the same @serviceName with multiple instance will distribute work across instances
 *
 */
const createTBus = (serviceName: string, configuration: TBusConfiguration) => {
  // K: task_name, should be unique
  const eventHandlersMap = new Map<TaskName, EventHandler<string, TSchema>>();
  const taskHandlersMap = new Map<TaskName, TaskHandler<any>>();
  const { schema, db, worker } = configuration;

  let taskWorker: string | null = null;

  const workerConfig = Object.assign<WorkerConfig, Partial<WorkerConfig>>(
    {
      concurrency: 25,
      intervalInMs: 1500,
    },
    worker ?? {}
  );

  const getTaskName = (task: string) => `${serviceName}--${task}`;

  const toJob = createJobFactory({
    getTaskName: getTaskName,
    taskConfig: configuration.handlerConfig ?? {},
  });

  const jobPlans = createMessagePlans(schema);
  const plans = createPlans(schema);

  let pool: Pool;

  const remotePool = 'query' in db;

  if ('query' in db) {
    pool = db;
  } else {
    // create connection pool
    pool = new Pool({
      max: 3,
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
  const fanoutWorker = createFanoutWorker({
    schema: configuration.schema,
    serviceName: serviceName,
    pool,
    getEventHandlers: () => Array.from(eventHandlersMap.values()),
    jobFactory: toJob,
    onNewTasks() {
      if (taskWorker) {
        boss.notifyWorker(taskWorker);
      }
    },
  });

  const maintainceWorker = createMaintainceWorker({ client: pool, retentionInDays: 30, schema: schema });

  /**
   * Register one or more event handlers
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
   * Register one or more task definitions + handlers
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
    await query(pool, plans.ensureServicePointer(serviceName, +(lastCursor?.position ?? 0)));

    await boss.start();
    const taskListener = getTaskName('*');

    taskWorker = await boss.work<TaskDTO<any>, any>(
      taskListener,
      {
        teamSize: workerConfig.concurrency * 2,
        newJobCheckInterval: workerConfig.intervalInMs,
        teamConcurrency: workerConfig.concurrency,
      },
      async function handler({ data: { data, tn, trace: trigger } }) {
        const taskHandler = taskHandlersMap.get(tn);

        // log
        if (!taskHandler) {
          return;
        }

        await taskHandler({ name: tn, input: data, trigger: trigger });
      }
    );

    fanoutWorker.start();
    maintainceWorker.start();
  }

  const notifyFanout = debounce(() => fanoutWorker.notify(), { ms: 75, maxMs: 300 });
  const notifyWorker = debounce(
    (taskWorker: string | null) => {
      if (taskWorker) {
        boss.notifyWorker(taskWorker);
      }
    },
    { maxMs: 300, ms: 75 }
  );

  /**
   * Returnes a query command which can be used to do manual submitting
   */
  function getPublishCommand(events: Event<string, any>[]): QueryCommand<{}> {
    return jobPlans.createEvents(events);
  }

  /**
   * Returnes a query command which can be used to do manual submitting
   */
  function getSendCommand(tasks: Task[]): QueryCommand<{}> {
    return jobPlans.createJobs(tasks.map((task) => toJob(task, directTrigger)));
  }

  return {
    registerTask,
    registerHandler,
    start,
    getPublishCommand,
    /**
     * Publish one or many events to pg-tbus.
     * Second argument can be used to provide own pg client, this is especially useful when publishing during a transaction
     */
    publish: async (events: Event<string, any> | Event<string, any>[], client?: PGClient) => {
      const _events = Array.isArray(events) ? events : [events];
      await query(client ?? pool, getPublishCommand(_events));

      // check if instance is affected by the published events
      const allRegisteredEvents = Array.from(eventHandlersMap.values());
      const hasEffectToCurrentWorker = _events.some((e) =>
        allRegisteredEvents.some((ee) => ee.def.event_name === e.event_name)
      );
      if (hasEffectToCurrentWorker) {
        notifyFanout();
      }
    },
    getSendCommand,
    /**
     * Send one or many task to pg-tbus
     * Second argument can be used to provide own pg client, this is especially useful when publishing during a transaction
     */
    send: async (tasks: Task | Task[], client?: PGClient) => {
      const _tasks = Array.isArray(tasks) ? tasks : [tasks];
      await query(client ?? pool, getSendCommand(_tasks));

      // check if instance is affected by the new tasks
      const hasEffectToCurrentWorker = _tasks.some((t) => taskHandlersMap.has(t.task_name));
      if (taskWorker && hasEffectToCurrentWorker) {
        notifyWorker(taskWorker);
      }
    },
    /**
     * Gracefully stops all the workers of pg-tbus.
     */
    stop: async () => {
      taskWorker = null;
      await boss.stop({ graceful: true, timeout: 2000 });

      await Promise.all([fanoutWorker.stop(), maintainceWorker.stop()]);

      if (!remotePool) {
        await pool.end();
      }
    },
  };
};

export default createTBus;
