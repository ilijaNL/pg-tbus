import { TSchema } from '@sinclair/typebox';
import {
  EventHandler,
  Event,
  Task,
  TaskHandler,
  TaskDefinition,
  TaskTrigger,
  TaskConfig,
  EventSpec,
} from './definitions';
import { query, PGClient, createSql, QueryCommand } from './sql';
import { Pool, PoolConfig } from 'pg';
import { migrate } from './migrations';
import path from 'path';
import { debounce } from './utils';
import { createTaskFactory, createMessagePlans } from './messages';
import { createFanoutWorker } from './workers/fanout';
import { createTaskWorker } from './workers/task';
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
  /**
   * Refill threshold percentage, value between 0 - 1, default 0.33.
   * Automatically refills the task workers queue when activeItems / concurrency < refillPct.
   */
  refillPct: number;
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
   * Postgres database schema to use.
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

export type TaskState = {
  task_name: string;
  /**
   * This task will be created when this event happens
   */
  on_event?: string;
  schema: TSchema;
  config: Partial<TaskConfig>;
};

export type BusState = {
  queue: string;
  events: Array<EventSpec<string, TSchema>>;
  tasks: Array<TaskState>;
};

export type Bus = {
  registerTask: (...definitions: TaskDefinition<any>[]) => void;
  registerHandler: (...handlers: EventHandler<string, any>[]) => void;
  start: () => Promise<void>;
  getPublishCommand: (events: Event<string, any>[]) => QueryCommand<{}>;
  publish: (events: Event<string, any> | Event<string, any>[], client?: PGClient) => Promise<void>;
  getSendCommand: (tasks: Task[]) => QueryCommand<{}>;
  send: (tasks: Task | Task[], client?: PGClient) => Promise<void>;
  stop: () => Promise<void>;
  getState: () => BusState;
};

/**
 * Create a pg-tbus instance.
 * `serviceName` should be unique for the service and is used as the queue name.
 * Using the same `serviceName` with multiple instance will distribute work across instances.
 */
export const createTBus = (serviceName: string, configuration: TBusConfiguration): Bus => {
  // K: task_name, should be unique
  const eventHandlersMap = new Map<TaskName, EventHandler<string, TSchema>>();
  const taskHandlersMap = new Map<TaskName, TaskState & { handler: TaskHandler<any> }>();
  const { schema, db, worker } = configuration;

  const state = {
    started: false,
    stopped: false,
  };

  const workerConfig = Object.assign<WorkerConfig, Partial<WorkerConfig>>(
    {
      concurrency: 25,
      intervalInMs: 1500,
      refillPct: 0.33,
    },
    worker ?? {}
  );

  const toTask = createTaskFactory({
    queue: serviceName,
    taskConfig: configuration.handlerConfig ?? {},
  });

  const taskPlans = createMessagePlans(schema);
  const plans = createPlans(schema);

  let pool: Pool;

  let cleanupDB = async () => {
    // noop
  };

  if ('query' in db) {
    pool = db;
  } else {
    // create connection pool
    pool = new Pool({
      max: 3,
      ...db,
    });
    cleanupDB = async () => {
      await pool.end();
    };
  }

  const tWorker = createTaskWorker({
    client: pool,
    maxConcurrency: workerConfig.concurrency,
    poolInternvalInMs: workerConfig.intervalInMs,
    queue: serviceName,
    refillThresholdPct: workerConfig.refillPct,
    schema,
    async handler({ data: { data, tn, trace } }) {
      const taskHandler = taskHandlersMap.get(tn);

      // log
      if (!taskHandler) {
        console.error('task handler ' + tn + 'not registered for service ' + serviceName);
        throw new Error('task handler ' + tn + 'not registered for service ' + serviceName);
      }

      await taskHandler.handler({ name: tn, input: data, trigger: trace });
    },
  });

  // worker which responsible for creating tasks from incoming integrations events
  const fanoutWorker = createFanoutWorker({
    schema: configuration.schema,
    serviceName: serviceName,
    pool,
    getEventHandlers: () => Array.from(eventHandlersMap.values()),
    taskFactory: toTask,
    onNewTasks() {
      notifyWorker();
    },
  });

  const maintainceWorker = createMaintainceWorker({ client: pool, retentionInDays: 30, schema: schema });

  const notifyFanout = debounce(() => fanoutWorker.notify(), { ms: 75, maxMs: 300 });
  const notifyWorker = debounce(() => tWorker.notify(), { maxMs: 300, ms: 75 });

  /**
   * Register one or more event handlers
   */
  function registerHandler(...handlers: EventHandler<string, any>[]) {
    handlers.forEach((handler) => {
      if (eventHandlersMap.has(handler.task_name)) {
        throw new Error(`task ${handler.task_name} already registered`);
      }

      taskHandlersMap.set(handler.task_name, {
        handler: handler.handler,
        schema: handler.def.schema,
        task_name: handler.task_name,
        on_event: handler.def.event_name,
        config: typeof handler.config === 'function' ? {} : handler.config,
      });
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

      if (definition.queue && definition.queue !== serviceName) {
        throw new Error(
          `task ${definition.task_name} belongs to a different queue. Expected ${serviceName}, got ${definition.queue}`
        );
      }

      taskHandlersMap.set(definition.task_name, {
        config: definition.config,
        handler: definition.handler,
        schema: definition.schema,
        task_name: definition.task_name,
      });
    });
  }

  /**
   * Start workers of the pg-tbus
   */
  async function start() {
    if (state.started) {
      return;
    }

    state.started = true;

    await migrate(pool, schema, path.join(__dirname, '..', 'migrations'));

    const lastCursor = (await query(pool, plans.getLastEvent()))[0];
    await query(pool, plans.ensureServicePointer(serviceName, +(lastCursor?.position ?? 0)));

    tWorker.start();
    fanoutWorker.start();
    maintainceWorker.start();
  }

  /**
   * Returnes a query command which can be used to do manual submitting
   */
  function getPublishCommand(events: Event<string, any>[]): QueryCommand<{}> {
    return taskPlans.createEvents(events);
  }

  /**
   * Returnes a query command which can be used to do manual submitting
   */
  function getSendCommand(tasks: Task[]): QueryCommand<{}> {
    return taskPlans.createTasks(tasks.map((task) => toTask(task, directTrigger)));
  }

  /**
   * Get the current state of all registered events, eventhandlers and taskhandlers
   */
  function getState(): BusState {
    const events: BusState['events'] = Array.from(eventHandlersMap.values()).map((eh) => ({
      event_name: eh.def.event_name,
      schema: eh.def.schema,
    }));

    const tasks: BusState['tasks'] = Array.from(taskHandlersMap.values()).map((eh) => ({
      config: eh.config,
      task_name: eh.task_name,
      on_event: eh.on_event,
      schema: eh.schema,
    }));

    return {
      events,
      queue: serviceName,
      tasks: tasks,
    };
  }

  return {
    registerTask,
    registerHandler,
    start,
    getPublishCommand,
    getState,
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
      if (hasEffectToCurrentWorker) {
        notifyWorker();
      }
    },
    /**
     * Gracefully stops all the workers of pg-tbus.
     */
    stop: async () => {
      if (state.started === false || state.stopped === true) {
        return;
      }

      state.stopped = true;

      await Promise.all([fanoutWorker.stop(), maintainceWorker.stop(), tWorker.stop()]);

      await cleanupDB();
    },
  };
};

export default createTBus;
