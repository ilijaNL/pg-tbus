import { TSchema } from '@sinclair/typebox';
import { EventHandler, Event, Task, TaskHandler, TaskDefinition, TaskTrigger, TaskOptions } from './definitions';
import { query, withTransaction, PGClient, createSql, combineSQL } from './sql';
import { createBaseWorker } from './worker';
import { Pool, PoolConfig } from 'pg';
import PgBoss from 'pg-boss';
import { migrate } from './migrations';
import path from 'path';
import fastJSON from 'fast-json-stable-stringify';
import { debounce } from './utils';

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
  tasks?: Partial<TaskOptions>;
};

type TaskName = string;

type TaskDTO<T> = { tn: string; data: T; trace: TaskTrigger };

type Job<T = object> = {
  name: string;
  data: TaskDTO<T>;
} & TaskOptions;

const directTrigger: TaskTrigger = {
  type: 'direct',
} as const;

const createPlans = (schema: string) => {
  const sql = createSql(schema);

  function createJobs(jobs: Job[]) {
    return sql<{}>`
      INSERT INTO {{schema}}.job (
        id,
        name,
        data,
        priority,
        retryLimit,
        retryDelay,
        retryBackoff,
        startAfter,
        expireIn,
        keepUntil,
        on_complete
      )
      SELECT
        gen_random_uuid() as id,
        name,
        data,
        0 as priority,
        "retryLimit",
        "retryDelay",
        "retryBackoff",
        (now() + ("startAfterSeconds" * interval '1s'))::timestamp with time zone as startAfter,
        "expireInSeconds" * interval '1s' as expireIn,
        (now() + ("startAfterSeconds" * interval '1s') + ("keepInSeconds" * interval '1s'))::timestamp with time zone as keepUntil,
        false as on_complete
      FROM json_to_recordset(${fastJSON(jobs)}) as x(
        name text,
        data jsonb,
        "retryLimit" integer,
        "retryDelay" integer,
        "retryBackoff" boolean,
        "startAfterSeconds" integer,
        "expireInSeconds" integer,
        "keepInSeconds" integer
      )
      ON CONFLICT DO NOTHING
    `;
  }

  function createJobsAndUpdateCursor(props: { jobs: Job[]; service_name: string; last_position: number }) {
    const res = combineSQL`
      WITH insert as (
        ${createJobs(props.jobs)}
      ) ${sql`
        UPDATE {{schema}}.cursors 
        SET l_p = ${props.last_position} 
        WHERE svc = ${props.service_name}
      `}
    `;

    const cmd = sql(res.sqlFragments, ...res.parameters);

    return cmd;
  }

  function getLastEvent() {
    return sql<{ id: string; position: string }>`
      SELECT 
        id,
        pos as position
      FROM {{schema}}.events
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

  function createEvents(events: Event[]) {
    return sql`
      INSERT INTO {{schema}}.events (
        event_name,
        event_data
      ) 
      SELECT
        event_name,
        data as event_data
      FROM json_to_recordset(${fastJSON(events)}) as x(
        event_name text,
        data jsonb
      )
    `;
  }

  function getEvents(service_name: string, options = { limit: 100 }) {
    const events = sql<{ id: string; event_name: string; event_data: any; position: string }>`
    WITH cursor AS (
      SELECT 
        l_p 
      FROM {{schema}}.cursors 
      WHERE svc = ${service_name}
      LIMIT 1
      FOR UPDATE
    ) SELECT 
        id, 
        event_name, 
        event_data,
        pos as position
      FROM {{schema}}.events, cursor
      WHERE pos > cursor.l_p
      ORDER BY pos ASC
      LIMIT ${options.limit}`;

    return events;
  }

  return {
    createJobs,
    createEvents,
    ensureServicePointer,
    getEvents,
    createJobsAndUpdateCursor,
    getLastEvent,
  };
};

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

  const toJob = (task: Task, trigger: TaskTrigger): Job => {
    const options = Object.assign<TaskOptions, Partial<TaskOptions>, Partial<TaskOptions>>(
      {
        retryBackoff: false,
        retryDelay: 3,
        retryLimit: 3,
        startAfterSeconds: 0,
        expireInSeconds: 60 * 5, // 5 minutes
        keepInSeconds: 7 * 24 * 60 * 60,
      },
      configuration.tasks ?? {},
      task.options
    );

    return {
      data: {
        data: task.data,
        tn: task.task_name,
        trace: trigger,
      },
      name: getTaskName(task.task_name),
      ...options,
    };
  };

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
  // TODO: convert getEvents & createJobsAndUpdateCursor into single CTE for more performance and less locking
  const fanoutWorker = createBaseWorker(
    async () => {
      // start transaction
      const newTasks = await withTransaction(pool, async (client) => {
        const events = await query(client, plans.getEvents(serviceName), { name: 'getEvents' });

        if (events.length === 0) {
          return false;
        }

        const jobs = events
          .map((event) => {
            return Array.from(eventHandlersMap.values()).reduce((agg, curr) => {
              if (curr.def.event_name !== event.event_name) {
                return agg;
              }

              const task: Job = toJob(
                {
                  data: event.event_data,
                  task_name: curr.task_name,
                  options: curr.taskOptions,
                },
                {
                  type: 'event',
                  e: { id: event.id, name: event.event_name, p: +event.position },
                }
              );

              return [...agg, task];
            }, [] as Job[]);
          })
          .flat();

        await query(
          client,
          plans.createJobsAndUpdateCursor({
            jobs,
            last_position: +events[events.length - 1]!.position,
            service_name: serviceName,
          }),
          {
            name: 'createJobsAndUpdateCursor',
          }
        );

        return jobs.length > 0;
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

  return {
    registerTask,
    registerHandler,
    start,
    publish: async (events: Event<string, any> | Event<string, any>[], client?: PGClient) => {
      const _events = Array.isArray(events) ? events : [events];
      await query(client ?? pool, plans.createEvents(_events));

      // check if instance is affected by the published events
      const allRegisteredEvents = Array.from(eventHandlersMap.values());
      const hasEffectToCurrentWorker = _events.some((e) =>
        allRegisteredEvents.some((ee) => ee.def.event_name === e.event_name)
      );
      if (hasEffectToCurrentWorker) {
        notifyFanout();
      }
    },
    send: async (tasks: Task | Task[], client?: PGClient) => {
      const _tasks = Array.isArray(tasks) ? tasks : [tasks];
      await query(client ?? pool, plans.createJobs(_tasks.map((task) => toJob(task, directTrigger))));

      // check if instance is affected by the new tasks
      const hasEffectToCurrentWorker = _tasks.some((t) => taskHandlersMap.has(t.task_name));
      if (taskWorker && hasEffectToCurrentWorker) {
        notifyWorker(taskWorker);
      }
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
