import { defaultTaskConfig, Task, TaskConfig, TaskTrigger, Event } from './definitions';
import { createSql } from './sql';

export type TaskName = string;

export const TASK_STATES = {
  created: 0,
  retry: 1,
  active: 2,
  completed: 3,
  expired: 4,
  cancelled: 5,
  failed: 6,
} as const;

export type TaskState = (typeof TASK_STATES)[keyof typeof TASK_STATES];

export type TaskDTO<T> = { tn: string; data: T; trace: TaskTrigger };

export type InsertTask<T = object> = {
  /**
   * Queue
   */
  q: string;
  /**
   * Data
   */
  d: TaskDTO<T>;
  /**
   * Task state
   */
  s?: TaskState;
  /**
   * Retry limit
   */
  r_l: number;
  /**
   * Retry delay
   */
  r_d: number;
  /**
   * Retry backoff
   */
  r_b: boolean;
  /**
   * Start after seconds
   */
  saf: number;
  /**
   * Expire in seconds
   */
  eis: number;
  /**
   * Keep until in seconds
   */
  kis: number;
  /**
   * Singleton key
   */
  skey: string | null;
};

export type SelectTask<T = object> = {
  /**
   * Bigint
   */
  id: string;
  retrycount: number;
  state: number;
  data: TaskDTO<T>;
  expire_in_seconds: number;
};

export const createMessagePlans = (schema: string) => {
  const sql = createSql(schema);
  function createTasks(tasks: InsertTask[]) {
    return sql<{}>`SELECT {{schema}}.create_bus_tasks(${JSON.stringify(tasks)}::jsonb)`;
  }

  function createEvents(events: Event[]) {
    return sql`SELECT {{schema}}.create_bus_events(${JSON.stringify(events)}::jsonb)`;
  }

  return {
    createTasks,
    createEvents,
    getTasks: (props: { queue: string; amount: number }) => sql<SelectTask>`
    with _tasks as (
      SELECT
        id
      FROM {{schema}}.tasks
      WHERE queue = ${props.queue}
        AND startAfter < now()
        AND state < ${TASK_STATES.active}
      ORDER BY createdOn ASC
      LIMIT ${props.amount}
      FOR UPDATE SKIP LOCKED
    ) UPDATE {{schema}}.tasks t 
      SET
        state = ${TASK_STATES.active}::smallint,
        startedOn = now(),
        retryCount = CASE WHEN state = ${TASK_STATES.retry} 
                      THEN retryCount + 1 
                      ELSE retryCount END
      FROM _tasks
      WHERE t.id = _tasks.id
      RETURNING t.id, t.retryCount, t.state, t.data, 
        (EXTRACT(epoch FROM expireIn))::int as expire_in_seconds
    `,
    resolveTasks: (tasks: Array<{ t: string; p: string; s: boolean }>) => sql`
    WITH _in as (
      SELECT 
        x.t as task_id,
        x.s as success,
        x.p as payload
      FROM json_to_recordset(${JSON.stringify(tasks)}) as x(
        t bigint,
        s boolean,
        p jsonb
      )
    ), _failed as (
      UPDATE {{schema}}.tasks t
      SET
        state = CASE
          WHEN retryCount < retryLimit THEN ${TASK_STATES.retry}::smallint ELSE ${TASK_STATES.failed}::smallint END,
        completedOn = CASE WHEN retryCount < retryLimit THEN NULL ELSE now() END,
        startAfter = CASE
                      WHEN retryCount = retryLimit THEN startAfter
                      WHEN NOT retryBackoff THEN now() + retryDelay * interval '1'
                      ELSE now() +
                        (
                            retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2
                            +
                            retryDelay * 2 ^ LEAST(16, retryCount + 1) / 2 * random()
                        )
                        * interval '1'
                      END,
        output = _in.payload
      FROM _in
      WHERE t.id = _in.task_id
        AND _in.success = false
        AND t.state < ${TASK_STATES.completed}
    ) UPDATE {{schema}}.tasks t
      SET
        state = ${TASK_STATES.completed}::smallint,
        completedOn = now(),
        output = _in.payload
      FROM _in
      WHERE t.id = _in.task_id
        AND _in.success = true
        AND t.state = ${TASK_STATES.active}
    `,
  };
};

export type TaskFactory = ReturnType<typeof createTaskFactory>;

export const createTaskFactory =
  (props: { taskConfig: Partial<TaskConfig>; queue: string }) =>
  (task: Task, trigger: TaskTrigger): InsertTask => {
    const config: TaskConfig = {
      ...defaultTaskConfig,
      ...props.taskConfig,
      ...task.config,
    };

    return {
      d: {
        data: task.data,
        tn: task.task_name,
        trace: trigger,
      },
      q: task.queue ?? props.queue,
      eis: config.expireInSeconds,
      kis: config.keepInSeconds,
      r_b: config.retryBackoff,
      r_d: config.retryDelay,
      r_l: config.retryLimit,
      saf: config.startAfterSeconds,
      skey: config.singletonKey,
    };
  };
