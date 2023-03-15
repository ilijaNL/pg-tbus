import { defaultTaskConfig, Task, TaskConfig, TaskTrigger, Event } from './definitions';
import { createSql } from './sql';
import fastJSON from 'fast-json-stable-stringify';

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

export type TaskDTO<T> = { tn: string; data: T; trace: TaskTrigger };

export type InsertTask<T = object> = {
  queue: string;
  data: TaskDTO<T>;
} & TaskConfig;

export type SelectTask<T = object> = InsertTask<T> & {
  id: string;
  retrycount: number;
  state: number;
  expireInSeconds: number;
};

export const createMessagePlans = (schema: string) => {
  const sql = createSql(schema);
  function createTasks(tasks: InsertTask[]) {
    return sql<{}>`
      INSERT INTO {{schema}}.tasks (
        id,
        queue,
        data,
        retryLimit,
        retryDelay,
        retryBackoff,
        startAfter,
        expireIn,
        keepUntil
      )
      SELECT
        gen_random_uuid() as id,
        queue,
        data,
        "retryLimit",
        "retryDelay",
        "retryBackoff",
        (now() + ("startAfterSeconds" * interval '1s'))::timestamp with time zone as startAfter,
        "expireInSeconds" * interval '1s' as expireIn,
        (now() + ("startAfterSeconds" * interval '1s') + ("keepInSeconds" * interval '1s'))::timestamp with time zone as keepUntil
      FROM json_to_recordset(${fastJSON(tasks)}) as x(
        queue text,
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
      RETURNING t.*, 
        EXTRACT(epoch FROM expireIn) as expireInSeconds
    `,
    resolveTasks: (tasks: Array<{ t: string; p: string; s: boolean }>) => sql`
    WITH _in as (
      SELECT 
        x.t as task_id,
        x.s as success,
        x.p as payload
      FROM json_to_recordset(${fastJSON(tasks)}) as x(
        t uuid,
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

export type taskFactory = ReturnType<typeof createTaskFactory>;

export const createTaskFactory =
  (props: { taskConfig: Partial<TaskConfig>; queue: string }) =>
  (task: Task, trigger: TaskTrigger): InsertTask => {
    const config: TaskConfig = {
      ...defaultTaskConfig,
      ...props.taskConfig,
      ...task.config,
    };

    return {
      data: {
        data: task.data,
        tn: task.task_name,
        trace: trigger,
      },
      queue: props.queue,
      ...config,
    };
  };
