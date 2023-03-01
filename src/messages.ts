import { defaultTaskConfig, Task, TaskConfig, TaskTrigger, Event } from './definitions';
import { createSql } from './sql';
import fastJSON from 'fast-json-stable-stringify';

export type TaskName = string;

export type TaskDTO<T> = { tn: string; data: T; trace: TaskTrigger };

export type Job<T = object> = {
  name: string;
  data: TaskDTO<T>;
} & TaskConfig;

export const createMessagePlans = (schema: string) => {
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
    createJobs,
    createEvents,
  };
};

export type JobFactory = ReturnType<typeof createJobFactory>;

export const createJobFactory =
  (props: { taskConfig: Partial<TaskConfig>; getTaskName: (name: string) => string }) =>
  (task: Task, trigger: TaskTrigger): Job => {
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
      name: props.getTaskName(task.task_name),
      ...config,
    };
  };
