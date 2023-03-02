import { Static, TSchema } from '@sinclair/typebox';
import { ValidateFunction } from 'ajv';
import { createValidateFn } from './schema';

export interface Event<Name = string, Data = {}> {
  event_name: Name;
  data: Data;
}

export type TaskTrigger =
  | {
      /**
       * Directly scheduled task
       */
      type: 'direct';
    }
  | {
      type: 'event';
      /**
       * Triggered by event
       */
      e: {
        /**
         * Event id
         */
        id: string;
        /**
         * Event name
         */
        name: string;
        /**
         * Event position
         */
        p: number;
      };
    };

export interface EventSpec<Name extends string, Schema extends TSchema> {
  /**
   * Event name.
   * It is wisely to prefix with servicename abbr
   */
  event_name: Name;
  /**
   * Typebox schema of the payload
   */
  schema: Schema;
}

export interface EventDefinition<Name extends string, Schema extends TSchema> {
  event_name: Name;
  schema: Schema;
  validate: ValidateFunction<Static<Schema, []>>;
  from: (input: Static<Schema>) => Event<Name, Static<Schema>>;
}

/**
 * Define an integration event.  Task name should be unique for a pg-tbus instance
 */
export const defineEvent = <TName extends string, T extends TSchema>(
  spec: EventSpec<TName, T>
): EventDefinition<TName, T> => {
  const { event_name, schema } = spec;
  const validate = createValidateFn(schema);

  function from(input: Static<T>): Event<TName, Static<T>> {
    const validateFn = validate;
    if (!validateFn(input)) {
      throw new Error(`invalid input for event ${event_name}: ${validateFn.errors?.map((e) => e.message).join(' \n')}`);
    }
    return {
      data: input,
      event_name: event_name,
    };
  }

  return {
    event_name,
    schema,
    validate,
    from,
  };
};

export interface TaskDefinition<T extends TSchema> {
  task_name: string;
  schema: T;
  config: Partial<TaskConfig>;
  validate: ValidateFunction<Static<T, []>>;
  handler: TaskHandler<Static<T>>;
  from: (input: Static<T>, config?: Partial<TaskConfig>) => Task<Static<T>>;
}

export interface Task<Data = {}> {
  task_name: string;
  data: Data;
  config: Partial<TaskConfig>;
}

export interface TaskHandler<Input> {
  (props: { name: string; input: Input; trigger: TaskTrigger }): Promise<any>;
}

export const defaultTaskConfig: TaskConfig = {
  retryBackoff: false,
  retryDelay: 5,
  retryLimit: 3,
  startAfterSeconds: 0,
  expireInSeconds: 60 * 5, // 5 minutes
  keepInSeconds: 7 * 24 * 60 * 60,
};

export type TaskConfig = {
  /**
   * Amount of times the task is retried, default 3
   */
  retryLimit: number;
  /**
   * Delay between retries of failed jobs, in seconds. Default 5
   */
  retryDelay: number;
  /**
   * Expentional retrybackoff, default false
   */
  retryBackoff: boolean;
  /**
   * Start after n seconds, default 0
   */
  startAfterSeconds: number;
  /**
   * How many seconds a job may be in active state before it is failed because of expiration. Default 60 * 5 (5minutes)
   */
  expireInSeconds: number;
  /**
   * How long job is hold in the jobs table before it is archieved. Default 7 * 24 * 60 * 60 (7 days)
   */
  keepInSeconds: number;
};

/**
 * Define a standalone task.
 */
export const defineTask = <T extends TSchema>(props: {
  /**
   * Task name
   */
  task_name: string;
  /**
   * Task payload schema
   */
  schema: T;
  /**
   * Task handler
   */
  handler: TaskHandler<Static<T>>;
  /**
   * Task configuration
   */
  config?: Partial<TaskConfig>;
}): TaskDefinition<T> => {
  const validateFn = createValidateFn(props.schema);

  const from: TaskDefinition<T>['from'] = function from(input, config) {
    if (!validateFn(input)) {
      throw new Error(
        `invalid input for task ${props.task_name}: ${validateFn.errors?.map((e) => e.message).join(' \n')}`
      );
    }
    return {
      data: input,
      task_name: props.task_name,
      config: { ...config, ...props.config },
    };
  };

  return {
    schema: props.schema,
    task_name: props.task_name,
    validate: validateFn,
    handler: props.handler,
    from,
    // specifiy some defaults
    config: props.config ?? {},
  };
};

export interface EventHandler<TName extends string, T extends TSchema> {
  task_name: string;
  def: EventDefinition<TName, T>;
  handler: TaskHandler<Static<T>>;
  config: Partial<TaskConfig> | ((input: Static<T>) => Partial<TaskConfig>);
}

/**
 * Create an event handler from an event definition. Task name should be unique for a pg-tbus instance
 */
export const createEventHandler = <TName extends string, T extends TSchema>(props: {
  /**
   * Task queue name. Should be unique per pg-tbus instance
   */
  task_name: string;
  /**
   * Event definitions
   */
  eventDef: EventDefinition<TName, T>;
  /**
   * Event handler
   */
  handler: TaskHandler<Static<T>>;
  /**
   * Event handler configuration. Can be static or a function
   */
  config?: Partial<TaskConfig> | ((input: Static<T>) => Partial<TaskConfig>);
}): EventHandler<TName, T> => {
  return {
    task_name: props.task_name,
    def: props.eventDef,
    handler: props.handler,
    config: typeof props.config === 'function' ? props.config : { ...props.config },
  };
};
