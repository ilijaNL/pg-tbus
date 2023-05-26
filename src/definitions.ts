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

export interface EventHandler<TName extends string, T extends TSchema> {
  task_name: string;
  def: EventDefinition<TName, T>;
  handler: TaskHandler<Static<T>>;
  config: Partial<TaskConfig> | ((input: Static<T>) => Partial<TaskConfig>);
}

export interface DeclareTaskProps<T extends TSchema> {
  /**
   * Task name
   */
  task_name: string;
  /**
   * Queue this task belongs to.
   * If not specified, the queue will be set to service when task is send from a tbus instance.
   */
  queue?: string;
  /**
   * Task payload schema
   */
  schema: T;
  /**
   * Default task configuration. Can be (partially) override when creating the task
   */
  config?: Partial<TaskConfig>;
}

export interface TaskDeclaration<T extends TSchema> extends DeclareTaskProps<T> {
  validate: ValidateFunction<Static<T, []>>;
  from: (input: Static<T>, config?: Partial<TaskConfig>) => Task<Static<T>>;
}

export interface TaskDefinition<T extends TSchema> extends TaskDeclaration<T> {
  handler: TaskHandler<Static<T>>;
  config: Partial<TaskConfig>;
}

export interface Task<Data = {}> {
  task_name: string;
  queue?: string;
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
  singletonKey: null,
};

export type TaskConfig = {
  /**
   * Amount of times the task is retried, default 3
   */
  retryLimit: number;
  /**
   * Delay between retries of failed tasks, in seconds. Default 5
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
   * How many seconds a task may be in active state before it is failed because of expiration. Default 60 * 5 (5minutes)
   */
  expireInSeconds: number;
  /**
   * How long task is hold in the tasks table before it is archieved. Default 7 * 24 * 60 * 60 (7 days)
   */
  keepInSeconds: number;
  /**
   * A singleton key which can be used to have an unique active task in a queue.
   */
  singletonKey: string | null;
};

export const declareTask = <T extends TSchema>(props: DeclareTaskProps<T>): TaskDeclaration<T> => {
  const validateFn = createValidateFn(props.schema);

  const from: TaskDeclaration<T>['from'] = function from(input, config) {
    if (!validateFn(input)) {
      throw new Error(
        `invalid input for task ${props.task_name}: ${validateFn.errors?.map((e) => e.message).join(' \n')}`
      );
    }

    return {
      queue: props.queue,
      task_name: props.task_name,
      data: input,
      config: { ...props.config, ...config },
    };
  };

  return {
    schema: props.schema,
    task_name: props.task_name,
    validate: validateFn,
    from,
    // specifiy some defaults
    config: props.config ?? {},
  };
};

/**
 * Define a standalone task.
 */
export const defineTask = <T extends TSchema>(
  props: (DeclareTaskProps<T> | TaskDeclaration<T>) & {
    handler: TaskHandler<Static<T>>;
  }
): TaskDefinition<T> => {
  if ('from' in props) {
    return {
      ...props,
      handler: props.handler,
      config: props.config ?? {},
    };
  }

  const decl = declareTask(props);

  return {
    schema: props.schema,
    task_name: props.task_name,
    validate: decl.validate,
    handler: props.handler,
    from: decl.from,
    // specifiy some defaults
    config: props.config ?? {},
  };
};

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
