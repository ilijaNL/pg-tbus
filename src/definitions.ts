import { Static, TSchema } from '@sinclair/typebox';
import { ValidateFunction } from 'ajv';
import { createValidateFn } from './schema';

export interface Event<Name = string, Data = {}> {
  event_name: Name;
  data: Data;
}

export type TaskTrigger =
  | {
      type: 'direct';
    }
  | { type: 'event'; e_id: string; e_name: string };

export interface EventSpec<Name extends string, Schema extends TSchema> {
  event_name: Name;
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
  options: Partial<TaskOptions>;
  validate: ValidateFunction<Static<T, []>>;
  handler: TaskHandler<Static<T>>;
  from: (input: Static<T>) => Task<Static<T>>;
}

export interface Task<Data = {}> {
  task_name: string;
  data: Data;
  options: Partial<TaskOptions>;
}

export interface TaskHandler<Input> {
  (props: { name: string; input: Input; trigger: TaskTrigger }): Promise<any>;
}

export type TaskOptions = {
  retryLimit: number;
  retryDelay: number;
  retryBackoff: boolean;
  startAfterSeconds: number;
  expireInSeconds: number;
  keepInSeconds: number;
};

/**
 * Define a standalone task.
 */
export const defineTask = <T extends TSchema>(props: {
  task_name: string;
  schema: T;
  handler: TaskHandler<Static<T>>;
  options?: Partial<TaskOptions>;
}): TaskDefinition<T> => {
  const validateFn = createValidateFn(props.schema);

  function from(input: Static<T>): Task<Static<T>> {
    if (!validateFn(input)) {
      throw new Error(
        `invalid input for task ${props.task_name}: ${validateFn.errors?.map((e) => e.message).join(' \n')}`
      );
    }
    return {
      data: input,
      task_name: props.task_name,
      options: props.options ?? {},
    };
  }

  return {
    schema: props.schema,
    task_name: props.task_name,
    validate: validateFn,
    handler: props.handler,
    from,
    // specifiy some defaults
    options: props.options ?? {},
  };
};

export interface EventHandler<TName extends string, T extends TSchema> {
  task_name: string;
  def: EventDefinition<TName, T>;
  handler: TaskHandler<Static<T>>;
  taskOptions: Partial<TaskOptions>;
}

/**
 * Create an event handler from an event definition. Task name should be unique for a pg-tbus instance
 */
export const createEventHandler = <TName extends string, T extends TSchema>(props: {
  task_name: string;
  eventDef: EventDefinition<TName, T>;
  handler: TaskHandler<Static<T>>;
  options?: Partial<TaskOptions>;
}): EventHandler<TName, T> => {
  return {
    task_name: props.task_name,
    def: props.eventDef,
    handler: props.handler,
    taskOptions: props.options ?? {},
  };
};
