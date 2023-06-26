# PG-TBus

End-to-end typesafe tasks and integration events on postgres made easy.

[![codecov](https://codecov.io/gh/ilijaNL/pg-tbus/branch/master/graph/badge.svg?token=08H5Z8ZL40)](https://codecov.io/gh/ilijaNL/pg-tbus)

## Usage

1. Define integration events

```typescript
const account_created_event = defineEvent({
  event_name: 'account_created',
  schema: Type.Object({
    account_id: Type.String({ format: 'uuid' }),
  }),
});
```

2. Initialize pg-tbus

```typescript
const bus = createTBus('my_service', { db: { connectionString: connectionString }, schema: schema });
```

3. Register eventHandler

```typescript
bus.registerHandler(
  createEventHandler({
    // should be unique for this bus handler
    task_name: 'send_email',
    eventDef: account_created_event,
    handler: async (props) => {
      // do something with the data
    },
  })
);
```

4. Start the pg-tbus

```typescript
bus.start();
```

5. Emit the event

```typescript
await bus.publish(account_created_event.from({ account_id: '1234' }));
```

## Tasks

1. Define task(s)

```typescript
const send_email_task = defineTask({
  task_name: 'send_email',
  queue: 'email_svc',
  schema: Type.Object({ email: Type.String() }),
});
```

2. Register task

```typescript
bus.registerTask(
  createTaskHandler({
    taskDef: send_email_task,
    handler: async (props) => {
      //send email to props.input.email
    },
  })
);
```

3. Create task from somewhere else

```ts
await bus.send(send_email_task.from({ email: 'test@test.com' }));
```

For more usage, see `tests/bus.ts`
