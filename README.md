# PG-TBus

End-to-end typesafe tasks and integration events on postgres made easy.

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

await bus.start();
```

3. Define event and register

```typescript
const handler = createEventHandler({
  task_name: 'send_email',
  eventDef: account_created_event,
  handler: async (props) => {
    // do something with the data
  },
});

bus.registerHandler(handler);
```

4. Emit the event

```typescript
await bus.publish(account_created_event.from({ account_id: '1234' }));
```

For more usage, see `tests/bus.ts`
