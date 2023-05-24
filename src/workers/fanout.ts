import { Pool } from 'pg';
import { createSql, query, withTransaction } from '../sql';
import { createBaseWorker } from './base';
import { EventHandler } from '../definitions';
import { createMessagePlans, InsertTask, TaskFactory } from '../messages';

type _Event = { id: string; event_name: string; event_data: any; position: string };

const createPlans = (schema: string) => {
  const sql = createSql(schema);
  const taskSql = createMessagePlans(schema);

  function createTasks(tasks: InsertTask[]) {
    return taskSql.createTasks(tasks);
  }

  function updateServiceCursor(service_name: string, last_position: number) {
    return sql`
      UPDATE {{schema}}.cursors 
      SET l_p = ${last_position} 
      WHERE svc = ${service_name}
    `;
  }

  function getCursorLockEvents(service_name: string, options: { limit: number }) {
    const events = sql<_Event>`
      SELECT 
        id, 
        event_name, 
        event_data,
        pos as position
      FROM {{schema}}.events
      -- pos > 0 needed for index scan
      WHERE pos > 0 
      AND pos > (
        SELECT 
          l_p as cursor
        FROM {{schema}}.cursors 
        WHERE svc = ${service_name}
        LIMIT 1
        FOR UPDATE
        SKIP LOCKED
      )
      ORDER BY pos ASC
      LIMIT ${options.limit}`;

    return events;
  }

  return {
    getCursorLockEvents,
    // getEvents,
    createTasks,
    updateServiceCursor,
  };
};

const createEventToTasks =
  (eventHandlers: EventHandler<string, any>[], taskFactory: TaskFactory) => (event: _Event) => {
    return eventHandlers.reduce((agg, curr) => {
      if (curr.def.event_name !== event.event_name) {
        return agg;
      }

      const config = typeof curr.config === 'function' ? curr.config(event.event_data) : curr.config;

      agg.push(
        taskFactory(
          {
            data: event.event_data,
            task_name: curr.task_name,
            config: config,
          },
          {
            type: 'event',
            e: { id: event.id, name: event.event_name, p: +event.position },
          }
        )
      );

      return agg;
    }, [] as InsertTask[]);
  };

export const createFanoutWorker = (props: {
  pool: Pool;
  serviceName: string;
  schema: string;
  getEventHandlers: () => EventHandler<string, any>[];
  onNewTasks: () => void;
  taskFactory: TaskFactory;
}) => {
  const plans = createPlans(props.schema);
  const fetchSize = 100;
  // worker which responsible for creating tasks from incoming integrations events
  const fanoutWorker = createBaseWorker(
    async () => {
      const handlers = props.getEventHandlers();
      if (handlers.length === 0) {
        return false;
      }

      const eventToTasks = createEventToTasks(handlers, props.taskFactory);
      // start transaction
      const trxResult = await withTransaction<{ hasMore: boolean; hasChanged: boolean }>(props.pool, async (client) => {
        const events = await query(client, plans.getCursorLockEvents(props.serviceName, { limit: fetchSize }));

        if (events.length === 0) {
          return { hasChanged: false, hasMore: false };
        }

        const newCursor = +events[events.length - 1]!.position;
        const tasks = events.map(eventToTasks).flat();

        await query(client, plans.createTasks(tasks));
        await query(client, plans.updateServiceCursor(props.serviceName, newCursor));

        return {
          hasChanged: tasks.length > 0,
          hasMore: events.length === fetchSize,
        };
      });

      if (trxResult.hasChanged) {
        props.onNewTasks();
      }

      return trxResult.hasMore;
    },
    { loopInterval: 1500 }
  );

  return fanoutWorker;
};
