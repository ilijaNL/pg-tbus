import { Pool } from 'pg';
import { combineSQL, createSql, query, withTransaction } from '../sql';
import { createBaseWorker } from './base';
import { EventHandler } from '../definitions';
import { createMessagePlans, Job, JobFactory } from '../messages';

const createPlans = (schema: string) => {
  const sql = createSql(schema);
  const taskSql = createMessagePlans(schema);

  function createJobsAndUpdateCursor(props: { jobs: Job[]; service_name: string; last_position: number }) {
    const res = combineSQL`
      WITH insert as (
        ${taskSql.createJobs(props.jobs)}
      ) ${sql`
        UPDATE {{schema}}.cursors 
        SET l_p = ${props.last_position} 
        WHERE svc = ${props.service_name}
      `}
    `;

    const cmd = sql(res.sqlFragments, ...res.parameters);

    return cmd;
  }

  function getCursorAndLock(service_name: string) {
    return sql<{ cursor: string }>`
      SELECT 
        l_p as cursor
      FROM {{schema}}.cursors 
      WHERE svc = ${service_name}
      LIMIT 1
      FOR UPDATE
      SKIP LOCKED
    `;
  }

  function getEvents(offset: number, options: { limit: number }) {
    const events = sql<{ id: string; event_name: string; event_data: any; position: string }>`
      SELECT 
        id, 
        event_name, 
        event_data,
        pos as position
      FROM {{schema}}.events
      WHERE pos > ${offset}
      ORDER BY pos ASC
      LIMIT ${options.limit}`;

    return events;
  }

  return {
    getCursorAndLock,
    getEvents,
    createJobsAndUpdateCursor,
  };
};

export const createFanoutWorker = (props: {
  pool: Pool;
  serviceName: string;
  schema: string;
  getEventHandlers: () => EventHandler<string, any>[];
  onNewTasks: () => void;
  jobFactory: JobFactory;
}) => {
  const plans = createPlans(props.schema);
  // worker which responsible for creating tasks from incoming integrations events
  // TODO: convert getEvents & createJobsAndUpdateCursor into single CTE for more performance and less locking
  const fanoutWorker = createBaseWorker(
    async () => {
      // start transaction
      const newTasks = await withTransaction(props.pool, async (client) => {
        const cursor = await query(client, plans.getCursorAndLock(props.serviceName), { name: 'getCursor' }).then(
          (d) => d[0]?.cursor
        );

        if (!cursor) {
          return false;
        }

        const events = await query(client, plans.getEvents(+cursor, { limit: 100 }), { name: 'getEvents' });

        if (events.length === 0) {
          return false;
        }

        const jobs = events
          .map((event) => {
            return props.getEventHandlers().reduce((agg, curr) => {
              if (curr.def.event_name !== event.event_name) {
                return agg;
              }

              const task: Job = props.jobFactory(
                {
                  data: event.event_data,
                  task_name: curr.task_name,
                  config: curr.config,
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
            service_name: props.serviceName,
          }),
          {
            name: 'createJobsAndUpdateCursor',
          }
        );

        return jobs.length > 0;
      });

      if (newTasks) {
        props.onNewTasks();
      }

      return false;
    },
    { loopInterval: 1000 }
  );

  return fanoutWorker;
};
