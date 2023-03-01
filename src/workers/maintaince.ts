import { createSql, PGClient, query } from '../sql';
import { createBaseWorker } from './base';

const createPlans = (schema: string) => {
  const sql = createSql(schema);

  return {
    deleteOldEvents: (days: number) => sql`
      DELETE FROM {{schema}}.events WHERE created_at < (now() - interval '1 day' * ${days})
    `,
  };
};

export const createMaintainceWorker = (props: { schema: string; client: PGClient; retentionInDays: number }) => {
  const plans = createPlans(props.schema);
  const worker = createBaseWorker(
    async () => {
      await query(props.client, plans.deleteOldEvents(props.retentionInDays));
    },
    { loopInterval: 30000 }
  );

  return worker;
};
