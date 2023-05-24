ALTER TABLE  {{schema}}."tasks" ADD COLUMN "singleton_key" text default null;
-- 0: create, 1: retry, 2: active, 3 >= all completed/failed
CREATE UNIQUE INDEX idx_unique_queue_task ON {{schema}}."tasks" ("queue", "singleton_key") WHERE state < 3;