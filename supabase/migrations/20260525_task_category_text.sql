-- Allow dynamic categories from ``dashboard_task_categories`` (not a fixed enum).
--
-- ``tickets_active.task_category`` was type ``task_type`` (Postgres enum). New
-- categories added in the dashboard were accepted by the bot regex but INSERT/
-- UPDATE failed with: invalid input value for enum task_type.

alter table public.tickets_active
  alter column task_category type text using task_category::text;

drop type if exists public.task_type;
