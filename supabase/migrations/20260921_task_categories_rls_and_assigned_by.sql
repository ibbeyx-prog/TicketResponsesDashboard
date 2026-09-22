-- Fix Supabase log noise from dashboard category sync + missing assignee column.
--
-- Symptoms (anon / publishable key):
--   42501 new row violates row-level security … dashboard_task_categories
--   42703 column tickets_active.dashboard_assigned_by does not exist
--
-- Category sync uses UPSERT; without anon UPDATE, conflicts fail RLS on the update leg.
-- Manage Categories also updates sort_order — needs anon UPDATE.

alter table public.tickets_active
  add column if not exists dashboard_assigned_by text;

alter table public.dashboard_task_categories enable row level security;

drop policy if exists "dashboard_task_categories_anon_select"
  on public.dashboard_task_categories;
drop policy if exists "dashboard_task_categories_anon_insert"
  on public.dashboard_task_categories;
drop policy if exists "dashboard_task_categories_anon_update"
  on public.dashboard_task_categories;
drop policy if exists "dashboard_task_categories_anon_delete"
  on public.dashboard_task_categories;

create policy "dashboard_task_categories_anon_select"
  on public.dashboard_task_categories
  for select
  to anon
  using (true);

create policy "dashboard_task_categories_anon_insert"
  on public.dashboard_task_categories
  for insert
  to anon
  with check (true);

create policy "dashboard_task_categories_anon_update"
  on public.dashboard_task_categories
  for update
  to anon
  using (true)
  with check (true);

create policy "dashboard_task_categories_anon_delete"
  on public.dashboard_task_categories
  for delete
  to anon
  using (true);

notify pgrst, 'reload schema';
