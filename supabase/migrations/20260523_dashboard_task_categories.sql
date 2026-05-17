-- Task categories for assignments (dashboard + Telegram bot regex).

create table if not exists public.dashboard_task_categories (
  name text primary key,
  sort_order int not null default 0,
  created_at timestamptz not null default now()
);

create unique index if not exists dashboard_task_categories_name_lower_idx
  on public.dashboard_task_categories (lower(name));

alter table public.dashboard_task_categories enable row level security;

drop policy if exists "dashboard_task_categories_anon_select"
  on public.dashboard_task_categories;
drop policy if exists "dashboard_task_categories_anon_insert"
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

create policy "dashboard_task_categories_anon_delete"
  on public.dashboard_task_categories
  for delete
  to anon
  using (true);

insert into public.dashboard_task_categories (name, sort_order) values
  ('Coverage Check', 10),
  ('Femto Installation', 20),
  ('Repeater Installation', 30),
  ('Femto Recover', 40),
  ('Femto Fault', 50),
  ('Repeater Fault', 60)
on conflict (name) do nothing;
