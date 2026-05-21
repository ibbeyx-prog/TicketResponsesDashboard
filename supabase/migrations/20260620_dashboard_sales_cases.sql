-- Sales cases (separate from field complaint tickets). Admin-first workflow.

create table if not exists public.dashboard_sales_cases (
  id uuid primary key default gen_random_uuid(),
  case_ref text not null,
  account_name text not null,
  sales_owner text not null,
  sales_priority text not null default 'Standard',
  account_region text not null,
  sales_category text not null,
  description text,
  status text not null default 'Sales ticket',
  admin_owner text,
  dispatch_type text,
  dispatch_region text,
  assigned_to text,
  field_task_category text,
  dispatch_reason text,
  additional_info text,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now()
);

create index if not exists dashboard_sales_cases_status_idx
  on public.dashboard_sales_cases (status);

create index if not exists dashboard_sales_cases_updated_idx
  on public.dashboard_sales_cases (updated_at desc);

alter table if exists public.dashboard_sales_cases enable row level security;

drop policy if exists "dashboard_sales_cases_anon_select" on public.dashboard_sales_cases;
drop policy if exists "dashboard_sales_cases_anon_insert" on public.dashboard_sales_cases;
drop policy if exists "dashboard_sales_cases_anon_update" on public.dashboard_sales_cases;

create policy "dashboard_sales_cases_anon_select"
  on public.dashboard_sales_cases
  for select
  to anon
  using (true);

create policy "dashboard_sales_cases_anon_insert"
  on public.dashboard_sales_cases
  for insert
  to anon
  with check (true);

create policy "dashboard_sales_cases_anon_update"
  on public.dashboard_sales_cases
  for update
  to anon
  using (true)
  with check (true);

notify pgrst, 'reload schema';
