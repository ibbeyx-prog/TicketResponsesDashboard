-- Track when a field engineer was assigned on a sales case (parity with tickets_active).

alter table if exists public.dashboard_sales_cases
  add column if not exists last_assigned_at timestamptz;

update public.dashboard_sales_cases
set last_assigned_at = updated_at
where assigned_to is not null
  and btrim(assigned_to) <> ''
  and last_assigned_at is null;

create index if not exists dashboard_sales_cases_last_assigned_idx
  on public.dashboard_sales_cases (last_assigned_at desc)
  where last_assigned_at is not null;

notify pgrst, 'reload schema';
