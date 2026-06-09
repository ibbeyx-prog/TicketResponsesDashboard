-- Optional second field engineer on CSM tickets and Sales cases.

alter table public.tickets_active
  add column if not exists assigned_to_2 text;

alter table public.dashboard_sales_cases
  add column if not exists assigned_to_2 text;

notify pgrst, 'reload schema';
