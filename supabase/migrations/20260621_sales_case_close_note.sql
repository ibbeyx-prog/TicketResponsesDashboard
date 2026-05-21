-- Optional note when admin closes a sales case.

alter table if exists public.dashboard_sales_cases
  add column if not exists close_note text;

notify pgrst, 'reload schema';
