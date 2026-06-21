-- Field engineer reply columns on sales cases (mirror tickets_active for bot capture).

alter table if exists public.dashboard_sales_cases
  add column if not exists field_response text,
  add column if not exists photo_url text,
  add column if not exists field_responded_by text,
  add column if not exists responded_at timestamptz;

notify pgrst, 'reload schema';
