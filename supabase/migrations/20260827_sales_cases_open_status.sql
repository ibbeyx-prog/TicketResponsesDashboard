-- Resort cases move Sales ticket → Open (Needs Review) after field response,
-- same as tickets_active. The original sales_cases_status_check omitted Open.

alter table public.dashboard_sales_cases
  drop constraint if exists sales_cases_status_check;

alter table public.dashboard_sales_cases
  add constraint sales_cases_status_check
  check (status in (
    'Sales ticket',
    'Open',
    'Investigation',
    'Regional for site visit',
    'Design',
    'Resolved'
  ));

notify pgrst, 'reload schema';
