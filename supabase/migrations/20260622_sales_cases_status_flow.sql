-- Sales cases queue: Sales ticket → Investigation → Regional → Design → Resolved

update public.dashboard_sales_cases
set status = 'Sales ticket'
where status = 'Sales intake';

update public.dashboard_sales_cases
set status = 'Investigation'
where status in ('Admin triage', 'System check', 'Dispatch approved');

update public.dashboard_sales_cases
set status = 'Regional for site visit'
where status in ('Awaiting field', 'Field in progress');

update public.dashboard_sales_cases
set status = 'Design'
where status = 'Admin review';

update public.dashboard_sales_cases
set status = 'Resolved'
where status = 'Closed';

notify pgrst, 'reload schema';
