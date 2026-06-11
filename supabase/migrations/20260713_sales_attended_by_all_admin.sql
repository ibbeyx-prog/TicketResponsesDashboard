-- Sales row ``attended_by`` = **Admin** (admin queue). Performance credits ``assigned_to`` when set.

update public.dashboard_sales_cases
set
  attended_by = 'Admin',
  updated_at = now()
where lower(trim(replace(coalesce(attended_by, ''), '@', ''))) <> 'admin';

notify pgrst, 'reload schema';
