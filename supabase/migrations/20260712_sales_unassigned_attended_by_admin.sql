-- Unassigned sales cases (admin-handled, no field engineer) → Overview label **Admin**.

update public.dashboard_sales_cases
set
  attended_by = 'Admin',
  updated_at = now()
where assigned_to is null or trim(assigned_to) = '';

notify pgrst, 'reload schema';
