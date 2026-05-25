-- Replace sales_owner with attended_by (Mular_s | Ibbe).

alter table if exists public.dashboard_sales_cases
  add column if not exists attended_by text;

update public.dashboard_sales_cases
set attended_by = case
  when trim(coalesce(sales_owner, '')) ilike 'ibbe%' then 'Ibbe'
  when trim(coalesce(sales_owner, '')) ilike 'mular%' then 'Mular_s'
  else 'Mular_s'
end
where attended_by is null or trim(attended_by) = '';

alter table if exists public.dashboard_sales_cases
  alter column attended_by set default 'Mular_s';

alter table if exists public.dashboard_sales_cases
  alter column attended_by set not null;

alter table if exists public.dashboard_sales_cases
  drop column if exists sales_owner;

notify pgrst, 'reload schema';
