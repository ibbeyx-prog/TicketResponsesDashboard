-- Retire "Coverage issue(s)" — canonical field label is **Coverage Check**.

-- Field tickets
update public.tickets_active
set task_category = 'Coverage Check'
where lower(trim(coalesce(task_category, ''))) in ('coverage issue', 'coverage issues');

update public.tickets_active
set outcome_category = 'Coverage Check'
where lower(trim(coalesce(outcome_category, ''))) in ('coverage issue', 'coverage issues');

-- Category picker table
delete from public.dashboard_task_categories
where lower(trim(name)) in ('coverage issue', 'coverage issues');

insert into public.dashboard_task_categories (name, sort_order)
values ('Coverage Check', 10)
on conflict (name) do nothing;

-- Sales cases (if table exists)
do $$
begin
  if exists (
    select 1 from information_schema.tables
    where table_schema = 'public' and table_name = 'dashboard_sales_cases'
  ) then
    update public.dashboard_sales_cases
    set sales_category = 'Coverage Check'
    where lower(trim(coalesce(sales_category, ''))) in ('coverage issue', 'coverage issues');

    update public.dashboard_sales_cases
    set field_task_category = 'Coverage Check'
    where lower(trim(coalesce(field_task_category, ''))) in ('coverage issue', 'coverage issues');
  end if;
end $$;

notify pgrst, 'reload schema';
