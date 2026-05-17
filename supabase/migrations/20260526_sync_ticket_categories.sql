-- Keep ``dashboard_task_categories`` aligned with categories already used on tickets.

insert into public.dashboard_task_categories (name, sort_order)
select distinct trim(t.task_category), 0
from public.tickets_active t
where t.task_category is not null
  and trim(t.task_category) <> ''
on conflict (name) do nothing;
