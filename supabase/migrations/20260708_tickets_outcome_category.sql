-- Actual work category at resolve/close (assignment ``task_category`` stays unchanged).
alter table public.tickets_active
  add column if not exists outcome_category text;

comment on column public.tickets_active.outcome_category is
  'Work actually performed when resolved; task_category remains dispatch intent.';

create index if not exists tickets_active_outcome_category_idx
  on public.tickets_active (outcome_category)
  where outcome_category is not null;
