-- Track the current assignee visit without relying on visit_end IS NULL alone.
-- is_active = true on exactly one row per ticket (the current assignment cycle).

alter table if exists public.ticket_visits
  add column if not exists is_active boolean not null default true;

-- Closed visits are never active.
update public.ticket_visits
set is_active = false
where visit_end is not null;

-- If duplicate open rows exist, keep only the newest open visit active per ticket.
with ranked as (
  select
    id,
    row_number() over (
      partition by ticket_number
      order by visit_start desc, id desc
    ) as rn
  from public.ticket_visits
  where visit_end is null
)
update public.ticket_visits tv
set is_active = (r.rn = 1)
from ranked r
where tv.id = r.id;

create index if not exists idx_visit_assignee_active
  on public.ticket_visits (assignee, is_active);

notify pgrst, 'reload schema';
