-- Accountability + reporting indexes (requires 20260703 is_active).
-- Outcome replaces "status" on visit rows; visit_start replaces created_at.

-- Fast engineer performance reports: filter by assignee + outcome + time window.
create index if not exists idx_visit_assignee_outcome_start
  on public.ticket_visits (assignee, outcome, visit_start desc);

-- Fast "who owns this ticket right now?" lookups.
create index if not exists idx_visit_ticket_active
  on public.ticket_visits (ticket_number)
  where is_active = true;

-- Optional read helper: current assignee from active visit (source of truth).
create or replace function public.current_ticket_assignee(p_ticket_number text)
returns text
language sql
stable
as $$
  select tv.assignee
  from public.ticket_visits tv
  where tv.ticket_number = trim(p_ticket_number)
    and tv.is_active = true
  order by tv.visit_start desc, tv.id desc
  limit 1;
$$;

-- Lightweight rollup for dashboards (refresh via cron if you promote to MATERIALIZED VIEW).
create or replace view public.engineer_visit_summary as
select
  assignee,
  outcome,
  count(*)::bigint as visit_count,
  min(visit_start) as first_visit_at,
  max(visit_start) as last_visit_at
from public.ticket_visits
group by assignee, outcome;

notify pgrst, 'reload schema';
