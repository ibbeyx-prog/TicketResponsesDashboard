-- Enforce allowed queue statuses on field tickets and sales cases.
-- ticket_visits.outcome already has CHECK (assigned, responded, reassigned, unattended, on_hold)
-- in 20260702_ticket_visits.sql — no change needed here.

-- Normalize any legacy ticket statuses before adding the constraint.
update public.tickets_active
set status = 'Daily Task', updated_at = now()
where status in ('Pending');

update public.tickets_active
set status = 'Resolved', updated_at = now()
where status in ('Completed');

update public.tickets_active
set status = 'On Hold', updated_at = now()
where status in ('No Answer', 'Unavailable');

-- Normalize legacy sales statuses (should already be backfilled by 20260622).
update public.dashboard_sales_cases
set status = 'Sales ticket', updated_at = now()
where status = 'Sales intake';

update public.dashboard_sales_cases
set status = 'Investigation', updated_at = now()
where status in ('Admin triage', 'System check', 'Dispatch approved');

update public.dashboard_sales_cases
set status = 'Regional for site visit', updated_at = now()
where status in ('Awaiting field', 'Field in progress');

update public.dashboard_sales_cases
set status = 'Design', updated_at = now()
where status = 'Admin review';

update public.dashboard_sales_cases
set status = 'Resolved', updated_at = now()
where status = 'Closed';

alter table public.tickets_active
  drop constraint if exists tickets_active_status_check;

alter table public.tickets_active
  add constraint tickets_active_status_check
  check (status in (
    'Daily Task',
    'Open',
    'On Hold',
    'Under Investigation',
    'Resolved',
    'Unattended'
  ));

alter table public.dashboard_sales_cases
  drop constraint if exists sales_cases_status_check;

alter table public.dashboard_sales_cases
  add constraint sales_cases_status_check
  check (status in (
    'Sales ticket',
    'Investigation',
    'Regional for site visit',
    'Design',
    'Resolved'
  ));

-- ticket_visits outcome values (already has CHECK — verified in 20260702_ticket_visits.sql):
-- assigned, responded, reassigned, unattended, on_hold

notify pgrst, 'reload schema';
