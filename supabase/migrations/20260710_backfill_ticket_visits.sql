-- Backfill ticket_visits for tickets with assignment history but no visit rows.
--
-- Common gap: dashboard **TicketQueued** → **AssignmentUpdated** (first assignee)
-- never called _visits_open_new, so Performance solo/shared omitted those engineers.

with tickets_needing_backfill as (
  select t.*
  from public.tickets_active t
  where t.assigned_to is not null
    and trim(t.assigned_to) <> ''
    and not exists (
      select 1
      from public.ticket_visits v
      where v.ticket_number = t.ticket_number
    )
),
visit_times as (
  select
    t.ticket_number,
    '@' || lower(trim(both '@ ' from t.assigned_to)) as assignee,
    coalesce(
      t.last_assigned_at,
      (
        select min(l.timestamp)
        from public.ticket_attendance_logs l
        where l.ticket_number = t.ticket_number
          and l.action_type in ('Assignment', 'AssignmentUpdated', 'TicketQueued')
      )
    ) as visit_start,
    coalesce(
      (
        select max(l.timestamp)
        from public.ticket_attendance_logs l
        where l.ticket_number = t.ticket_number
          and l.action_type = 'Response'
      ),
      t.responded_at,
      t.updated_at,
      t.last_assigned_at
    ) as visit_end,
    t.field_response,
    t.photo_url,
    t.status
  from tickets_needing_backfill t
),
normalized as (
  select
    ticket_number,
    assignee,
    visit_start,
    case
      when visit_end is null then visit_start
      when visit_end < visit_start then visit_start + interval '1 second'
      else visit_end
    end as visit_end,
    case
      when status = 'Unattended' then 'unattended'
      when status = 'On Hold' then 'on_hold'
      when status in ('Resolved', 'Open')
        and nullif(trim(coalesce(field_response, '')), '') is not null then 'responded'
      when status = 'Resolved' then 'responded'
      when status in ('Daily Task', 'Under Investigation') then 'assigned'
      else 'responded'
    end as outcome,
    nullif(trim(coalesce(field_response, '')), '') as response_note,
    nullif(trim(coalesce(photo_url, '')), '') as photo_url
  from visit_times
  where visit_start is not null
    and assignee <> '@'
)
insert into public.ticket_visits (
  ticket_number,
  assignee,
  visit_start,
  visit_end,
  outcome,
  response_note,
  photo_url,
  closed_by,
  is_active
)
select
  ticket_number,
  assignee,
  visit_start,
  visit_end,
  outcome,
  response_note,
  photo_url,
  'backfill',
  false
from normalized;

notify pgrst, 'reload schema';
