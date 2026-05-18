-- Backfill field_responded_by and repair tickets where Response was logged but the
-- active row was never updated.
--
-- **Only tickets assigned on/after the bot was added to the field group** are
-- included. Pre-deploy assignments stay as-is (no dashboard backfill).
--
-- Before running: set deploy_cutoff below to your group deploy time (UTC).
-- Example: first assignment the live bot saved after redeploy.

with deploy_cutoff as (
  select timestamptz '2026-05-17 00:00:00+00' as t
),
latest_response as (
  select distinct on (ticket_number)
    ticket_number,
    member_username,
    note,
    photo_url,
    timestamp
  from public.ticket_attendance_logs
  where action_type = 'Response'
  order by ticket_number, timestamp desc
),
parsed as (
  select
    l.ticket_number,
    l.member_username,
    l.note,
    l.photo_url,
    l.timestamp,
    t.assigned_to,
    t.status,
    t.responded_at,
    t.field_response,
    t.last_assigned_at,
    case
      when l.note ~* '^Responded by\s+[^:]+:\s*(.+)$' then
        trim((regexp_match(l.note, '^Responded by\s+[^:]+:\s*(.+)$', 'is'))[1])
      else nullif(trim(coalesce(l.note, '')), '')
    end as inferred_field_response,
    case
      when l.member_username ~* '\(via\s+@?([^)]+)\)' then
        '@' || lower(trim(both '@ ' from (regexp_match(l.member_username, '\(via\s+@?([^)]+)\)', 'i'))[1]))
      when l.note ~* '^Responded by\s+([^:]+):' then
        trim((regexp_match(l.note, '^Responded by\s+([^:]+):', 'i'))[1])
      when l.member_username is not null
        and lower(trim(both '@ ' from l.member_username))
          <> lower(trim(both '@ ' from coalesce(t.assigned_to, '')))
        then trim(l.member_username)
      else null
    end as inferred_responded_by
  from latest_response l
  join public.tickets_active t on t.ticket_number = l.ticket_number
  cross join deploy_cutoff d
  where t.last_assigned_at is not null
    and t.last_assigned_at >= d.t
)
update public.tickets_active t
set
  status = case
    when p.status = 'Pending' then 'Open'
    else t.status
  end,
  field_response = coalesce(
    t.field_response,
    p.inferred_field_response
  ),
  photo_url = coalesce(t.photo_url, p.photo_url),
  responded_at = coalesce(t.responded_at, p.timestamp),
  field_responded_by = coalesce(
    nullif(trim(t.field_responded_by), ''),
    nullif(trim(p.inferred_responded_by), '')
  ),
  updated_at = greatest(
    coalesce(t.updated_at, p.timestamp),
    p.timestamp,
    now()
  )
from parsed p
where t.ticket_number = p.ticket_number
  and (
    coalesce(t.field_responded_by, '') = ''
    and p.inferred_responded_by is not null
    or p.status = 'Pending'
    or t.responded_at is null
  );
