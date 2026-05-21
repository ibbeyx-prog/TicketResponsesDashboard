-- First-class **No Answer** queue: assigned tickets with no field reply on the current visit.

update public.tickets_active
set
  status = 'No Answer',
  updated_at = now()
where status in ('Pending', 'Unavailable')
  and assigned_to is not null
  and trim(assigned_to) <> ''
  and coalesce(trim(field_response), '') = ''
  and photo_url is null
  and (
    responded_at is null
    or last_assigned_at is null
    or responded_at < last_assigned_at
  );

-- Legacy label from earlier experiments
update public.tickets_active
set status = 'No Answer', updated_at = now()
where status = 'Unavailable';
