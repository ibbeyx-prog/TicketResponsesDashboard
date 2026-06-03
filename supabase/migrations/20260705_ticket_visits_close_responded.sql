-- Close the active visit for a specific engineer when they respond.
-- Matches app/bot logic: ticket_number + assignee + is_active = true.

create or replace function public.close_ticket_visit_responded(
  p_ticket_number text,
  p_assignee text,
  p_response_note text default null,
  p_photo_url text default null,
  p_closed_by text default 'system'
)
returns void
language plpgsql
as $$
declare
  v_assignee text;
begin
  v_assignee := '@' || lower(trim(both '@' from coalesce(p_assignee, '')));

  update public.ticket_visits
  set
    is_active = false,
    outcome = 'responded',
    visit_end = coalesce(visit_end, now()),
    response_note = coalesce(p_response_note, response_note),
    photo_url = coalesce(p_photo_url, photo_url),
    closed_by = coalesce(nullif(p_closed_by, ''), closed_by)
  where ticket_number = trim(p_ticket_number)
    and assignee = v_assignee
    and is_active = true;
end;
$$;

notify pgrst, 'reload schema';
