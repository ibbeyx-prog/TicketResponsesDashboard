-- DB-level guard: inserting a new visit deactivates any prior active visit
-- for the same ticket (avoids duplicate is_active rows under race conditions).
-- Requires: 20260703_ticket_visits_is_active.sql (is_active column).

create or replace function public.handle_ticket_reassignment()
returns trigger
language plpgsql
as $$
begin
  -- Close the current active visit for this ticket before the new row is inserted.
  update public.ticket_visits
  set
    is_active = false,
    outcome = case
      when outcome = 'assigned' then 'reassigned'
      else outcome
    end,
    visit_end = coalesce(visit_end, now())
  where ticket_number = new.ticket_number
    and is_active = true;

  -- Ensure the incoming row is the sole active visit.
  new.is_active := true;
  if new.outcome is null or new.outcome = '' then
    new.outcome := 'assigned';
  end if;

  return new;
end;
$$;

drop trigger if exists trg_reassign_ticket on public.ticket_visits;

create trigger trg_reassign_ticket
before insert on public.ticket_visits
for each row
execute function public.handle_ticket_reassignment();

notify pgrst, 'reload schema';
