-- One-time merge when both ``public.tickets`` and ``public.tickets_active`` exist
-- (e.g. bot had ``TICKETS_TABLE=tickets`` while the dashboard read ``tickets_active``).
--
-- Inserts rows from ``tickets`` whose ``ticket_number`` is not yet in
-- ``tickets_active``. Does not overwrite existing ``tickets_active`` rows (avoids
-- clobbering newer Command Center state). Re-runnable.
--
-- Uses dynamic SQL so this migration applies cleanly when ``tickets`` was already
-- renamed away (only ``tickets_active`` exists).

do $$
begin
  if to_regclass('public.tickets') is null or to_regclass('public.tickets_active') is null then
    raise notice 'sync_legacy_tickets_into_tickets_active: skipped (need both public.tickets and public.tickets_active)';
    return;
  end if;

  execute $sync$
    insert into public.tickets_active (
      ticket_number,
      assigned_to,
      task_category,
      status,
      created_at,
      updated_at,
      last_assigned_at
    )
    select
      t.ticket_number,
      t.assigned_to,
      t.task_category,
      t.status,
      coalesce(t.created_at, now()),
      coalesce(t.updated_at, now()),
      coalesce(t.updated_at, t.created_at, now())
    from public.tickets t
    where not exists (
      select 1
      from public.tickets_active ta
      where ta.ticket_number = t.ticket_number
    )
  $sync$;
end$$;
