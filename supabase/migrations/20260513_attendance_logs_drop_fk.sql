-- Drop the FK on ticket_attendance_logs.ticket_number so admins can delete
-- a ticket from the dashboard while keeping its history searchable.
--
-- The Log tab searches by exact ticket_number string anyway, so we don't
-- need the referential constraint -- we just need the column to survive
-- the parent row being removed.
alter table public.ticket_attendance_logs
  drop constraint if exists ticket_attendance_logs_ticket_number_fkey;

-- Index so the Log tab's `eq(ticket_number, ...)` lookup stays fast.
create index if not exists ticket_attendance_logs_ticket_number_idx
  on public.ticket_attendance_logs (ticket_number);

notify pgrst, 'reload schema';
