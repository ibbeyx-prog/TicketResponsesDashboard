-- Capture free-text "additional info" alongside each assignment.
--
-- When a dispatcher writes
--
--   @Dissiby Femto Installation 100591416
--   Ma. Naaringuge (Ground Floor)
--   Musthafa Ibrahim Manik  7363330
--   MA20433451
--
-- the regex now grabs everything after the ticket number as a single
-- string. The bot stores it on the active row so the dashboard can
-- display it next to the assignee / category, and copies the same text
-- into the Assignment row's `note` in ticket_attendance_logs so the
-- history view shows what context the field engineer received.
--
-- Idempotent: safe to re-run.

alter table public.tickets_active
  add column if not exists additional_info text;
