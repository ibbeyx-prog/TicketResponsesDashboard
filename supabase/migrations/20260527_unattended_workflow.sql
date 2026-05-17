-- Unattended workflow: 6h nudge tracking + broader attendance log action types.

alter table public.tickets_active
  add column if not exists unattended_nudge_sent_at timestamptz;

-- Drop legacy enum-style check so new action types (Nudge, AutoUnattended, …) are allowed.
alter table public.ticket_attendance_logs
  drop constraint if exists ticket_attendance_logs_action_type_check;

alter table public.ticket_attendance_logs
  drop constraint if exists ticket_attendance_logs_action_type_check1;
