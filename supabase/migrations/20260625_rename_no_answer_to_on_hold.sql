-- Rename queue status **No Answer** → **On Hold** (admin-set via dashboard Action).

update public.tickets_active
set status = 'On Hold', updated_at = now()
where status = 'No Answer';

update public.ticket_attendance_logs
set action_type = 'OnHold'
where action_type = 'NoAnswer';
