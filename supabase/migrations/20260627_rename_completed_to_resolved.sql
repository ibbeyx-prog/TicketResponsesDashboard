-- Rename field ticket status **Completed** → **Resolved**.

update public.tickets_active
set status = 'Resolved', updated_at = now()
where status = 'Completed';

update public.ticket_attendance_logs
set action_type = 'Resolved'
where action_type = 'Completed';
