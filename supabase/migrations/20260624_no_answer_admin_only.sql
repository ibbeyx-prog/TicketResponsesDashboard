-- No Answer is set only by admins (dashboard Action → No Answer), not by bulk SQL rules.
-- Undo automated Pending → No Answer from 20260623; keep rows with an explicit NoAnswer log.

update public.tickets_active t
set
  status = 'Pending',
  updated_at = now()
where t.status = 'No Answer'
  and not exists (
    select 1
    from public.ticket_attendance_logs l
    where l.ticket_number = t.ticket_number
      and l.action_type = 'NoAnswer'
  );
