-- Rename queue status **Pending** → **Daily Task** (field assignment queue).

update public.tickets_active
set status = 'Daily Task', updated_at = now()
where status = 'Pending';

alter table public.tickets_active
  alter column status set default 'Daily Task';
