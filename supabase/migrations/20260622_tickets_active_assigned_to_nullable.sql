-- Allow Pending tickets queued without an engineer (Command Center "Add to Pending only").

alter table if exists public.tickets_active
  alter column assigned_to drop not null;

notify pgrst, 'reload schema';
