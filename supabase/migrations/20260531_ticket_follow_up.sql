-- Optional follow-up metadata for individual cases moved to Under Investigation.

alter table if exists public.tickets_active
  add column if not exists follow_up_at timestamptz;

alter table if exists public.tickets_active
  add column if not exists follow_up_note text;

notify pgrst, 'reload schema';
