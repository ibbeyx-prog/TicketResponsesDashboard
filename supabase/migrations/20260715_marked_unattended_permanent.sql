-- Permanent unattended flag: count never clears when status changes.
-- Auto-unattended tickets move to Needs Review (Open) for admin.

alter table public.tickets_active
  add column if not exists marked_unattended_at timestamptz;

comment on column public.tickets_active.marked_unattended_at is
  'Set once when auto-unattended (no same-day field response). Permanent metric; ticket moves to Open.';

-- Backfill legacy Unattended status rows.
update public.tickets_active
set marked_unattended_at = coalesce(updated_at, now())
where marked_unattended_at is null
  and lower(trim(coalesce(status, ''))) = 'unattended';

update public.tickets_active
set status = 'Open',
    updated_at = coalesce(updated_at, now())
where lower(trim(coalesce(status, ''))) = 'unattended';

notify pgrst, 'reload schema';
