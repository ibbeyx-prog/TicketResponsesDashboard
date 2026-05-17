-- Who actually sent the field reply (e.g. test phone), when different from assigned_to.

alter table public.tickets_active
  add column if not exists field_responded_by text;
