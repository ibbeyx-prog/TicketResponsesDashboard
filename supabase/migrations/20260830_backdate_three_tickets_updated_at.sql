-- Backdate updated_at so three resolved tickets fall in week 2026-08-23 .. 2026-08-29.
-- Trigger trg_tickets_set_updated_at overwrites updated_at on every UPDATE; disable briefly.

alter table public.tickets_active disable trigger trg_tickets_set_updated_at;

update public.tickets_active
set updated_at = timestamptz '2026-08-29 18:00:00+00'
where ticket_number in ('100630645', '100729388', '100624730');

alter table public.tickets_active enable trigger trg_tickets_set_updated_at;
