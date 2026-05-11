-- Adds the missing updated_at column on public.tickets and keeps it fresh
-- on every UPDATE via a trigger. Idempotent: safe to re-run.
--
-- After this runs, the Streamlit dashboard will sort by updated_at again
-- and the bot's _execute_ticket_update fallback becomes a no-op.

alter table public.tickets
  add column if not exists updated_at timestamptz default now();

create or replace function public.set_updated_at()
returns trigger
language plpgsql
as $$
begin
  new.updated_at := now();
  return new;
end;
$$;

drop trigger if exists trg_tickets_set_updated_at on public.tickets;

create trigger trg_tickets_set_updated_at
  before update on public.tickets
  for each row
  execute function public.set_updated_at();
