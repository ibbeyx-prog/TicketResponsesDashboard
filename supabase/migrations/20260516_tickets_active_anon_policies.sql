-- Fix anon (publishable key) access on ``tickets_active`` after the table rename.
--
-- ``20260512_history_and_rename.sql`` runs *before*
-- ``20260512_tickets_anon_rls_policies.sql`` (same date, lexical order). The
-- rename removes ``public.tickets``, so the older RLS migration can fail or
-- never attach policies to the table Streamlit and anon-key bots use.
-- Service-role keys bypass RLS, which matches “Telegram writes work but the
-- dashboard never loads / never updates”.

alter table if exists public.tickets_active enable row level security;

-- Drop legacy policy names that may still be bound to ``tickets_active`` after
-- ``alter table … rename`` (names stay; table OID is the same).
drop policy if exists "tickets_anon_select" on public.tickets_active;
drop policy if exists "tickets_anon_insert" on public.tickets_active;
drop policy if exists "tickets_anon_update" on public.tickets_active;

-- Idempotent re-create under explicit names (also clears duplicates if any).
drop policy if exists "tickets_active_anon_select" on public.tickets_active;
drop policy if exists "tickets_active_anon_insert" on public.tickets_active;
drop policy if exists "tickets_active_anon_update" on public.tickets_active;

create policy "tickets_active_anon_select"
  on public.tickets_active
  for select
  to anon
  using (true);

create policy "tickets_active_anon_insert"
  on public.tickets_active
  for insert
  to anon
  with check (true);

create policy "tickets_active_anon_update"
  on public.tickets_active
  for update
  to anon
  using (true)
  with check (true);

-- DELETE is defined in ``20260514_tickets_active_anon_delete.sql``; leave it.

notify pgrst, 'reload schema';
