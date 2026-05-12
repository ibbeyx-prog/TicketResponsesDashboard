-- Permissive RLS policies for public.tickets.
--
-- RLS is enabled on the table (Supabase default) but no policies exist,
-- which causes PostgREST to deny every request from the `anon` role
-- (i.e. anything using the publishable key). Both the Telegram bot
-- (writes) and the Streamlit dashboard (reads) authenticate as `anon`,
-- so without these policies the entire ticket pipeline is locked.
--
-- The policies below grant unconditional SELECT / INSERT / UPDATE to
-- anon. That is acceptable for this project because:
--   * The publishable key is only ever embedded in our internal bot
--     and dashboard, never in a public-facing client.
--   * There is no per-user authorization model in `tickets` -- every
--     entry in the table is meant to be visible to the ops team.
--   * DELETE is intentionally NOT granted; tickets are only ever
--     completed in place, never removed by app code.
--
-- Idempotent: each `drop policy if exists` lets the migration re-run
-- safely.

drop policy if exists "tickets_anon_select" on public.tickets;
drop policy if exists "tickets_anon_insert" on public.tickets;
drop policy if exists "tickets_anon_update" on public.tickets;

create policy "tickets_anon_select"
  on public.tickets
  for select
  to anon
  using (true);

create policy "tickets_anon_insert"
  on public.tickets
  for insert
  to anon
  with check (true);

create policy "tickets_anon_update"
  on public.tickets
  for update
  to anon
  using (true)
  with check (true);
