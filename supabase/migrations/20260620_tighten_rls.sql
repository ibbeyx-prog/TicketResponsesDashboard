-- Tighten RLS for production.
--
-- ⚠️ Review with your team before applying. This changes who can write to the DB.
-- Test on a staging environment first.
--
-- Current Streamlit dashboard + Telegram bot use SUPABASE_KEY (anon/publishable).
-- Dropping anon INSERT/UPDATE/DELETE on tickets_active will block dashboard writes
-- until you route mutations through a backend using the service role key.
-- If still on anon-key clients, do NOT apply the tickets_active section yet.

-- -------------------------------------------------------
-- tickets_active: keep anon SELECT for dashboard reads.
-- Restrict INSERT/UPDATE/DELETE to service role only.
-- Dashboard writes go through app server with service key.
-- -------------------------------------------------------

drop policy if exists "tickets_active_anon_insert" on public.tickets_active;
drop policy if exists "tickets_anon_insert" on public.tickets_active;
drop policy if exists "tickets_active_anon_update" on public.tickets_active;
drop policy if exists "tickets_anon_update" on public.tickets_active;
drop policy if exists "tickets_active_anon_delete" on public.tickets_active;
drop policy if exists "tickets_anon_delete" on public.tickets_active;

-- If your Streamlit app uses the anon key directly (current setup),
-- comment out the three DROP lines above and leave policies in place
-- until you move to a backend API with the service role key.

-- -------------------------------------------------------
-- ticket_attendance_logs: already correct (no UPDATE/DELETE).
-- Verify the policies are in place:
-- -------------------------------------------------------
-- select * from pg_policies where tablename = 'ticket_attendance_logs';
-- expected: anon select + anon insert only.

-- -------------------------------------------------------
-- dashboard_field_engineers + dashboard_task_categories:
-- restrict delete to prevent accidental removals via anon key.
-- soft-delete (is_active=false) replaces hard-delete for engineers.
-- -------------------------------------------------------
drop policy if exists "dashboard_field_engineers_anon_delete" on public.dashboard_field_engineers;
-- engineers are now soft-deleted via update is_active=false.

notify pgrst, 'reload schema';
