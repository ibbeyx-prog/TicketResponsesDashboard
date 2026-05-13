-- Allow dashboard (anon / publishable key) to delete rows from
-- tickets_active. The Streamlit admin "Delete" control issues DELETE
-- through PostgREST; without this policy RLS rejects the request even
-- though SELECT/INSERT/UPDATE were already granted.

drop policy if exists "tickets_active_anon_delete" on public.tickets_active;

create policy "tickets_active_anon_delete"
  on public.tickets_active
  for delete
  to anon
  using (true);

notify pgrst, 'reload schema';
