-- Allow dashboard (anon key) to delete sales case rows (Remove action).

drop policy if exists "dashboard_sales_cases_anon_delete" on public.dashboard_sales_cases;

create policy "dashboard_sales_cases_anon_delete"
  on public.dashboard_sales_cases
  for delete
  to anon
  using (true);

notify pgrst, 'reload schema';
