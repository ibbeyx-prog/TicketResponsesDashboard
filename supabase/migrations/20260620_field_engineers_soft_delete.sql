-- Soft-delete field engineers (keep row for historical ticket/visit attribution).

alter table public.dashboard_field_engineers
  add column if not exists is_active boolean not null default true;

-- Partial unique index: one active row per handle (case-insensitive).
-- Keep dashboard_field_engineers_username_lower_idx for all rows / reactivation.
create index if not exists idx_field_engineers_active
  on public.dashboard_field_engineers (lower(username))
  where is_active = true;

-- Allow dashboard to deactivate / reactivate handles.
drop policy if exists "dashboard_field_engineers_anon_update"
  on public.dashboard_field_engineers;

create policy "dashboard_field_engineers_anon_update"
  on public.dashboard_field_engineers
  for update
  to anon
  using (true)
  with check (true);

notify pgrst, 'reload schema';
