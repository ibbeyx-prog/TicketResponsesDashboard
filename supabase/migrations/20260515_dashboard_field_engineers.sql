-- Field-team Telegram handles for the Streamlit Command Center assignee picker.
-- Managed from the dashboard (add / remove). Seeded with the initial roster.

create table if not exists public.dashboard_field_engineers (
  username text primary key,
  created_at timestamptz not null default now()
);

create unique index if not exists dashboard_field_engineers_username_lower_idx
  on public.dashboard_field_engineers (lower(username));

alter table public.dashboard_field_engineers enable row level security;

drop policy if exists "dashboard_field_engineers_anon_select"
  on public.dashboard_field_engineers;
drop policy if exists "dashboard_field_engineers_anon_insert"
  on public.dashboard_field_engineers;
drop policy if exists "dashboard_field_engineers_anon_delete"
  on public.dashboard_field_engineers;

create policy "dashboard_field_engineers_anon_select"
  on public.dashboard_field_engineers
  for select
  to anon
  using (true);

create policy "dashboard_field_engineers_anon_insert"
  on public.dashboard_field_engineers
  for insert
  to anon
  with check (true);

create policy "dashboard_field_engineers_anon_delete"
  on public.dashboard_field_engineers
  for delete
  to anon
  using (true);

insert into public.dashboard_field_engineers (username) values
  ('ibeyx'),
  ('Dissiby'),
  ('Nallu10'),
  ('FatrixShaquiell')
on conflict (username) do nothing;
