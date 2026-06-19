-- Legacy append-only log for /respond fallback (primary path: tickets_active + attendance).

create table if not exists public.ticket_responses (
  id            bigint generated always as identity primary key,
  ticket_id     text        not null,
  user_handle   text        not null,
  response_data text,
  created_at    timestamptz not null default now()
);

alter table public.ticket_responses enable row level security;

drop policy if exists ticket_responses_anon_select on public.ticket_responses;
drop policy if exists ticket_responses_anon_insert on public.ticket_responses;

create policy ticket_responses_anon_select
  on public.ticket_responses
  for select
  to anon
  using (true);

create policy ticket_responses_anon_insert
  on public.ticket_responses
  for insert
  to anon
  with check (true);

notify pgrst, 'reload schema';
