-- Durable /respond session state for the Telegram bot (survives restarts).

create table if not exists public.bot_sessions (
  telegram_user_id bigint primary key,
  chat_id          bigint not null,
  active_ticket    text,
  updated_at       timestamptz not null default now()
);

alter table public.bot_sessions enable row level security;

drop policy if exists bot_sessions_anon_select on public.bot_sessions;
drop policy if exists bot_sessions_anon_insert on public.bot_sessions;
drop policy if exists bot_sessions_anon_update on public.bot_sessions;
drop policy if exists bot_sessions_anon_delete on public.bot_sessions;

create policy bot_sessions_anon_select
  on public.bot_sessions
  for select
  to anon
  using (true);

create policy bot_sessions_anon_insert
  on public.bot_sessions
  for insert
  to anon
  with check (true);

create policy bot_sessions_anon_update
  on public.bot_sessions
  for update
  to anon
  using (true)
  with check (true);

create policy bot_sessions_anon_delete
  on public.bot_sessions
  for delete
  to anon
  using (true);

notify pgrst, 'reload schema';
