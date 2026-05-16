-- Per-user dashboard login (username + password). Passwords use pgcrypto bcrypt.
-- Apply in Supabase SQL editor. Streamlit calls SECURITY DEFINER RPCs (anon cannot read hashes).

create extension if not exists pgcrypto;

create table if not exists public.dashboard_users (
  username text primary key,
  operator_id text not null,
  password_hash text not null,
  is_active boolean not null default true,
  reset_token_hash text,
  reset_token_expires_at timestamptz,
  created_at timestamptz not null default now(),
  updated_at timestamptz not null default now(),
  constraint dashboard_users_username_lower_chk check (username = lower(username)),
  constraint dashboard_users_operator_id_nonempty_chk check (length(trim(operator_id)) > 0)
);

create unique index if not exists dashboard_users_operator_id_lower_idx
  on public.dashboard_users (lower(operator_id));

alter table public.dashboard_users enable row level security;

-- No anon policies: login/reset only via RPC below.

create or replace function public.dashboard_users_configured()
returns boolean
language sql
stable
security definer
set search_path = public
as $$
  select exists (select 1 from public.dashboard_users where is_active limit 1);
$$;

create or replace function public.dashboard_verify_login(p_username text, p_password text)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  r public.dashboard_users%rowtype;
  u text := lower(trim(coalesce(p_username, '')));
begin
  if u = '' or p_password is null or length(p_password) < 1 then
    return jsonb_build_object('ok', false, 'error', 'invalid_credentials');
  end if;

  select * into r
  from public.dashboard_users
  where username = u and is_active;

  if not found then
    return jsonb_build_object('ok', false, 'error', 'invalid_credentials');
  end if;

  if r.password_hash is distinct from crypt(p_password, r.password_hash) then
    return jsonb_build_object('ok', false, 'error', 'invalid_credentials');
  end if;

  update public.dashboard_users
  set updated_at = now(),
      reset_token_hash = null,
      reset_token_expires_at = null
  where username = u;

  return jsonb_build_object(
    'ok', true,
    'username', r.username,
    'operator_id', r.operator_id
  );
end;
$$;

create or replace function public.dashboard_request_password_reset(p_username text)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  r public.dashboard_users%rowtype;
  u text := lower(trim(coalesce(p_username, '')));
  tok text;
begin
  if u = '' then
    return jsonb_build_object('ok', true, 'message', 'generic');
  end if;

  select * into r
  from public.dashboard_users
  where username = u and is_active;

  if not found then
    return jsonb_build_object('ok', true, 'message', 'generic');
  end if;

  tok := upper(substr(encode(gen_random_bytes(4), 'hex'), 1, 8));

  update public.dashboard_users
  set reset_token_hash = crypt(tok, gen_salt('bf')),
      reset_token_expires_at = now() + interval '15 minutes',
      updated_at = now()
  where username = u;

  return jsonb_build_object(
    'ok', true,
    'message', 'code_issued',
    'reset_code', tok,
    'expires_minutes', 15
  );
end;
$$;

create or replace function public.dashboard_reset_password(
  p_username text,
  p_reset_code text,
  p_new_password text
)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  r public.dashboard_users%rowtype;
  u text := lower(trim(coalesce(p_username, '')));
  code text := upper(trim(coalesce(p_reset_code, '')));
begin
  if u = '' or code = '' or p_new_password is null or length(p_new_password) < 8 then
    return jsonb_build_object('ok', false, 'error', 'invalid_request');
  end if;

  select * into r
  from public.dashboard_users
  where username = u and is_active;

  if not found then
    return jsonb_build_object('ok', false, 'error', 'invalid_reset');
  end if;

  if r.reset_token_hash is null
     or r.reset_token_expires_at is null
     or r.reset_token_expires_at < now()
     or r.reset_token_hash is distinct from crypt(code, r.reset_token_hash) then
    return jsonb_build_object('ok', false, 'error', 'invalid_reset');
  end if;

  update public.dashboard_users
  set password_hash = crypt(p_new_password, gen_salt('bf')),
      reset_token_hash = null,
      reset_token_expires_at = null,
      updated_at = now()
  where username = u;

  return jsonb_build_object('ok', true);
end;
$$;

revoke all on function public.dashboard_users_configured() from public;
revoke all on function public.dashboard_verify_login(text, text) from public;
revoke all on function public.dashboard_request_password_reset(text) from public;
revoke all on function public.dashboard_reset_password(text, text, text) from public;

grant execute on function public.dashboard_users_configured() to anon, authenticated, service_role;
grant execute on function public.dashboard_verify_login(text, text) to anon, authenticated, service_role;
grant execute on function public.dashboard_request_password_reset(text) to anon, authenticated, service_role;
grant execute on function public.dashboard_reset_password(text, text, text) to anon, authenticated, service_role;

-- First admin (change password after deploy via Forgot password on login screen).
insert into public.dashboard_users (username, operator_id, password_hash)
values ('admin', 'admin', crypt('ChangeMeNow!', gen_salt('bf')))
on conflict (username) do nothing;
