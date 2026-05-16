-- Admin RPCs: list / create / activate dashboard users (admin password required per call).

create or replace function public._dashboard_admin_is_verified(
  p_admin_username text,
  p_admin_password text
)
returns boolean
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  r public.dashboard_users%rowtype;
  u text := lower(trim(coalesce(p_admin_username, '')));
begin
  if u = '' or p_admin_password is null or length(p_admin_password) < 1 then
    return false;
  end if;

  select * into r
  from public.dashboard_users
  where username = u and is_active;

  if not found then
    return false;
  end if;

  return r.password_hash is not distinct from crypt(p_admin_password, r.password_hash);
end;
$$;

revoke all on function public._dashboard_admin_is_verified(text, text) from public;

create or replace function public.dashboard_admin_list_users(
  p_admin_username text,
  p_admin_password text
)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
begin
  if not public._dashboard_admin_is_verified(p_admin_username, p_admin_password) then
    return jsonb_build_object('ok', false, 'error', 'forbidden');
  end if;

  return jsonb_build_object(
    'ok', true,
    'users', coalesce(
      (
        select jsonb_agg(
          jsonb_build_object(
            'username', u.username,
            'operator_id', u.operator_id,
            'is_active', u.is_active,
            'created_at', u.created_at
          )
          order by u.username
        )
        from public.dashboard_users u
      ),
      '[]'::jsonb
    )
  );
end;
$$;

create or replace function public.dashboard_admin_create_user(
  p_admin_username text,
  p_admin_password text,
  p_new_username text,
  p_new_operator_id text,
  p_new_password text
)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  nu text := lower(trim(coalesce(p_new_username, '')));
  nop text := trim(coalesce(p_new_operator_id, ''));
begin
  if not public._dashboard_admin_is_verified(p_admin_username, p_admin_password) then
    return jsonb_build_object('ok', false, 'error', 'forbidden');
  end if;

  if nu = '' or nu <> lower(nu) or length(nu) > 48 then
    return jsonb_build_object('ok', false, 'error', 'invalid_username');
  end if;

  if nop = '' or length(nop) > 64 then
    return jsonb_build_object('ok', false, 'error', 'invalid_operator_id');
  end if;

  if p_new_password is null or length(p_new_password) < 8 then
    return jsonb_build_object('ok', false, 'error', 'weak_password');
  end if;

  if exists (select 1 from public.dashboard_users where lower(operator_id) = lower(nop)) then
    return jsonb_build_object('ok', false, 'error', 'operator_id_taken');
  end if;

  insert into public.dashboard_users (username, operator_id, password_hash)
  values (nu, nop, crypt(p_new_password, gen_salt('bf')));

  return jsonb_build_object(
    'ok', true,
    'username', nu,
    'operator_id', nop
  );
exception
  when unique_violation then
    return jsonb_build_object('ok', false, 'error', 'username_taken');
end;
$$;

create or replace function public.dashboard_admin_set_user_active(
  p_admin_username text,
  p_admin_password text,
  p_target_username text,
  p_is_active boolean
)
returns jsonb
language plpgsql
security definer
set search_path = public, extensions
as $$
declare
  admin_u text := lower(trim(coalesce(p_admin_username, '')));
  tu text := lower(trim(coalesce(p_target_username, '')));
  active_admins int;
begin
  if not public._dashboard_admin_is_verified(p_admin_username, p_admin_password) then
    return jsonb_build_object('ok', false, 'error', 'forbidden');
  end if;

  if tu = '' then
    return jsonb_build_object('ok', false, 'error', 'invalid_username');
  end if;

  if tu = admin_u and p_is_active is not true then
    return jsonb_build_object('ok', false, 'error', 'cannot_deactivate_self');
  end if;

  if p_is_active is not true then
    select count(*)::int into active_admins
    from public.dashboard_users
    where is_active and username <> tu;

    if active_admins < 1 then
      return jsonb_build_object('ok', false, 'error', 'last_active_user');
    end if;
  end if;

  update public.dashboard_users
  set is_active = coalesce(p_is_active, true),
      updated_at = now()
  where username = tu;

  if not found then
    return jsonb_build_object('ok', false, 'error', 'not_found');
  end if;

  return jsonb_build_object('ok', true, 'username', tu, 'is_active', coalesce(p_is_active, true));
end;
$$;

revoke all on function public.dashboard_admin_list_users(text, text) from public;
revoke all on function public.dashboard_admin_create_user(text, text, text, text, text) from public;
revoke all on function public.dashboard_admin_set_user_active(text, text, text, boolean) from public;

grant execute on function public.dashboard_admin_list_users(text, text) to anon, authenticated, service_role;
grant execute on function public.dashboard_admin_create_user(text, text, text, text, text) to anon, authenticated, service_role;
grant execute on function public.dashboard_admin_set_user_active(text, text, text, boolean) to anon, authenticated, service_role;
