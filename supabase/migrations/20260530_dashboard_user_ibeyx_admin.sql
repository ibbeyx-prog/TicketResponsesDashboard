-- Ensure ibeyx exists as a dashboard login (for projects that ran 20260520 before ibeyx seed).

insert into public.dashboard_users (username, operator_id, password_hash)
values ('ibeyx', 'ibeyx', crypt('ChangeMeNow!', gen_salt('bf')))
on conflict (username) do update
  set operator_id = excluded.operator_id,
      is_active = true,
      updated_at = now();
