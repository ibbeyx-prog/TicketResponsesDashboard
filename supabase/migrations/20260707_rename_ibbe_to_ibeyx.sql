-- Rename legacy @ibbe / ibbe handles to @ibeyx / ibeyx across dashboard data.

create or replace function public._dash_handle_is_ibbe(raw text)
returns boolean
language sql
immutable
as $$
  select coalesce(
    lower(trim(both '@ ' from coalesce(raw, ''))) like 'ibbe%',
    false
  );
$$;

create or replace function public._dash_replace_ibbe_handle(raw text)
returns text
language sql
immutable
as $$
  select case
    when raw is null or trim(raw) = '' then raw
    when public._dash_handle_is_ibbe(raw) then
      case
        when trim(raw) like '@%' then '@ibeyx'
        else 'ibeyx'
      end
    else raw
  end;
$$;

update public.dashboard_sales_cases
set
  attended_by = public._dash_replace_ibbe_handle(attended_by),
  admin_owner = public._dash_replace_ibbe_handle(admin_owner),
  assigned_to = public._dash_replace_ibbe_handle(assigned_to)
where public._dash_handle_is_ibbe(attended_by)
   or public._dash_handle_is_ibbe(admin_owner)
   or public._dash_handle_is_ibbe(assigned_to);

update public.tickets_active
set
  assigned_to = public._dash_replace_ibbe_handle(assigned_to),
  field_responded_by = public._dash_replace_ibbe_handle(field_responded_by)
where public._dash_handle_is_ibbe(assigned_to)
   or public._dash_handle_is_ibbe(field_responded_by);

update public.ticket_visits
set assignee = public._dash_replace_ibbe_handle(assignee)
where public._dash_handle_is_ibbe(assignee);

update public.ticket_attendance_logs
set member_username = public._dash_replace_ibbe_handle(member_username)
where public._dash_handle_is_ibbe(member_username);

update public.dashboard_field_engineers
set username = 'ibeyx'
where public._dash_handle_is_ibbe(username);

update public.dashboard_users
set
  username = 'ibeyx',
  operator_id = case
    when public._dash_handle_is_ibbe(operator_id) then 'ibeyx'
    else operator_id
  end
where public._dash_handle_is_ibbe(username)
   or public._dash_handle_is_ibbe(operator_id);

drop function if exists public._dash_replace_ibbe_handle(text);
drop function if exists public._dash_handle_is_ibbe(text);

notify pgrst, 'reload schema';
