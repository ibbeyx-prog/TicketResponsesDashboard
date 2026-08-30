-- @ibex was added to dashboard_field_engineers by mistake; @ibeyx is the correct handle.

create or replace function public._dash_handle_is_ibex(raw text)
returns boolean
language sql
immutable
as $$
  select lower(trim(both '@ ' from coalesce(raw, ''))) = 'ibex';
$$;

create or replace function public._dash_replace_ibex_handle(raw text)
returns text
language sql
immutable
as $$
  select case
    when raw is null or trim(raw) = '' then raw
    when public._dash_handle_is_ibex(raw) then
      case when trim(raw) like '@%' then '@ibeyx' else 'ibeyx' end
    else raw
  end;
$$;

update public.tickets_active
set
  assigned_to = public._dash_replace_ibex_handle(assigned_to),
  assigned_to_2 = public._dash_replace_ibex_handle(assigned_to_2),
  field_responded_by = public._dash_replace_ibex_handle(field_responded_by)
where public._dash_handle_is_ibex(assigned_to)
   or public._dash_handle_is_ibex(assigned_to_2)
   or public._dash_handle_is_ibex(field_responded_by);

update public.dashboard_sales_cases
set
  assigned_to = public._dash_replace_ibex_handle(assigned_to),
  assigned_to_2 = public._dash_replace_ibex_handle(assigned_to_2),
  field_responded_by = public._dash_replace_ibex_handle(field_responded_by),
  attended_by = public._dash_replace_ibex_handle(attended_by)
where public._dash_handle_is_ibex(assigned_to)
   or public._dash_handle_is_ibex(assigned_to_2)
   or public._dash_handle_is_ibex(field_responded_by)
   or public._dash_handle_is_ibex(attended_by);

update public.ticket_visits
set assignee = public._dash_replace_ibex_handle(assignee)
where public._dash_handle_is_ibex(assignee);

update public.ticket_attendance_logs
set member_username = public._dash_replace_ibex_handle(member_username)
where public._dash_handle_is_ibex(member_username);

update public.dashboard_field_engineers
set is_active = false
where public._dash_handle_is_ibex(username);

update public.dashboard_field_engineers
set is_active = true
where lower(trim(username)) = 'ibeyx';

insert into public.dashboard_field_engineers (username, is_active)
select 'ibeyx', true
where not exists (
  select 1 from public.dashboard_field_engineers where lower(trim(username)) = 'ibeyx'
);

drop function if exists public._dash_replace_ibex_handle(text);
drop function if exists public._dash_handle_is_ibex(text);
