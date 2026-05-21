-- Legacy label from earlier experiments (admin sets No Answer via dashboard; see 20260624).
-- NOTE: An earlier draft auto-moved Pending rows here; 20260624 reverts that bulk update.

update public.tickets_active
set status = 'No Answer', updated_at = now()
where status = 'Unavailable';
