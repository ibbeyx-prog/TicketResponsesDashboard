-- Canonical @lowercase assignee on ticket_visits (merge @Dissiby / @dissiby etc.).

update public.ticket_visits
set assignee = '@' || lower(trim(both '@ ' from assignee))
where assignee is not null
  and trim(assignee) <> ''
  and assignee <> '@' || lower(trim(both '@ ' from assignee));

notify pgrst, 'reload schema';
