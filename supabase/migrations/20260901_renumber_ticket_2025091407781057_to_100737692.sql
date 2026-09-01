-- Renumber mistaken residential ticket ID to the correct CSM number.
-- Wrong: 2025091407781057  →  Correct: 100737692
-- ticket_visits FK has ON DELETE CASCADE only (no ON UPDATE), so copy row then repoint children.

INSERT INTO public.tickets_active (
  ticket_number,
  assigned_to,
  assigned_to_2,
  task_category,
  status,
  additional_info,
  field_response,
  field_responded_by,
  photo_url,
  responded_at,
  last_assigned_at,
  created_at,
  updated_at,
  assignment_telegram_chat_id,
  assignment_telegram_message_id,
  last_response_telegram_chat_id,
  last_response_telegram_message_id,
  marked_unattended_at,
  unattended_nudge_sent_at,
  nudge_telegram_chat_id,
  nudge_telegram_message_id,
  outcome_category,
  follow_up_at,
  follow_up_note
)
SELECT
  '100737692',
  assigned_to,
  assigned_to_2,
  task_category,
  status,
  additional_info,
  field_response,
  field_responded_by,
  replace(photo_url, '2025091407781057', '100737692'),
  responded_at,
  last_assigned_at,
  created_at,
  updated_at,
  assignment_telegram_chat_id,
  assignment_telegram_message_id,
  last_response_telegram_chat_id,
  last_response_telegram_message_id,
  marked_unattended_at,
  unattended_nudge_sent_at,
  nudge_telegram_chat_id,
  nudge_telegram_message_id,
  outcome_category,
  follow_up_at,
  follow_up_note
FROM public.tickets_active
WHERE ticket_number = '2025091407781057'
  AND NOT EXISTS (
    SELECT 1 FROM public.tickets_active WHERE ticket_number = '100737692'
  );

UPDATE public.ticket_visits
SET ticket_number = '100737692'
WHERE ticket_number = '2025091407781057';

UPDATE public.ticket_attendance_logs
SET ticket_number = '100737692'
WHERE ticket_number = '2025091407781057';

DELETE FROM public.tickets_active
WHERE ticket_number = '2025091407781057';

INSERT INTO public.ticket_attendance_logs (
  ticket_number,
  member_username,
  action_type,
  note
)
SELECT
  '100737692',
  'ibeyx',
  'AdminCorrection',
  'Renumbered ticket from mistaken ID 2025091407781057 to correct ID 100737692.'
WHERE EXISTS (
  SELECT 1 FROM public.tickets_active WHERE ticket_number = '100737692'
);
