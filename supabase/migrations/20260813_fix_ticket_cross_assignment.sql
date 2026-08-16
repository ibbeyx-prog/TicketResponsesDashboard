-- Correct mistaken assignment: Gulisthaanuge Coverage Check was posted on
-- 2024021010000101 instead of new ticket 2026081310000101 (2026-08-13).
-- Applied via scripts/fix_ticket_2024021010000101_2026081310000101.py on live DB.

-- Restore 2024021010000101 (Device Recovery — Nasrussabaa / Zidhan)
UPDATE public.tickets_active
SET
  task_category = 'Device Recovery',
  additional_info = '- H. Nasrussabaa Hulhangubai (Ground Foor)  Zidhan Mohamed    / 9553300 / 9550330 / 7393300',
  assigned_to = '@FatrixShaquiell',
  last_assigned_at = '2026-08-13T05:02:10.39415+00:00',
  assignment_telegram_chat_id = NULL,
  assignment_telegram_message_id = NULL,
  updated_at = now()
WHERE ticket_number = '2024021010000101';

-- Create 2026081310000101 (Coverage Check — Gulisthaanuge / Sofvan)
INSERT INTO public.tickets_active (
  ticket_number,
  assigned_to,
  task_category,
  status,
  additional_info,
  last_assigned_at,
  assignment_telegram_chat_id,
  assignment_telegram_message_id
)
SELECT
  '2026081310000101',
  '@FatrixShaquiell',
  'Coverage Check',
  'Daily Task',
  'M. Gulisthaanuge, Fiyaathoshi Magu, 7th Floor - Sofvan 7222847',
  '2026-08-13T05:15:22.981549+00:00',
  -1001428974576,
  104000
WHERE NOT EXISTS (
  SELECT 1 FROM public.tickets_active WHERE ticket_number = '2026081310000101'
);

-- Move visit 803 (mistaken assignment) to the new ticket
UPDATE public.ticket_visits
SET ticket_number = '2026081310000101'
WHERE id = 803 AND ticket_number = '2024021010000101';

-- Re-open visit 802 on the restored ticket
UPDATE public.ticket_visits
SET visit_end = NULL, is_active = TRUE, outcome = 'assigned'
WHERE id = 802 AND ticket_number = '2024021010000101';
