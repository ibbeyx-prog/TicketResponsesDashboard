-- Move 5 resort/sales cases → residential (tickets_active), keep Resolved timestamps.
-- Case refs: 2026050310000043, 2026060127749364, 2026053127784626,
--            2026060170000009, 100671037

BEGIN;

INSERT INTO public.tickets_active (
  ticket_number,
  assigned_to,
  assigned_to_2,
  task_category,
  outcome_category,
  status,
  field_response,
  field_responded_by,
  additional_info,
  responded_at,
  updated_at,
  created_at,
  last_assigned_at
)
SELECT
  s.case_ref,
  s.assigned_to,
  s.assigned_to_2,
  COALESCE(NULLIF(trim(s.field_task_category), ''), NULLIF(trim(s.sales_category), ''), 'Coverage Check'),
  COALESCE(NULLIF(trim(s.field_task_category), ''), NULLIF(trim(s.sales_category), ''), 'Coverage Check'),
  'Resolved',
  s.field_response,
  s.field_responded_by,
  COALESCE(NULLIF(trim(s.additional_info), ''), NULLIF(trim(s.description), ''), NULLIF(trim(s.close_note), '')),
  s.responded_at,
  s.updated_at,
  s.created_at,
  s.last_assigned_at
FROM public.dashboard_sales_cases s
WHERE s.case_ref IN (
  '2026050310000043',
  '2026060127749364',
  '2026053127784626',
  '2026060170000009',
  '100671037'
)
AND NOT EXISTS (
  SELECT 1 FROM public.tickets_active t WHERE t.ticket_number = s.case_ref
);

INSERT INTO public.ticket_attendance_logs (
  ticket_number,
  member_username,
  action_type,
  note,
  timestamp
)
SELECT
  s.case_ref,
  '@dashboard-admin',
  'TransferredFromSales',
  'Moved from Resort/Sales Cases → Residential (**Resolved**). Timestamps preserved.',
  s.updated_at
FROM public.dashboard_sales_cases s
WHERE s.case_ref IN (
  '2026050310000043',
  '2026060127749364',
  '2026053127784626',
  '2026060170000009',
  '100671037'
);

DELETE FROM public.dashboard_sales_cases
WHERE case_ref IN (
  '2026050310000043',
  '2026060127749364',
  '2026053127784626',
  '2026060170000009',
  '100671037'
);

COMMIT;
