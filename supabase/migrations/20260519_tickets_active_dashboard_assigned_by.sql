-- Who used the Streamlit Command Center to assign (display name / operator id).
-- Telegram-originated assignments clear this to NULL in ``bot.py`` on insert/reassign.

alter table public.tickets_active
  add column if not exists dashboard_assigned_by text;
