-- Ensure field-reply columns exist on tickets_active.
--
-- The bot and dashboard expect `field_response` (latest Telegram text), `photo_url`,
-- and `responded_at` on the active ticket row. Older DBs created before these were
-- documented may be missing them: SELECT * then omits keys, Streamlit hides the
-- column, and the bot's missing-column retry can PATCH status without persisting text.
--
-- Idempotent: safe to re-run.

alter table public.tickets_active
  add column if not exists field_response text;

alter table public.tickets_active
  add column if not exists photo_url text;

alter table public.tickets_active
  add column if not exists responded_at timestamptz;
