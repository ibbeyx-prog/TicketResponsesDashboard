-- Link dashboard-posted assignments to the Telegram group message (for in-place edits).

alter table public.tickets_active
  add column if not exists assignment_telegram_chat_id bigint;

alter table public.tickets_active
  add column if not exists assignment_telegram_message_id bigint;
