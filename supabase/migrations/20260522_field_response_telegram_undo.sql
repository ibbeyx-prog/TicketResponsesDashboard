-- Link field replies to Telegram messages so undo-on-delete (1h window) can clear the dashboard.

alter table public.tickets_active
  add column if not exists last_response_telegram_chat_id bigint;

alter table public.tickets_active
  add column if not exists last_response_telegram_message_id bigint;

alter table public.ticket_attendance_logs
  add column if not exists telegram_chat_id bigint;

alter table public.ticket_attendance_logs
  add column if not exists telegram_message_id bigint;

create index if not exists ticket_attendance_logs_telegram_msg_idx
  on public.ticket_attendance_logs (telegram_chat_id, telegram_message_id)
  where telegram_message_id is not null;
