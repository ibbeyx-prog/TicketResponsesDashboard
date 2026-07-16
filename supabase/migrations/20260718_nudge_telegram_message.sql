-- Link 6h unattended nudge posts to Telegram message ids so field replies
-- to the reminder (not only the original assignment) can be captured.

alter table public.tickets_active
  add column if not exists nudge_telegram_chat_id bigint;

alter table public.tickets_active
  add column if not exists nudge_telegram_message_id bigint;

create index if not exists tickets_active_nudge_telegram_msg_idx
  on public.tickets_active (nudge_telegram_chat_id, nudge_telegram_message_id)
  where nudge_telegram_message_id is not null;
