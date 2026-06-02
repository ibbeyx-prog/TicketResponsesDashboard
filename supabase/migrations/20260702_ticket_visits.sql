-- Phase 2: ticket_visits table — one row per assignment cycle.
--
-- Each time a ticket is assigned or reassigned, a new visit row is OPENED
-- (visit_end IS NULL, outcome = 'assigned').
-- When the field engineer responds, the visit is CLOSED: visit_end set,
-- outcome = 'responded'.
-- If the ticket is reassigned before a response, the previous visit is
-- CLOSED with outcome = 'reassigned'.
-- If no response comes before assign-day cutoff, outcome = 'unattended'.
--
-- This is the source of truth for per-engineer multi-visit KPIs.

create table if not exists public.ticket_visits (
    id             bigserial primary key,
    ticket_number  text        not null references public.tickets_active(ticket_number) on delete cascade,
    assignee       text        not null,                 -- @username assigned for this visit
    visit_start    timestamptz not null default now(),   -- when this visit began (Assignment)
    visit_end      timestamptz,                          -- when closed (null = still open)
    outcome        text        not null default 'assigned'
                               check (outcome in ('assigned','responded','reassigned','unattended','on_hold')),
    response_note  text,                                 -- field_response text captured at close
    photo_url      text,                                 -- photo at time of response
    closed_by      text        not null default 'system' -- 'bot', 'dashboard', or 'system'
);

-- Fast lookups used for open-visit queries and per-person reporting
create index if not exists ticket_visits_ticket_idx   on public.ticket_visits(ticket_number);
create index if not exists ticket_visits_assignee_idx on public.ticket_visits(assignee);
create index if not exists ticket_visits_open_idx     on public.ticket_visits(ticket_number) where visit_end is null;
create index if not exists ticket_visits_start_idx    on public.ticket_visits(visit_start);

-- RLS: same anon policy pattern as tickets_active
alter table public.ticket_visits enable row level security;

create policy "anon read ticket_visits"
    on public.ticket_visits for select to anon using (true);

create policy "anon insert ticket_visits"
    on public.ticket_visits for insert to anon with check (true);

create policy "anon update ticket_visits"
    on public.ticket_visits for update to anon using (true);

create policy "anon delete ticket_visits"
    on public.ticket_visits for delete to anon using (true);
