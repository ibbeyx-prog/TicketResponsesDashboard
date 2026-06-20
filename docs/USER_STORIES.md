# NetOps Coverage Eye — User Stories

**Format:** As a **[role]**, I want **[goal]**, so that **[benefit]**.  
**Platform-agnostic:** Stories describe intent, not Streamlit-specific widgets.  
**Version:** 2026-06 (dispatch console + Performance rebuild)

**Related docs:** [DEVELOPER_REQUIREMENTS.md](./DEVELOPER_REQUIREMENTS.md) · [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md)

---

## Role Definitions

| Role | Who | Primary channel |
|------|-----|-----------------|
| **Dashboard Admin** | Senior ops lead (usernames in `DASHBOARD_ADMIN_USERNAMES`, e.g. admin, ibeyx) | Web dashboard |
| **Dashboard Operator** | Coverage coordinator / dispatcher | Web dashboard |
| **Field Engineer** | On-site network engineer | Telegram group |
| **Sales Coordinator** | Sales intake owner (same dashboard login as operator) | Web dashboard |
| **System** | Bot + unattended cron | Background jobs |

---

# Dashboard Admin

## Authentication & team management

**US-ADM-01**  
As a **Dashboard Admin**, I want to create dashboard user accounts with usernames and passwords, so that each coordinator has their own login and audit trail.

- **Given** I re-enter my admin password for RPC authorization  
- **When** I create a user with username, operator display name, and password  
- **Then** the user can sign in and their Operator ID appears on assignments they make

**US-ADM-02**  
As a **Dashboard Admin**, I want to disable a user account without deleting history, so that former staff cannot access the dashboard but past actions remain auditable.

**US-ADM-03**  
As a **Dashboard Admin**, I want admin privileges controlled by configuration (`DASHBOARD_ADMIN_USERNAMES`), so that admin powers can be granted without code changes.

**US-ADM-04**  
As a **Dashboard Admin**, I want to reset my password via a time-limited code, so that I can recover access without IT intervention.

---

## CSM ticket control (admin-only)

**US-ADM-05**  
As a **Dashboard Admin**, I want to move a ticket to **On Hold**, so that I can pause field work while I chase the customer or internal teams.

- **Given** a ticket in Daily Task, Needs Review, or Investigation  
- **When** I select On Hold from the row menu  
- **Then** the ticket appears in the On Hold queue and an attendance log entry is recorded

**US-ADM-06**  
As a **Dashboard Admin**, I want to **record a field response manually**, so that phone or in-person updates are captured when Telegram was not used.

**US-ADM-07**  
As a **Dashboard Admin**, I want to **reassign** a Daily Task or On Hold ticket to a different engineer, so that coverage continues when the original assignee is unavailable.

**US-ADM-08**  
As a **Dashboard Admin**, I want to **admin-close** a ticket with a note, so that duplicate or invalid tickets leave active queues without a field visit.

**US-ADM-09**  
As a **Dashboard Admin**, I want to resolve a ticket with an **outcome category**, so that Performance reports credit the correct work type.

---

## Oversight & analytics

**US-ADM-10**  
As a **Dashboard Admin**, I want **unattended tickets** tracked separately from active workload, so that I can hold engineers accountable without mixing them into daily credit counts.

**US-ADM-11**  
As a **Dashboard Admin**, I want the **Performance Overview** to show solo vs shared tickets per engineer based on **assignment** (one vs two assignees on the ticket), so that I see collaboration patterns on the current queue snapshot.

**US-ADM-12**  
As a **Dashboard Admin**, I want undispatched sales cases credited to a single **Admin** bucket in Performance, so that queue ownership is clear and individual admin handles are not double-counted.

**US-ADM-13**  
As a **Dashboard Admin**, I want **Handled** metrics to include both field tickets and sales cases in the selected date range, so that weekly reporting reflects total ops output.

**US-ADM-14**  
As a **Dashboard Admin**, I want to drill from Performance into a specific ticket or sales case on the CSM/Sales floor, so that I can act immediately during escalations.

**US-ADM-15**  
As a **Dashboard Admin**, I want the **Weekly** Performance report to use a fixed Sun–Sat week in UTC+5, so that executive numbers match local operations calendars.

---

# Dashboard Operator

## Dispatch floor (CSM)

**US-OP-01**  
As a **Dashboard Operator**, I want a **three-column CSM floor** (sidebar queues, center assign + table, right detail), so that I can dispatch and review without switching screens.

**US-OP-02**  
As a **Dashboard Operator**, I want to assign a field ticket with ticket number, category, engineer(s), and notes from the center panel, so that the engineer receives a Telegram assignment immediately.

- **Given** I am signed in with Operator ID  
- **When** I click Assign (Telegram mode)  
- **Then** a row is created with status Daily Task, a visit cycle opens, and Telegram posts the assignment

**US-OP-03**  
As a **Dashboard Operator**, I want to add a ticket to **Daily Task only** without Telegram, so that I can queue work for later dispatch.

**US-OP-04**  
As a **Dashboard Operator**, I want to assign a **second engineer** on shared tickets, so that paired jobs show as shared in Performance.

**US-OP-05**  
As a **Dashboard Operator**, I want to manage field engineers and task categories from the floor, so that picklists stay current without a developer.

**US-OP-06**  
As a **Dashboard Operator**, I want clickable queue counts in the left sidebar, so that I can switch queues in one click.

**US-OP-07**  
As a **Dashboard Operator**, I want to search tickets within the active queue, so that I can find a case quickly during a live call.

**US-OP-08**  
As a **Dashboard Operator**, I want the right detail panel to show status, engineers, notes, field response, photo, and recent attendance, so that I have full context on the selected row.

---

## Queue management (CSM)

**US-OP-09**  
As a **Dashboard Operator**, I want to review tickets in **Needs Review** after a field response, so that I can verify quality before closing or escalating.

**US-OP-10**  
As a **Dashboard Operator**, I want to move a ticket to **Under Investigation** with an optional follow-up date, so that long-running cases stay visible.

**US-OP-11**  
As a **Dashboard Operator**, I want to **reassign** a Needs Review or Investigation ticket without admin help, so that I can redirect work quickly.

**US-OP-12**  
As a **Dashboard Operator**, I want to **move a field ticket to Sales Cases**, so that sales-owned work continues in the correct pipeline.

**US-OP-13**  
As a **Dashboard Operator**, I want to view the **photo gallery** for a ticket, so that I can see all images the field engineer submitted.

**US-OP-14**  
As a **Dashboard Operator**, I want active queue tickets to remain visible even when outside the global time range, so that I never lose open work when filtering history.

**US-OP-15**  
As a **Dashboard Operator**, I want row **⋯ menus** for status moves, edit, reassign, record response, admin close, and photos, so that actions are one click away on the selected ticket.

---

## Sales cases floor

**US-OP-16**  
As a **Dashboard Operator**, I want a **three-column Sales floor** matching CSM layout, so that I learn one interaction pattern for both pipelines.

**US-OP-17**  
As a **Dashboard Operator**, I want to create a **sales case** with account, region, category, and priority, so that sales intake is tracked separately from field complaints.

**US-OP-18**  
As a **Dashboard Operator**, I want to choose **intake only** vs **assign engineer** when creating a case, so that desk triage does not always trigger field dispatch.

**US-OP-19**  
As a **Dashboard Operator**, I want to advance a sales case through Investigation → Design → Resolved with notes, so that the pipeline reflects current state.

**US-OP-20**  
As a **Dashboard Operator**, I want to dispatch a **site visit** by assigning field engineer(s) to a regional case, so that on-site work is coordinated like CSM tickets.

**US-OP-21**  
As a **Dashboard Operator**, I want to reopen a resolved sales case, so that regressions can be handled without a new case ref.

---

## Settings, Log & Performance

**US-OP-22**  
As a **Dashboard Operator**, I want **Settings (⚙)** in the header for time range, auto-refresh, and sign-out, so that global filters are always reachable.

**US-OP-23**  
As a **Dashboard Operator**, I want to set the dashboard **time range** (today, this week, custom), so that Log and Performance reflect the period I am reporting on.

**US-OP-24**  
As a **Dashboard Operator**, I want optional **auto-refresh**, so that the floor updates while I monitor on a wall screen.

**US-OP-25**  
As a **Dashboard Operator**, I want to view the **Log** filtered by date, ticket, and member, so that I can answer "who did what and when" during disputes.

**US-OP-26**  
As a **Dashboard Operator**, I want to filter **Performance** by focus assignee and view (Overview, Weekly, Handled, etc.), so that I can review one engineer's workload for a period.

**US-OP-27**  
As a **Dashboard Operator**, I want the **Case Info** view to show field tickets and sales cases in one staff matrix with comments and photos, so that I see all involvement for a case in the period.

**US-OP-28**  
As a **Dashboard Operator**, I want to jump from Performance **Unattended** to the CSM Unattended queue filtered by engineer, so that I can follow up on accountability items immediately.

---

# Field Engineer

## Receiving work

**US-FE-01**  
As a **Field Engineer**, I want to receive a Telegram message when a ticket is assigned to me, so that I know the ticket number, category, and notes before visiting site.

**US-FE-02**  
As a **Field Engineer**, I want assignments to show a second engineer when paired, so that we know shared responsibility on large jobs.

---

## Responding

**US-FE-03**  
As a **Field Engineer**, I want to reply by **swipe-reply** in Telegram with text and optional photo, so that my response is logged without opening the dashboard.

- **Given** I am assigned ticket 100651990  
- **When** I swipe-reply with "Replaced ONT, signal restored" and a photo  
- **Then** the ticket moves to Needs Review, my response and photo are stored, and the visit closes as responded

**US-FE-04**  
As a **Field Engineer**, I want to use `/respond` when swipe-reply is awkward, so that I can submit updates from mobile.

**US-FE-05**  
As a **Field Engineer**, I want a **reminder nudge** if I have not responded within 6 hours, so that I do not miss an assignment during a busy day.

---

## Accountability

**US-FE-06**  
As a **Field Engineer**, I want my responses attributed to my `@handle`, so that Performance and visit credit reflect my actual work.

**US-FE-07**  
As a **Field Engineer**, I want tickets marked **unattended** only when I did not respond by end of assign day (UTC+5), so that the record is fair and I can explain exceptions to my lead.

**US-FE-08**  
As a **Field Engineer**, I want shared tickets to credit both assignees in Performance, so that collaboration is visible in workload reports.

---

# Sales Coordinator

**US-SAL-01**  
As a **Sales Coordinator**, I want new sales cases to appear in the **Sales ticket** queue, so that intake from resorts and accounts is tracked centrally.

**US-SAL-02**  
As a **Sales Coordinator**, I want to triage cases into **Investigation** or **Design** without field dispatch, so that desk research completes before site visits.

**US-SAL-03**  
As a **Sales Coordinator**, I want to assign a **regional site visit** to a field engineer when on-site survey is required, so that technical validation happens before design sign-off.

**US-SAL-04**  
As a **Sales Coordinator**, I want to close a case with a **close note**, so that the resolution reason is visible in Case Info and Handled reporting.

**US-SAL-05**  
As a **Sales Coordinator**, I want resolved sales cases in **Performance → Handled** for the completion week, so that sales throughput is measured alongside field work.

**US-SAL-06**  
As a **Sales Coordinator**, I want undispatched cases to appear under the **Admin** performance bucket, so that queue backlog is visible without crediting individual operators.

**US-SAL-07**  
As a **Sales Coordinator**, I want sales cases in **Case Info** with status, description, and notes, so that I can review history without opening the Sales floor.

---

# System Automation

**US-SYS-01**  
As the **System**, I want to send a Telegram nudge 6 hours after assignment when no field response exists, so that tickets are less likely to miss the assign-day cutoff.

**US-SYS-02**  
As the **System**, I want to auto-close unanswered Daily Task tickets after the assign-day cutoff (UTC+5), so that unattended work is flagged for admin review.

- **Given** a ticket assigned today with no response by 23:59 UTC+5  
- **When** the unattended job runs  
- **Then** status becomes Unattended, `marked_unattended_at` is set, ticket routes to Open for review, and log records `AutoUnattended`

**US-SYS-03**  
As the **System**, I want to maintain one **active visit row** per ticket, so that reassignment history is accurate for Performance and fair credit.

**US-SYS-04**  
As the **System**, I want to append every assignment and response to the **attendance log**, so that the Log tab is a complete audit trail.

**US-SYS-05**  
As the **System**, I want to update `updated_at` on every ticket change, so that time-range filters reflect latest activity.

**US-SYS-06**  
As the **System**, I want to close the prior visit with outcome `reassigned` when a new assignee is inserted, so that visit timelines reflect handoffs accurately.

---

# Cross-Role Epics (End-to-End)

## Epic A: Standard field complaint resolution

| Step | Role | Story ref |
|------|------|-----------|
| 1. Assign ticket | Operator | US-OP-02 |
| 2. Receive Telegram | Field Engineer | US-FE-01 |
| 3. Respond with photo | Field Engineer | US-FE-03 |
| 4. Review in Needs Review | Operator | US-OP-09 |
| 5. Resolve with category | Operator / Admin | US-ADM-09 |
| 6. Appears in Handled | Admin | US-ADM-13 |

## Epic B: Unattended escalation

| Step | Role | Story ref |
|------|------|-----------|
| 1. Assign, no response | Operator | US-OP-02 |
| 2. 6h nudge | System | US-SYS-01 |
| 3. End-of-day auto-close | System | US-SYS-02 |
| 4. Review in Open + Unattended | Admin | US-ADM-10, US-OP-28 |

## Epic C: Sales case with site visit

| Step | Role | Story ref |
|------|------|-----------|
| 1. Create sales case | Operator | US-OP-17 |
| 2. Move to Investigation | Sales Coordinator | US-SAL-02 |
| 3. Dispatch site visit | Operator | US-OP-20 |
| 4. Field engineer responds | Field Engineer | US-FE-03 |
| 5. Design → Resolved | Sales Coordinator | US-SAL-04 |
| 6. Credit in Performance | Admin | US-ADM-13, US-SAL-05 |

## Epic D: Field ticket → Sales handoff

| Step | Role | Story ref |
|------|------|-----------|
| 1. Open CSM ticket | Operator | US-OP-09 |
| 2. Move to Sales | Operator | US-OP-12 |
| 3. Continue in Sales queue | Sales Coordinator | US-SAL-01 |

## Epic E: Shared assignment performance

| Step | Role | Story ref |
|------|------|-----------|
| 1. Assign two engineers | Operator | US-OP-04 |
| 2. Both see Telegram | Field Engineer | US-FE-02 |
| 3. Overview shows shared for both | Admin | US-ADM-11, US-FE-08 |

---

# Story Map by Screen

| Screen | Primary stories |
|--------|-----------------|
| Login | US-ADM-01, US-ADM-04 |
| CSM Cases (3-column floor) | US-OP-01–15, US-ADM-05–09 |
| Sales Cases (3-column floor) | US-OP-16–21, US-SAL-01–07 |
| Log | US-OP-25 |
| Performance | US-ADM-10–15, US-OP-26–28 |
| Settings popover | US-OP-22–24 |
| Telegram (field) | US-FE-01–08 |
| Background jobs | US-SYS-01–06 |

---

# Non-Functional User Expectations

| ID | Story |
|----|-------|
| US-NFR-01 | As any dashboard user, I want dates shown in **UTC+5**, so that reports match local operations. |
| US-NFR-02 | As any dashboard user, I want the dashboard to reflect live Supabase data when reachable, so that queues are trustworthy. |
| US-NFR-03 | As a Field Engineer, I want photo uploads to succeed on mobile Telegram, so that site evidence is captured. |
| US-NFR-04 | As an Admin, I want deleted tickets to leave attendance logs intact, so that audits are never silently erased. |
| US-NFR-05 | As any dashboard user, I want micro labels at least **9px**, so that small text remains readable on ops monitors. |
| US-NFR-06 | As an Operator, I want cross-tab jumps from Performance to preserve ticket selection, so that drill-down does not lose context. |
