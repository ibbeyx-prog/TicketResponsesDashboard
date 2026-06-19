# NetOps Coverage Eye — User Stories

**Format:** As a [role], I want [goal], so that [benefit].  
**Platform-agnostic:** Stories describe intent, not Streamlit-specific UI.  
**Roles covered:** Dashboard Admin, Dashboard Operator, Field Engineer, Sales Coordinator, System Automation

**Acceptance criteria** use **Given / When / Then** where helpful.

---

## Role Definitions

| Role | Who | Primary channel |
|------|-----|-----------------|
| **Dashboard Admin** | Senior ops lead (e.g. admin, ibeyx) | Web dashboard |
| **Dashboard Operator** | Coverage coordinator / dispatcher | Web dashboard |
| **Field Engineer** | On-site network engineer | Telegram group |
| **Sales Coordinator** | Sales intake owner (Mular_s, ibeyx queue) | Web dashboard |
| **System** | Bot + unattended cron | Background jobs |

---

# Dashboard Admin

## Authentication & team management

**US-ADM-01**  
As a **Dashboard Admin**, I want to create dashboard user accounts with usernames and temporary passwords, so that each coordinator has their own login and audit trail.

- **Given** I am signed in as admin and re-enter my password  
- **When** I add a user with username, operator display name, and password  
- **Then** the user can sign in and their Operator ID appears on assignments they make

**US-ADM-02**  
As a **Dashboard Admin**, I want to disable a user account without deleting history, so that former staff cannot access the dashboard but past actions remain auditable.

**US-ADM-03**  
As a **Dashboard Admin**, I want to manage which usernames have admin privileges via configuration, so that admin powers can be granted without code changes.

---

## CSM ticket control

**US-ADM-04**  
As a **Dashboard Admin**, I want to move a ticket to **On Hold**, so that I can pause field work while I chase the customer or internal teams.

- **Given** a ticket in Daily Task, Open, or Investigation  
- **When** I select On Hold  
- **Then** the ticket appears in the On Hold queue and an attendance log entry is recorded

**US-ADM-05**  
As a **Dashboard Admin**, I want to **record a field response manually** on behalf of an engineer, so that phone or in-person updates are captured when Telegram was not used.

**US-ADM-06**  
As a **Dashboard Admin**, I want to **reassign** a Daily Task or On Hold ticket to a different engineer, so that coverage continues when the original assignee is unavailable.

**US-ADM-07**  
As a **Dashboard Admin**, I want to **admin-close** a ticket with a note, so that duplicate or invalid tickets can be removed from active queues without a field visit.

**US-ADM-08**  
As a **Dashboard Admin**, I want to resolve a ticket with an **outcome category**, so that Performance reports credit the correct work type.

---

## Oversight & analytics

**US-ADM-09**  
As a **Dashboard Admin**, I want to see **unattended tickets** separately from active workload, so that I can hold engineers accountable without mixing them into daily queue counts for credit.

**US-ADM-10**  
As a **Dashboard Admin**, I want the **Performance Overview** to show solo vs shared tickets per engineer, so that I can see collaboration patterns and fair workload distribution.

**US-ADM-11**  
As a **Dashboard Admin**, I want **Handled** metrics to include both field tickets and sales cases in the selected date range, so that weekly reporting reflects total ops output.

**US-ADM-12**  
As a **Dashboard Admin**, I want to look up any ticket or sales case by ID in the **Case Info** matrix outside the current date range, so that I can investigate historical cases during escalations.

---

# Dashboard Operator

## Daily dispatch

**US-OP-01**  
As a **Dashboard Operator**, I want to assign a field ticket from the Command Center with ticket number, category, engineer(s), and notes, so that the engineer receives a Telegram assignment immediately.

- **Given** I have entered my Operator ID  
- **When** I click Assign  
- **Then** a row is created in tickets_active with status Daily Task, a visit cycle opens, and a Telegram message is posted

**US-OP-02**  
As a **Dashboard Operator**, I want to add a ticket to **Daily Task only** without notifying engineers, so that I can queue work for later dispatch.

**US-OP-03**  
As a **Dashboard Operator**, I want to add or remove field engineers and task categories from the Command Center, so that picklists stay current without a developer.

**US-OP-04**  
As a **Dashboard Operator**, I want clickable queue metrics (Assigned today, Responded today, etc.), so that I can jump directly to the relevant queue.

---

## Queue management (CSM)

**US-OP-05**  
As a **Dashboard Operator**, I want to review tickets in **Needs Review** after a field response, so that I can verify quality before closing or escalating.

**US-OP-06**  
As a **Dashboard Operator**, I want to move a ticket to **Under Investigation** with an optional follow-up date, so that long-running cases stay visible and tracked.

**US-OP-07**  
As a **Dashboard Operator**, I want to **reassign** an Open or Investigation ticket, so that I can redirect work without admin intervention.

**US-OP-08**  
As a **Dashboard Operator**, I want to **move a field ticket to Sales Cases**, so that sales-owned work continues in the correct pipeline without re-entering data.

**US-OP-09**  
As a **Dashboard Operator**, I want to search tickets within a queue by ticket number, so that I can find a case quickly during a live call.

**US-OP-10**  
As a **Dashboard Operator**, I want to view the **photo gallery** for a ticket, so that I can see all images the field engineer submitted.

**US-OP-11**  
As a **Dashboard Operator**, I want active queue tickets to remain visible even when outside the sidebar date range, so that I never lose sight of open work when filtering history.

---

## Sales cases

**US-OP-12**  
As a **Dashboard Operator**, I want to create a **sales case** from the Command Center with account, region, category, and priority, so that sales intake is tracked separately from field complaints.

**US-OP-13**  
As a **Dashboard Operator**, I want to advance a sales case through Investigation → Design → Resolved with comments, so that the sales pipeline reflects current state.

**US-OP-14**  
As a **Dashboard Operator**, I want to dispatch a **site visit** for regional sales cases by assigning a field engineer and optionally posting to Telegram, so that on-site work is coordinated like CSM tickets.

**US-OP-15**  
As a **Dashboard Operator**, I want to reopen a resolved sales case back to Design or Investigation, so that regressions or new customer feedback can be handled without a new case ref.

---

## Audit & reporting

**US-OP-16**  
As a **Dashboard Operator**, I want to view the **Log** filtered by date, ticket, and member, so that I can answer "who did what and when" during disputes.

**US-OP-17**  
As a **Dashboard Operator**, I want to filter **Performance** by focus assignee, so that I can review one engineer's handled work, visits, and sales credit for a week.

**US-OP-18**  
As a **Dashboard Operator**, I want the **Case Info** tab to show both field tickets and sales cases in one matrix, so that I can see staff involvement and notes for any case type in the period.

**US-OP-19**  
As a **Dashboard Operator**, I want to set the dashboard **time range** (today, this week, custom), so that Log and Performance reflect the period I am reporting on.

**US-OP-20**  
As a **Dashboard Operator**, I want optional **auto-refresh**, so that the queue updates while I keep the dashboard open on a monitoring screen.

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
As a **Field Engineer**, I want to reply to an assignment by **swipe-reply** in Telegram with text and optional photo, so that my response is logged without opening the dashboard.

- **Given** I am assigned ticket 100651990  
- **When** I swipe-reply with "Replaced ONT, signal restored" and a photo  
- **Then** the ticket moves to Needs Review, my response and photo are stored, and a visit cycle closes as responded

**US-FE-04**  
As a **Field Engineer**, I want to use `/respond` when swipe-reply is awkward, so that I can still submit updates from mobile.

**US-FE-05**  
As a **Field Engineer**, I want a **reminder nudge** if I have not responded within 6 hours, so that I do not forget an assignment during a busy day.

---

## Accountability

**US-FE-06**  
As a **Field Engineer**, I want my responses attributed to my `@handle`, so that Performance and visit credit reflect my actual work.

**US-FE-07**  
As a **Field Engineer**, I want tickets marked **unattended** only when I truly did not respond by end of assign day, so that the record is fair and I can explain exceptions to my lead.

---

# Sales Coordinator

**US-SAL-01**  
As a **Sales Coordinator**, I want new sales cases to land in the **Sales Ticket** queue with my attended-by label, so that I own intake from resorts and accounts.

**US-SAL-02**  
As a **Sales Coordinator**, I want to triage cases into **Investigation** or **Design** without field dispatch, so that desk research completes before site visits.

**US-SAL-03**  
As a **Sales Coordinator**, I want to assign a **regional site visit** to a field engineer for cases that need on-site survey, so that technical validation happens before design sign-off.

**US-SAL-04**  
As a **Sales Coordinator**, I want to close a case with a **close note**, so that the resolution reason is visible in Case Info and Handled reporting.

**US-SAL-05**  
As a **Sales Coordinator**, I want resolved sales cases to appear in **Performance → Handled** for the week they were completed, so that sales throughput is measured alongside field work.

**US-SAL-06**  
As a **Sales Coordinator**, I want sales cases to appear in **Performance → Case Info** with status, description, and notes, so that I can review case history without opening the Sales Cases queue.

---

# System Automation

**US-SYS-01**  
As the **System**, I want to send a Telegram nudge 6 hours after assignment when no field response exists, so that tickets are less likely to miss the assign-day cutoff.

**US-SYS-02**  
As the **System**, I want to auto-close unanswered Daily Task tickets after the assign-day cutoff (UTC+5), so that unattended work is flagged for admin review.

- **Given** a ticket assigned today with no response by 23:59 UTC+5  
- **When** the unattended job runs  
- **Then** status becomes Unattended, marked_unattended_at is set, ticket also routes to Open, and attendance log records AutoUnattended

**US-SYS-03**  
As the **System**, I want to maintain one **active visit row** per ticket, so that reassignment history is accurate for Performance matrix and fair credit.

**US-SYS-04**  
As the **System**, I want to append every assignment and response to the **attendance log**, so that the Log tab is a complete audit trail.

**US-SYS-05**  
As the **System**, I want to update `updated_at` on every ticket change, so that time-range filters reflect latest activity.

**US-SYS-06**  
As the **System**, I want to parse coordinator assignment messages in Telegram when configured, so that assignments posted by coordinators create or update tickets automatically.

---

# Cross-Role Epics (End-to-End)

## Epic A: Standard field complaint resolution

| Step | Role | Story ref |
|------|------|-----------|
| 1. Assign ticket | Operator | US-OP-01 |
| 2. Receive Telegram | Field Engineer | US-FE-01 |
| 3. Respond with photo | Field Engineer | US-FE-03 |
| 4. Review in Open queue | Operator | US-OP-05 |
| 5. Resolve with category | Operator / Admin | US-ADM-08 |
| 6. Appears in Handled | Admin | US-ADM-11 |

## Epic B: Unattended escalation

| Step | Role | Story ref |
|------|------|-----------|
| 1. Assign, no response | Operator | US-OP-01 |
| 2. 6h nudge | System | US-SYS-01 |
| 3. End-of-day auto-close | System | US-SYS-02 |
| 4. Review in Open + Unattended | Admin | US-ADM-09 |

## Epic C: Sales case with site visit

| Step | Role | Story ref |
|------|------|-----------|
| 1. Create sales case | Operator | US-OP-12 |
| 2. Move to Investigation | Sales Coordinator | US-SAL-02 |
| 3. Dispatch site visit | Operator | US-OP-14 |
| 4. Field engineer responds | Field Engineer | US-FE-03 |
| 5. Design → Resolved | Sales Coordinator | US-SAL-04 |
| 6. Credit in Performance | Admin | US-ADM-11, US-SAL-05 |

## Epic D: Field ticket → Sales handoff

| Step | Role | Story ref |
|------|------|-----------|
| 1. Open CSM ticket | Operator | US-OP-05 |
| 2. Move to Sales | Operator | US-OP-08 |
| 3. Continue in Sales queue | Sales Coordinator | US-SAL-01 |

---

# Story Map by Screen

| Screen | Primary stories |
|--------|-----------------|
| Login | US-ADM-01, US-OP-* (auth) |
| Command Center | US-OP-01, US-OP-02, US-OP-03, US-OP-12 |
| CSM Cases queues | US-OP-05–11, US-ADM-04–08 |
| Sales Cases | US-OP-12–15, US-SAL-01–06 |
| Log | US-OP-16 |
| Performance | US-ADM-09–12, US-OP-17–18 |
| Telegram (field) | US-FE-01–07 |
| Background jobs | US-SYS-01–06 |

---

# Non-Functional User Expectations

| ID | Story |
|----|-------|
| US-NFR-01 | As any dashboard user, I want dates shown in **UTC+5**, so that reports match local operations. |
| US-NFR-02 | As any dashboard user, I want the dashboard to work when Supabase is reachable, so that queues reflect live data. |
| US-NFR-03 | As a Field Engineer, I want photo uploads to succeed on mobile Telegram, so that site evidence is captured. |
| US-NFR-04 | As an Admin, I want deleted tickets to leave attendance logs intact, so that audits are never silently erased. |

---

## Related Documents

- [DEVELOPER_REQUIREMENTS.md](./DEVELOPER_REQUIREMENTS.md) — full technical requirements
- [DATABASE_SCHEMA.md](./DATABASE_SCHEMA.md) — tables and fields
