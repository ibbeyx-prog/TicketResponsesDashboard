import { ExternalLink, ImageIcon, MessageSquare } from "lucide-react";
import type { Ticket, TicketComment } from "../types/ticket";

interface CaseInfoPanelProps {
  ticket: Ticket | null;
  staffColors: Record<string, string>;
  staffCount: number;
}

function staffRoleLabel(outcome: string | undefined): string {
  if (outcome === "active") return "Active Dev";
  if (outcome === "responded") return "Peer Reviewer";
  if (outcome === "reassigned") return "Handoff";
  if (outcome === "on_hold") return "On Hold";
  if (outcome === "unattended") return "Observer";
  return "Contributor";
}

function commentKindLabel(kind: TicketComment["kind"]): string {
  if (kind === "visit") return "Visit";
  if (kind === "assignment") return "Assignment";
  if (kind === "field") return "Field";
  return "Response";
}

export function CaseInfoPanel({ ticket, staffColors, staffCount }: CaseInfoPanelProps) {
  if (!ticket) {
    return (
      <aside className="flex w-[300px] shrink-0 flex-col border-l border-dashboard-border bg-[#121212]">
        <div className="border-b border-dashboard-border px-4 py-4">
          <h2 className="text-sm font-semibold text-dashboard-text">Case Info</h2>
        </div>
        <p className="px-4 py-6 text-xs text-dashboard-muted">
          Select a row to view case details and staff involvement.
        </p>
      </aside>
    );
  }

  const comments = ticket.comments ?? [];
  const photos = ticket.photos ?? [];
  const hasCaseActivity = comments.length > 0 || photos.length > 0;

  const involved = ticket.assignedStaff.filter(
    (s) => ticket.staffAssignments[s]?.outcome !== "unattended",
  );
  const observers = ticket.assignedStaff.filter(
    (s) => ticket.staffAssignments[s]?.outcome === "unattended",
  );

  return (
    <aside className="flex w-[300px] shrink-0 flex-col border-l border-dashboard-border bg-[#121212]">
      <div className="border-b border-dashboard-border px-4 py-3">
        <h2 className="text-sm font-semibold text-white">Case Info</h2>
      </div>

      <div className="flex-1 space-y-5 overflow-y-auto px-4 py-4 text-xs">
        <dl className="space-y-2.5">
          <div>
            <dt className="text-dashboard-muted">Case #</dt>
            <dd className="mt-0.5 font-medium text-dashboard-text">{ticket.caseLabel}</dd>
          </div>
          <div>
            <dt className="text-dashboard-muted">Ticket ID</dt>
            <dd className="mt-0.5 font-medium text-dashboard-accent">{ticket.id}</dd>
          </div>
          <div>
            <dt className="text-dashboard-muted">Priority</dt>
            <dd className="mt-0.5 font-medium">{ticket.priority}</dd>
          </div>
          <div>
            <dt className="text-dashboard-muted">Avg. Staff per Case</dt>
            <dd className="mt-0.5 font-medium">{staffCount}</dd>
          </div>
          <div>
            <dt className="text-dashboard-muted">Status</dt>
            <dd className="mt-0.5 font-medium">{ticket.displayStatus}</dd>
          </div>
        </dl>

        <div>
          <h3 className="mb-2 text-[10px] font-semibold uppercase tracking-wide text-dashboard-muted">
            Staffs
          </h3>
          <p className="mb-2 text-[10px] text-dashboard-muted">Involved</p>
          <ul className="space-y-2">
            {involved.map((staff) => {
              const a = ticket.staffAssignments[staff];
              const color = staffColors[staff] ?? "#9ec5e8";
              return (
                <li key={staff} className="flex items-center gap-2">
                  <span
                    className="h-2.5 w-2.5 shrink-0 rounded-full"
                    style={{ backgroundColor: color }}
                  />
                  <span className="min-w-0 flex-1 truncate font-medium text-dashboard-text">
                    {staff}
                  </span>
                  <span className="shrink-0 text-[10px] text-dashboard-muted">
                    {a?.role ?? staffRoleLabel(a?.outcome)}
                  </span>
                </li>
              );
            })}
          </ul>
          {observers.length > 0 ? (
            <>
              <p className="mb-2 mt-4 text-[10px] text-dashboard-muted">Observer</p>
              <ul className="space-y-2">
                {observers.map((staff) => {
                  const color = staffColors[staff] ?? "#8fa89e";
                  return (
                    <li key={staff} className="flex items-center gap-2">
                      <span
                        className="h-2.5 w-2.5 shrink-0 rounded-full"
                        style={{ backgroundColor: color }}
                      />
                      <span className="truncate font-medium text-dashboard-text">{staff}</span>
                    </li>
                  );
                })}
              </ul>
            </>
          ) : null}
        </div>

        {hasCaseActivity ? (
          <>
            <div>
              <h3 className="mb-2 flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wide text-dashboard-muted">
                <MessageSquare className="h-3 w-3" />
                Comments
                {comments.length > 0 ? (
                  <span className="font-normal normal-case text-dashboard-muted">
                    ({comments.length})
                  </span>
                ) : null}
              </h3>
              {comments.length > 0 ? (
                <ul className="space-y-2">
                  {comments.map((item, idx) => (
                    <li
                      key={`${item.at}-${item.author}-${idx}`}
                      className="rounded-md border border-dashboard-border bg-[#1a1a1a] px-2.5 py-2"
                    >
                      <div className="flex items-baseline justify-between gap-2">
                        <span className="truncate font-medium text-dashboard-text">
                          {item.author}
                        </span>
                        <span className="shrink-0 text-[10px] text-dashboard-muted">
                          {commentKindLabel(item.kind)}
                        </span>
                      </div>
                      {item.at ? (
                        <p className="mt-0.5 text-[10px] text-dashboard-muted">{item.at}</p>
                      ) : null}
                      <p className="mt-1.5 whitespace-pre-wrap break-words text-[11px] leading-snug text-dashboard-text">
                        {item.text}
                      </p>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-[10px] text-dashboard-muted">No comment text for this ticket.</p>
              )}
            </div>

            <div>
              <h3 className="mb-2 flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-wide text-dashboard-muted">
                <ImageIcon className="h-3 w-3" />
                Photos
                {photos.length > 0 ? (
                  <span className="font-normal normal-case text-dashboard-muted">
                    ({photos.length})
                  </span>
                ) : null}
              </h3>
              {photos.length > 0 ? (
                <ul className="space-y-1.5">
                  {photos.map((item, idx) => (
                    <li key={`${item.url}-${idx}`}>
                      <a
                        href={item.url}
                        target="_blank"
                        rel="noopener noreferrer"
                        className="flex items-center gap-2 rounded-md border border-dashboard-border bg-[#1a1a1a] px-2.5 py-2 transition-colors hover:border-dashboard-accent/40"
                      >
                        <span className="flex h-7 w-7 shrink-0 items-center justify-center rounded bg-[#252525] text-dashboard-accent">
                          <ImageIcon className="h-3.5 w-3.5" />
                        </span>
                        <span className="min-w-0 flex-1">
                          <span className="flex items-center gap-1 text-[11px] font-medium text-dashboard-accent">
                            Photo {photos.length > 1 ? idx + 1 : ""}
                            <ExternalLink className="h-3 w-3 shrink-0 opacity-70" />
                          </span>
                          <span className="block truncate text-[10px] text-dashboard-muted">
                            {item.author}
                            {item.at ? ` · ${item.at}` : ""}
                          </span>
                        </span>
                      </a>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-[10px] text-dashboard-muted">No photos uploaded for this ticket.</p>
              )}
            </div>
          </>
        ) : (
          <p className="text-[10px] text-dashboard-muted">
            No responses or comments recorded for this ticket.
          </p>
        )}
      </div>
    </aside>
  );
}
