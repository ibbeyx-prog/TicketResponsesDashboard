import type {
  MatrixPayload,
  MatrixSummary,
  StaffOutcome,
  Ticket,
  TicketActivity,
  TicketComment,
  TicketPhoto,
} from "../types/ticket";

function splitLegacyActivities(activities: TicketActivity[] | undefined): {
  comments: TicketComment[];
  photos: TicketPhoto[];
} {
  if (!activities?.length) return { comments: [], photos: [] };
  const commentKeys = new Set<string>();
  const photoUrls = new Set<string>();
  const comments: TicketComment[] = [];
  const photos: TicketPhoto[] = [];
  for (const item of activities) {
    const text = (item.text || "").trim();
    if (text) {
      const key = `${item.author.toLowerCase()}|${text.toLowerCase().slice(0, 220)}`;
      if (!commentKeys.has(key)) {
        commentKeys.add(key);
        comments.push({
          at: item.at,
          author: item.author,
          text,
          kind: item.kind,
        });
      }
    }
    const url = (item.photoUrl || "").trim();
    if (url.startsWith("http") && !photoUrls.has(url)) {
      photoUrls.add(url);
      photos.push({ at: item.at, author: item.author, url });
    }
  }
  return { comments, photos };
}

function mergeCaseInfo(raw: Partial<Ticket>): { comments: TicketComment[]; photos: TicketPhoto[] } {
  const fromLegacy = splitLegacyActivities(
    Array.isArray((raw as { activities?: TicketActivity[] }).activities)
      ? (raw as { activities: TicketActivity[] }).activities
      : undefined,
  );
  const comments = Array.isArray(raw.comments) ? raw.comments : fromLegacy.comments;
  const photos = Array.isArray(raw.photos) ? raw.photos : fromLegacy.photos;
  return { comments, photos };
}

function randOutcome(i: number, j: number): StaffOutcome {
  const outcomes: StaffOutcome[] = [
    "active",
    "assigned",
    "responded",
    "reassigned",
    "on_hold",
    "unattended",
  ];
  return outcomes[(i * 7 + j * 3) % outcomes.length];
}

function mapDisplayStatus(status: string): string {
  const m: Record<string, string> = {
    Active: "In Progress",
    Responded: "Resolved",
    Assigned: "Open",
    Reassigned: "In Progress",
    "On Hold": "On Hold",
    Unattended: "Open",
    Unknown: "Open",
  };
  return m[status] ?? status;
}

function staffRole(outcome: StaffOutcome): string {
  if (outcome === "active") return "Active Dev";
  if (outcome === "responded") return "Peer Reviewer";
  if (outcome === "reassigned") return "Handoff";
  if (outcome === "unattended") return "Observer";
  if (outcome === "on_hold") return "On Hold";
  return "Contributor";
}

function caseLabel(id: string, priority: string, index: number): string {
  const code = { Critical: "C", High: "H", Normal: "N", Low: "L" }[priority] ?? "N";
  return `Case #${id} (${code}-${String((index % 99) + 1).padStart(2, "0")})`;
}

function buildSummary(tickets: Ticket[]): MatrixSummary {
  if (!tickets.length) {
    return {
      totalCases: 0,
      avgStaffPerCase: 0,
      topCollaborativeCaseId: "",
      topCollaborativeStaffCount: 0,
    };
  }
  let top = tickets[0];
  let sum = 0;
  for (const t of tickets) {
    sum += t.assignedStaff.length;
    if (t.assignedStaff.length > top.assignedStaff.length) top = t;
  }
  return {
    totalCases: tickets.length,
    avgStaffPerCase: sum / tickets.length,
    topCollaborativeCaseId: top.id,
    topCollaborativeStaffCount: top.assignedStaff.length,
  };
}

function enrichTicket(raw: Partial<Ticket>, index: number): Ticket {
  const id = String(raw.id ?? "");
  const priority = (raw.priority as Ticket["priority"]) ?? "Normal";
  const status = String(raw.status ?? "Unknown");
  const assignedStaff = Array.isArray(raw.assignedStaff) ? raw.assignedStaff : [];
  const staffAssignments = (raw.staffAssignments ?? {}) as Ticket["staffAssignments"];
  for (const staff of assignedStaff) {
    if (staffAssignments[staff] && !staffAssignments[staff].role) {
      staffAssignments[staff].role = staffRole(staffAssignments[staff].outcome);
    }
  }
  const { comments, photos } = mergeCaseInfo(raw);
  return {
    id,
    caseLabel: raw.caseLabel ?? caseLabel(id, priority, index),
    status,
    displayStatus: raw.displayStatus ?? mapDisplayStatus(status),
    priority,
    assignedStaff,
    staffAssignments,
    isShared: Boolean(raw.isShared ?? assignedStaff.length > 1),
    comments,
    photos,
  };
}

/** Demo payload for local Vite dev (1000+ rows). */
export function buildDemoPayload(rowCount = 1200): MatrixPayload {
  const staffMembers = [
    "@dissiby",
    "@fatrixshaquiell",
    "@nallu10",
    "@alexj",
    "@maria_s",
  ];
  const staffColors: Record<string, string> = {
    "@dissiby": "#9ec5e8",
    "@fatrixshaquiell": "#D7B491",
    "@nallu10": "#b8d4a8",
    "@alexj": "#8fa89e",
    "@maria_s": "#7eb8da",
  };

  const tickets: Ticket[] = [];
  for (let i = 0; i < rowCount; i += 1) {
    const id = `1006${String(10000 + i)}`;
    const staffCount = i % 5 === 0 ? 5 : i % 3 === 0 ? 2 : 1;
    const assignedStaff = staffMembers.slice(0, Math.min(staffCount, staffMembers.length));
    const staffAssignments: Ticket["staffAssignments"] = {};
    assignedStaff.forEach((staff, j) => {
      const outcome = randOutcome(i, j);
      staffAssignments[staff] = {
        outcome,
        label: outcome.replace("_", " "),
        role: staffRole(outcome),
      };
    });
    const status =
      outcomeStatus(assignedStaff.map((s) => staffAssignments[s].outcome)) ?? "Open";
    const demoCaseInfo =
      i % 7 === 0
        ? {
            comments: [
              {
                at: "2026-06-02 14:30",
                author: assignedStaff[0] ?? "@dissiby",
                text: "Site checked — panel replaced, awaiting sign-off.",
                kind: "response" as const,
              },
              {
                at: "2026-06-01 09:15",
                author: "@dashboard-admin",
                text: "Reassigned after no response within window.",
                kind: "assignment" as const,
              },
            ],
            photos: [
              {
                at: "2026-06-02 14:30",
                author: assignedStaff[0] ?? "@dissiby",
                url: "https://example.com/field-photo.jpg",
              },
            ],
          }
        : { comments: [], photos: [] };
    tickets.push(
      enrichTicket(
        {
          id,
          status,
          priority: staffCount >= 3 ? "High" : staffCount > 1 ? "Normal" : "Low",
          assignedStaff,
          staffAssignments,
          isShared: staffCount > 1,
          ...demoCaseInfo,
        },
        i,
      ),
    );
  }

  return { tickets, staffMembers, staffColors, summary: buildSummary(tickets) };
}

function outcomeStatus(outcomes: StaffOutcome[]): string {
  if (outcomes.includes("active")) return "Active";
  if (outcomes.includes("responded")) return "Responded";
  if (outcomes.includes("reassigned")) return "Reassigned";
  if (outcomes.includes("on_hold")) return "On Hold";
  if (outcomes.includes("unattended")) return "Unattended";
  return "Assigned";
}

export function normalizePayload(raw: unknown): MatrixPayload {
  if (!raw || typeof raw !== "object") {
    return { tickets: [], staffMembers: [], staffColors: {} };
  }
  const obj = raw as Partial<MatrixPayload>;
  const tickets = (Array.isArray(obj.tickets) ? obj.tickets : []).map((t, i) =>
    enrichTicket(t as Partial<Ticket>, i),
  );
  return {
    tickets,
    staffMembers: Array.isArray(obj.staffMembers) ? obj.staffMembers : [],
    staffColors:
      obj.staffColors && typeof obj.staffColors === "object" ? obj.staffColors : {},
    lookupTicket:
      typeof obj.lookupTicket === "string" ? obj.lookupTicket : undefined,
    summary: obj.summary ?? buildSummary(tickets),
  };
}
