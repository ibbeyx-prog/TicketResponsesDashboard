/** Visit outcome for a staff member on a ticket. */
export type StaffOutcome =
  | "active"
  | "assigned"
  | "responded"
  | "reassigned"
  | "unattended"
  | "on_hold";

/** Per-staff assignment cell metadata. */
export interface StaffAssignment {
  outcome: StaffOutcome;
  label: string;
  role?: string;
}

/** Text-only comment in Case Info (no photo URL here). */
export interface TicketComment {
  at: string;
  author: string;
  text: string;
  kind: "response" | "visit" | "assignment" | "field" | "admin";
}

/** Photo link in Case Info (deduped by URL). */
export interface TicketPhoto {
  at: string;
  author: string;
  url: string;
}

/** @deprecated Legacy combined activity — split into comments + photos. */
export interface TicketActivity {
  at: string;
  author: string;
  text: string;
  photoUrl?: string;
  kind: TicketComment["kind"];
}

/** Ticket row in the case-management matrix. */
export interface Ticket {
  id: string;
  caseLabel: string;
  status: string;
  displayStatus: string;
  priority: "Low" | "Normal" | "High" | "Critical";
  assignedStaff: string[];
  staffAssignments: Record<string, StaffAssignment>;
  isShared: boolean;
  comments?: TicketComment[];
  photos?: TicketPhoto[];
}

export interface MatrixSummary {
  totalCases: number;
  avgStaffPerCase: number;
  topCollaborativeCaseId: string;
  topCollaborativeStaffCount: number;
}

/** Payload from Streamlit / demo loader. */
export interface MatrixPayload {
  tickets: Ticket[];
  staffMembers: string[];
  staffColors: Record<string, string>;
  summary?: MatrixSummary;
}

export interface MatrixFilters {
  search: string;
  staff: string;
  priority: string;
  status: string;
}

export const PRIORITY_OPTIONS = ["All", "Critical", "High", "Normal", "Low"] as const;

export const DISPLAY_STATUS_OPTIONS = [
  "All",
  "Open",
  "In Progress",
  "Resolved",
  "On Hold",
] as const;

export const OUTCOME_ORDER: StaffOutcome[] = [
  "active",
  "responded",
  "reassigned",
  "assigned",
  "on_hold",
  "unattended",
];

export function outcomeRank(outcome: StaffOutcome): number {
  const idx = OUTCOME_ORDER.indexOf(outcome);
  return idx === -1 ? OUTCOME_ORDER.length : idx;
}
