export interface WeeklyKpi {
  label: string;
  value: number;
}

export interface CategoryOutcomeRow {
  category: string;
  outcome: string;
  count: number;
}

export interface PriorityCaseRow {
  outcome: string;
  category: string;
  tickets: number;
}

export interface WeeklyDashboardPayload {
  title: string;
  weekLabel: string;
  timezone: string;
  kpis: WeeklyKpi[];
  categoryOutcome: CategoryOutcomeRow[];
  priorityCases: PriorityCaseRow[];
}

export const EMPTY_PAYLOAD: WeeklyDashboardPayload = {
  title: "NetOps | Coverage Eye",
  weekLabel: "—",
  timezone: "UTC+5",
  kpis: [],
  categoryOutcome: [],
  priorityCases: [],
};

export function normalizePayload(raw: unknown): WeeklyDashboardPayload {
  if (!raw || typeof raw !== "object") {
    return EMPTY_PAYLOAD;
  }
  const data = raw as Record<string, unknown>;
  return {
    title: typeof data.title === "string" ? data.title : EMPTY_PAYLOAD.title,
    weekLabel:
      typeof data.weekLabel === "string" ? data.weekLabel : EMPTY_PAYLOAD.weekLabel,
    timezone:
      typeof data.timezone === "string" ? data.timezone : EMPTY_PAYLOAD.timezone,
    kpis: Array.isArray(data.kpis) ? (data.kpis as WeeklyKpi[]) : [],
    categoryOutcome: Array.isArray(data.categoryOutcome)
      ? (data.categoryOutcome as CategoryOutcomeRow[])
      : [],
    priorityCases: Array.isArray(data.priorityCases)
      ? (data.priorityCases as PriorityCaseRow[])
      : [],
  };
}

export const DEMO_PAYLOAD: WeeklyDashboardPayload = {
  title: "NetOps | Coverage Eye",
  weekLabel: "01 Jun – 07 Jun 2026",
  timezone: "UTC+5",
  kpis: [
    { label: "Total Tickets", value: 42 },
    { label: "CSM Tickets", value: 37 },
    { label: "Sales Cases", value: 5 },
    { label: "Investigation", value: 11 },
  ],
  categoryOutcome: [
    { category: "Coverage Check", outcome: "Resolved", count: 12 },
    { category: "Coverage Check", outcome: "Investigation", count: 4 },
    { category: "Hardware", outcome: "On Hold", count: 3 },
    { category: "Hardware", outcome: "Resolved", count: 8 },
    { category: "Power", outcome: "Investigation", count: 5 },
  ],
  priorityCases: [
    { outcome: "Investigation", category: "Coverage Check", tickets: 4 },
    { outcome: "Investigation", category: "Power", tickets: 5 },
    { outcome: "Investigation", category: "Backhaul", tickets: 2 },
  ],
};
