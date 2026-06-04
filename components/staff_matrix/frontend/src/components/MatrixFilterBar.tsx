import { Search } from "lucide-react";
import {
  DISPLAY_STATUS_OPTIONS,
  PRIORITY_OPTIONS,
  type MatrixFilters,
} from "../types/ticket";

interface MatrixFilterBarProps {
  filters: MatrixFilters;
  staffOptions: string[];
  onChange: (next: MatrixFilters) => void;
}

export function MatrixFilterBar({
  filters,
  staffOptions,
  onChange,
}: MatrixFilterBarProps) {
  const selectClass =
    "w-full rounded border border-dashboard-border bg-[#1a1a1a] px-2.5 py-2 text-xs text-dashboard-text outline-none focus:border-dashboard-accent/50";

  return (
    <div className="flex flex-wrap items-center gap-3 border-b border-dashboard-border bg-[#161616] px-4 py-3">
      <label className="relative min-w-[180px] flex-1">
        <Search className="pointer-events-none absolute left-2.5 top-1/2 h-3.5 w-3.5 -translate-y-1/2 text-dashboard-muted" />
        <input
          type="search"
          value={filters.search}
          onChange={(e) => onChange({ ...filters, search: e.target.value })}
          placeholder="Ticket ID"
          className={`${selectClass} pl-8`}
        />
      </label>

      <label className="min-w-[120px] flex-1 text-[10px] text-dashboard-muted">
        <span className="mb-1 block">Staff</span>
        <select
          value={filters.staff}
          onChange={(e) => onChange({ ...filters, staff: e.target.value })}
          className={selectClass}
        >
          <option value="All">All</option>
          {staffOptions.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </label>

      <label className="min-w-[100px] flex-1 text-[10px] text-dashboard-muted">
        <span className="mb-1 block">Priority</span>
        <select
          value={filters.priority}
          onChange={(e) => onChange({ ...filters, priority: e.target.value })}
          className={selectClass}
        >
          {PRIORITY_OPTIONS.map((p) => (
            <option key={p} value={p}>
              {p}
            </option>
          ))}
        </select>
      </label>

      <label className="min-w-[110px] flex-1 text-[10px] text-dashboard-muted">
        <span className="mb-1 block">Status</span>
        <select
          value={filters.status}
          onChange={(e) => onChange({ ...filters, status: e.target.value })}
          className={selectClass}
        >
          {DISPLAY_STATUS_OPTIONS.map((s) => (
            <option key={s} value={s}>
              {s}
            </option>
          ))}
        </select>
      </label>
    </div>
  );
}
