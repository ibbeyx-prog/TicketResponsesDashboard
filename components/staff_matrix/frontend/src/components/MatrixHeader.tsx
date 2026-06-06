import type { MatrixSummary } from "../types/ticket";

interface MatrixHeaderProps {
  summary: MatrixSummary;
}

export function MatrixHeader({ summary }: MatrixHeaderProps) {
  const topLabel =
    summary.topCollaborativeStaffCount > 1
      ? `#${summary.topCollaborativeCaseId} (${summary.topCollaborativeStaffCount} Staff)`
      : "—";

  return (
    <header className="border-b border-dashboard-border bg-dashboard-bg px-4 py-4">
      <h1 className="text-lg font-semibold tracking-tight text-white">
        Multi-Staff Case Management Matrix
      </h1>
      <p className="mt-0.5 text-xs text-dashboard-muted">
        Scalable Ticket Performance Tracking
      </p>
      <div className="mt-3 flex flex-wrap gap-4 text-[11px] text-dashboard-muted">
        <span>
          Total Cases:{" "}
          <strong className="font-semibold text-dashboard-text">
            {summary.totalCases.toLocaleString()}
          </strong>
        </span>
        <span>
          Avg. Staff per Case:{" "}
          <strong className="font-semibold text-dashboard-text">
            {summary.avgStaffPerCase.toFixed(1)}
          </strong>
        </span>
        <span>
          Most Collaborative Case:{" "}
          <strong className="font-semibold text-dashboard-accent">{topLabel}</strong>
        </span>
      </div>
    </header>
  );
}
