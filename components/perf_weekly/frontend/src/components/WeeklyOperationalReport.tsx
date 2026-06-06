import CategoryOutcomeChart from "./CategoryOutcomeChart";
import type { WeeklyDashboardPayload } from "../types";

interface Props {
  payload: WeeklyDashboardPayload;
}

function KpiCard({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded-lg border border-weekly-border bg-weekly-card p-6">
      <h3 className="text-sm font-medium text-weekly-muted">{label}</h3>
      <p className="mt-2 text-3xl font-semibold tabular-nums">{value}</p>
    </div>
  );
}

export default function WeeklyOperationalReport({ payload }: Props) {
  const { title, weekLabel, timezone, kpis, categoryOutcome, priorityCases } =
    payload;

  return (
    <div className="dashboard-container bg-weekly-bg p-6 text-white">
      <header className="mb-8 flex flex-wrap items-start justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold tracking-tight">{title}</h1>
          <p className="mt-1 text-sm text-weekly-muted">
            Weekly operational performance · {timezone}
          </p>
        </div>
        <div className="rounded-lg border border-weekly-border bg-weekly-card px-4 py-2 text-sm text-weekly-muted">
          Weekly Operational Report ({weekLabel})
        </div>
      </header>

      {kpis.length > 0 && (
        <section className="mb-8 grid grid-cols-2 gap-4 sm:grid-cols-4">
          {kpis.map((kpi) => (
            <KpiCard key={kpi.label} label={kpi.label} value={kpi.value} />
          ))}
        </section>
      )}

      <section className="grid grid-cols-1 gap-8 lg:grid-cols-2">
        <div className="rounded-lg border border-weekly-border bg-weekly-card p-6">
          <h3 className="mb-4 text-base font-medium">Tickets by Category &amp; Outcome</h3>
          <CategoryOutcomeChart rows={categoryOutcome} />
        </div>

        <div className="rounded-lg border border-weekly-border bg-weekly-card p-6">
          <h3 className="mb-4 text-base font-medium">Priority Cases (Investigation)</h3>
          {priorityCases.length === 0 ? (
            <p className="text-sm text-weekly-muted">No investigation cases this week.</p>
          ) : (
            <div className="overflow-x-auto">
              <table className="w-full text-left text-sm">
                <thead>
                  <tr className="border-b border-weekly-border text-weekly-muted">
                    <th className="pb-2 pr-4 font-medium">Outcome</th>
                    <th className="pb-2 pr-4 font-medium">Category</th>
                    <th className="pb-2 font-medium text-right">Tickets</th>
                  </tr>
                </thead>
                <tbody>
                  {priorityCases.map((row, idx) => (
                    <tr
                      key={`${row.outcome}-${row.category}-${idx}`}
                      className="border-b border-weekly-border/60 last:border-0"
                    >
                      <td className="py-2.5 pr-4 text-weekly-investigation">{row.outcome}</td>
                      <td className="py-2.5 pr-4">{row.category}</td>
                      <td className="py-2.5 text-right tabular-nums font-medium">{row.tickets}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>
      </section>
    </div>
  );
}
