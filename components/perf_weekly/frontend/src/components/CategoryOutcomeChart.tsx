import {
  Bar,
  BarChart,
  CartesianGrid,
  Legend,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";
import type { CategoryOutcomeRow } from "../types";

const OUTCOME_COLORS: Record<string, string> = {
  Resolved: "#34D399",
  "On Hold": "#FBBF24",
  Investigation: "#F87171",
  Regional: "#A78BFA",
  "Under Investigation": "#F87171",
};

function outcomeColor(outcome: string): string {
  return OUTCOME_COLORS[outcome] ?? "#60A5FA";
}

function buildChartRows(rows: CategoryOutcomeRow[]) {
  const categories = [...new Set(rows.map((r) => r.category))].sort((a, b) =>
    a.localeCompare(b),
  );
  const outcomes = [...new Set(rows.map((r) => r.outcome))].sort((a, b) =>
    a.localeCompare(b),
  );
  const chartData = categories.map((category) => {
    const point: Record<string, string | number> = { category };
    for (const outcome of outcomes) {
      const match = rows.find(
        (r) => r.category === category && r.outcome === outcome,
      );
      point[outcome] = match?.count ?? 0;
    }
    return point;
  });
  return { chartData, outcomes };
}

interface Props {
  rows: CategoryOutcomeRow[];
}

export default function CategoryOutcomeChart({ rows }: Props) {
  if (!rows.length) {
    return (
      <div className="flex h-64 items-center justify-center text-sm text-weekly-muted">
        No category data for this week.
      </div>
    );
  }

  const { chartData, outcomes } = buildChartRows(rows);

  return (
    <div className="h-72 w-full">
      <ResponsiveContainer width="100%" height="100%">
        <BarChart data={chartData} margin={{ top: 8, right: 8, left: 0, bottom: 48 }}>
          <CartesianGrid strokeDasharray="3 3" stroke="#374151" />
          <XAxis
            dataKey="category"
            tick={{ fill: "#9CA3AF", fontSize: 11 }}
            angle={-28}
            textAnchor="end"
            height={64}
            interval={0}
          />
          <YAxis
            allowDecimals={false}
            tick={{ fill: "#9CA3AF", fontSize: 11 }}
            width={32}
          />
          <Tooltip
            contentStyle={{
              background: "#1E1E1E",
              border: "1px solid #374151",
              borderRadius: "8px",
              color: "#fff",
            }}
          />
          <Legend wrapperStyle={{ fontSize: 12, color: "#9CA3AF" }} />
          {outcomes.map((outcome) => (
            <Bar
              key={outcome}
              dataKey={outcome}
              stackId="tickets"
              fill={outcomeColor(outcome)}
              radius={[2, 2, 0, 0]}
            />
          ))}
        </BarChart>
      </ResponsiveContainer>
    </div>
  );
}
