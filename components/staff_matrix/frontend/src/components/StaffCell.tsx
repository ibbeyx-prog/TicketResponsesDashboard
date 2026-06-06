import { Check } from "lucide-react";
import type { StaffAssignment } from "../types/ticket";

interface StaffCellProps {
  assignment?: StaffAssignment;
  staffColor?: string;
  highlight?: boolean;
}

export function StaffCell({ assignment, staffColor, highlight }: StaffCellProps) {
  if (!assignment) {
    return <div className="h-10 w-full" />;
  }

  const color = staffColor ?? "#9ec5e8";
  const isResponded = assignment.outcome === "responded";
  const title = assignment.role
    ? `${assignment.label} · ${assignment.role}`
    : assignment.label;

  return (
    <div
      className={`flex h-10 w-full items-center justify-center ${
        highlight ? "rounded-sm" : ""
      }`}
      style={highlight ? { backgroundColor: `${color}28` } : undefined}
      title={title}
    >
      {isResponded ? (
        <span
          className="flex h-5 w-5 items-center justify-center rounded-full"
          style={{ backgroundColor: color, color: "#141414" }}
        >
          <Check className="h-3 w-3 stroke-[3]" aria-hidden />
        </span>
      ) : (
        <span
          className="h-3.5 w-3.5 rounded-full shadow-sm"
          style={{ backgroundColor: color }}
          aria-label={title}
        />
      )}
    </div>
  );
}
