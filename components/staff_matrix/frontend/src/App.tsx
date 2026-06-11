import { MultiStaffCaseMatrix } from "./components/MultiStaffCaseMatrix";
import { buildDemoPayload, normalizePayload } from "./data/demoPayload";
import type { MatrixComponentValue, MatrixPayload } from "./types/ticket";

interface AppProps {
  payload?: MatrixPayload;
  height?: number;
  onLookupChange?: (value: MatrixComponentValue) => void;
}

export default function App({ payload, height = 720, onLookupChange }: AppProps) {
  const data = payload ?? buildDemoPayload(1200);

  if (!data.tickets.length) {
    return (
      <div className="flex h-full items-center justify-center p-6 text-sm text-dashboard-muted">
        No ticket data available for the matrix.
      </div>
    );
  }

  return (
    <MultiStaffCaseMatrix payload={data} height={height} onLookupChange={onLookupChange} />
  );
}

export { normalizePayload, buildDemoPayload };
