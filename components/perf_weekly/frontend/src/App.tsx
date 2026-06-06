import WeeklyOperationalReport from "./components/WeeklyOperationalReport";
import { DEMO_PAYLOAD, normalizePayload } from "./types";

interface AppProps {
  payload?: ReturnType<typeof normalizePayload>;
}

export { normalizePayload };

export default function App({ payload }: AppProps) {
  const data = payload ?? DEMO_PAYLOAD;
  return <WeeklyOperationalReport payload={data} />;
}
