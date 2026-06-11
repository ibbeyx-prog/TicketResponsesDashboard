import { Streamlit, withStreamlitConnection } from "streamlit-component-lib";
import { createRoot } from "react-dom/client";
import { StrictMode, useEffect } from "react";
import App, { normalizePayload } from "./App";
import "./index.css";

interface StreamlitArgs {
  data?: unknown;
  height?: number;
}

function ConnectedMatrix(props: { args: StreamlitArgs }) {
  const payload = normalizePayload(props.args?.data);
  const height = typeof props.args?.height === "number" ? props.args.height : 620;

  useEffect(() => {
    Streamlit.setFrameHeight(height + 8);
  }, [height]);

  return (
    <App
      payload={payload}
      height={height}
      onLookupChange={(value) => Streamlit.setComponentValue(value)}
    />
  );
}

const WrappedMatrix = withStreamlitConnection(ConnectedMatrix);

const rootEl = document.getElementById("root");
if (!rootEl) {
  throw new Error("Missing #root element");
}

const inStreamlit = window.location.href.includes("streamlit") || window.parent !== window;

createRoot(rootEl).render(
  <StrictMode>{inStreamlit ? <WrappedMatrix /> : <App />}</StrictMode>,
);
