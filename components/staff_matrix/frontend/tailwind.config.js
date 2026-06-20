/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        dashboard: {
          bg: "#0b0f18",
          panel: "#0d1220",
          border: "#1a2035",
          accent: "#e2e8f8",
          muted: "#8a9ac0",
          dim: "#2a3a5a",
          text: "#e2e8f8",
        },
        outcome: {
          active: "#3b82f6",
          responded: "#22c55e",
          reassigned: "#60a5fa",
          assigned: "#3b82f6",
          unattended: "#ef4444",
          hold: "#f59e0b",
        },
      },
    },
  },
  plugins: [],
};
