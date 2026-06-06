/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        dashboard: {
          bg: "#141414",
          panel: "#1a1a1a",
          border: "rgba(215, 180, 145, 0.22)",
          accent: "#D7B491",
          muted: "#a39e97",
          text: "#e8e6e3",
        },
        outcome: {
          active: "#9ec5e8",
          responded: "#b8d4a8",
          reassigned: "#D7B491",
          assigned: "#9ec5e8",
          unattended: "#c97a7a",
          hold: "#a39e97",
        },
      },
    },
  },
  plugins: [],
};
