/** @type {import('tailwindcss').Config} */
export default {
  content: ["./index.html", "./src/**/*.{js,ts,jsx,tsx}"],
  theme: {
    extend: {
      colors: {
        weekly: {
          bg: "#121212",
          card: "#1E1E1E",
          border: "#374151",
          muted: "#9CA3AF",
          accent: "#60A5FA",
          resolved: "#34D399",
          hold: "#FBBF24",
          investigation: "#F87171",
          sales: "#A78BFA",
        },
      },
    },
  },
  plugins: [],
};
