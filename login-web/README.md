# React login (Next.js + shadcn + Tailwind)

Animated login shell for the Ticket Responses dashboard, using the BON brand palette and Supabase `dashboard_verify_login` RPC (same as Streamlit).

## Stack

| Piece | Path / tool |
|--------|-------------|
| **Components** | `src/components/ui/` (shadcn default) |
| **App routes** | `src/app/` |
| **Utilities** | `src/lib/utils.ts` (`cn`) |
| **Global styles** | `src/app/globals.css` |
| **Aliases** | `@/components`, `@/lib` (see `components.json`) |

### Why `components/ui`?

shadcn CLI installs primitives under **`components/ui`** so imports stay consistent (`@/components/ui/button`). Custom blocks like `background-paths.tsx` live beside `button`, `input`, etc.

## Prerequisites

- [Node.js 20+](https://nodejs.org/) (includes `npm`)
- Supabase migration `20260520_dashboard_users.sql` applied

## Setup (first time)

From the repo root:

```powershell
cd login-web
npm install
copy .env.local.example .env.local
# Edit .env.local — set NEXT_PUBLIC_SUPABASE_URL and NEXT_PUBLIC_SUPABASE_ANON_KEY
```

### Optional: scaffold with shadcn CLI

If you prefer the official generator (after Node is installed):

```powershell
npx create-next-app@latest . --typescript --tailwind --eslint --app --src-dir --import-alias "@/*"
npx shadcn@latest init
npx shadcn@latest add button input label
npm install framer-motion @radix-ui/react-slot class-variance-authority clsx tailwind-merge tailwindcss-animate lucide-react @supabase/supabase-js
```

This repo already contains the files above; use the CLI only when adding more shadcn components.

## Run

```powershell
npm run dev
```

- **Login:** http://localhost:3000  
- **Component demo:** http://localhost:3000/demo  

## Brand colors (Tailwind)

| Token | Hex |
|--------|-----|
| `bon-orange` | `#F15A29` |
| `bon-cyan` | `#00B3C6` |
| `bon-red` | `#E2231A` |
| `bon-lightOrange` | `#F7931E` |
| `bon-black` | `#000000` |

## Streamlit Cloud vs local

| Host | What runs |
|------|-----------|
| `http://localhost:3000` | **login-web** (Next.js) — you start with `npm run dev` |
| `http://localhost:8501` | **app.py** (Streamlit) |
| `https://….streamlit.app` | **Only Streamlit** — React is **not** included unless you deploy `login-web` elsewhere |

If `DASHBOARD_REACT_LOGIN_URL=http://localhost:3000` is copied into Streamlit secrets, visitors’ browsers cannot open your PC’s localhost — the app falls back to the built-in Streamlit login on Cloud.

## Deploy login-web (e.g. Vercel)

1. Import this repo (or only the `login-web/` folder) on [Vercel](https://vercel.com).
2. Set **Root Directory** to `login-web`.
3. Environment variables:
   - `NEXT_PUBLIC_SUPABASE_URL`
   - `NEXT_PUBLIC_SUPABASE_ANON_KEY`
   - `SUPABASE_KEY` (same as Streamlit — handoff signing)
   - `NEXT_PUBLIC_DASHBOARD_URL=https://ticketresponsesdashboard-1.streamlit.app`
4. In **Streamlit Cloud → Secrets**, set:
   - `DASHBOARD_REACT_LOGIN_URL=https://YOUR-VERCEL-URL.vercel.app`

## Integration note

The React app verifies credentials via Supabase, then redirects to `NEXT_PUBLIC_DASHBOARD_URL` (Streamlit) with an `_auth` token. Streamlit completes the session via HMAC handoff.

Built-in Streamlit login: `https://your-app.streamlit.app/?native=1`
