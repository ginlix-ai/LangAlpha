# LangAlpha Web

React frontend for LangAlpha — a vibe investing agent with AI-powered research, trading charts, and automated workflows.

## Features

- **Supabase Auth** — OAuth login with session management and protected routes
- **SSE Streaming Chat** — Real-time agent responses with subagent task cards, tool call display, and reasoning blocks
- **HITL Plan Approval** — Review and approve/reject agent plans before execution
- **Market Dashboard** — Watchlist and portfolio overview with stock data
- **TradingView-Style Charting** — Interactive candlestick charts with AI chat sidebar for stock analysis
- **Scheduled Automations** — Create and manage recurring agent tasks with cron scheduling and execution history
- **Document Viewers** — Inline rendering of PDF, Excel, CSV, and HTML artifacts from agent responses
- **Todo Tracking** — Drawer-based task list synced with agent todo updates

## Tech Stack

| Category | Libraries |
|----------|-----------|
| Framework | React 18, Vite 5 |
| Routing | React Router 6 |
| UI Components | Ant Design 5, Radix UI (dialog, toast), shadcn-style `ui/` components |
| Styling | Tailwind CSS 3, `clsx`, `tailwind-merge`, `class-variance-authority` |
| Animation | Framer Motion 12 |
| Icons | Lucide React |
| Charts | `lightweight-charts` (TradingView), Recharts |
| Auth | `@supabase/supabase-js` |
| HTTP | Axios |
| Markdown | `react-markdown`, `remark-gfm`, `rehype-raw`, `react-syntax-highlighter` |
| File Handling | `react-pdf`, `exceljs`, `xlsx`, `html2canvas`, `react-to-print` |
| Dev Tools | ESLint 9 |

## Project Structure

```
src/
├── api/                    # Axios client, Supabase client, stock API
├── assets/                 # Static assets
├── components/
│   ├── Main/               # Route definitions
│   ├── Sidebar/            # Navigation sidebar
│   └── ui/                 # Reusable UI primitives (button, card, dialog, toast, etc.)
├── contexts/               # AuthContext provider
├── lib/                    # Utility helpers (cn)
├── pages/
│   ├── Login/              # OAuth login with animated background
│   ├── Dashboard/          # Watchlist, portfolio, market overview
│   ├── ChatAgent/          # Streaming chat with workspaces, threads, file panel
│   ├── MarketView/         # Candlestick charts with AI chat sidebar
│   ├── Automations/        # Scheduled agent task management
│   └── Detail/             # Stock detail page
├── styles/                 # Global styles
├── utils/                  # Shared utilities
├── App.jsx                 # Root component
└── main.jsx                # Entry point
```

## Routes

| Path | Page |
|------|------|
| `/` | Login |
| `/callback` | OAuth callback handler |
| `/dashboard` | Dashboard |
| `/chat` | Chat Agent (workspace gallery) |
| `/chat/:workspaceId` | Chat Agent (thread gallery) |
| `/chat/:workspaceId/:threadId` | Chat Agent (conversation) |
| `/market` | Market View |
| `/automations` | Automations |
| `/detail/:indexNumber` | Stock Detail |

## Environment Variables

| Variable | Description |
|----------|-------------|
| `VITE_API_BASE_URL` | Backend API base URL (empty in production for relative URLs) |
| `VITE_SUPABASE_URL` | Supabase project URL |
| `VITE_SUPABASE_PUBLISHABLE_KEY` | Supabase publishable (anon) key |
| `VITE_AUTH_USER_ID` | Fallback user ID for local dev when Supabase is unset |

Copy `.env.example` and fill in your values:

```bash
cp .env.example .env
```

## Getting Started

### Prerequisites

- Node.js 18+
- npm

### Install

```bash
npm install
```

### Scripts

```bash
npm run dev       # Start dev server (http://localhost:5173)
npm run build     # Production build → dist/
npm run preview   # Preview production build
npm run lint      # Run ESLint
```

## Navigation

The sidebar provides four main sections:

| Icon | Label | Path |
|------|-------|------|
| LayoutDashboard | Dashboard | `/dashboard` |
| MessageSquareText | Chat Agent | `/chat` |
| ChartCandlestick | Market View | `/market` |
| Timer | Automations | `/automations` |
