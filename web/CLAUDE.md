# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

The frontend for langalpha — an AI-driven financial research platform. React 19 SPA that communicates with the FastAPI backend via REST + SSE streaming for real-time agent responses.

## Commands

```bash
pnpm dev          # Dev server on 127.0.0.1:5173 (proxies /api → localhost:8080)
pnpm build        # Production build (Vite 7, manual chunk splitting)
pnpm lint         # ESLint 9 flat config
pnpm preview      # Preview production build

npx vitest run                        # All tests (CI mode)
npx vitest run src/path/to/test.ts    # Single test file
npx vitest                            # Watch mode
```

## Architecture

### Provider Stack (`main.tsx`)

```
QueryClientProvider (React Query — 2min staleTime, retry: 1)
  → BrowserRouter (react-router-dom v6, v7 compat flags on)
    → ThemeProvider (light/dark via CSS variables)
      → AuthProvider (Supabase session or local-dev bypass)
        → App + Toaster
```

### Routing

**`App.tsx`** handles top-level routes: `/` (login or redirect), `/callback` (OAuth), `/s/:shareToken` (public shared chat).

**`components/Main/Main.tsx`** handles authenticated routes inside the app shell (Sidebar + Main). All pages are **lazy-loaded** with `React.lazy` and animated via `AnimatePresence` (keyed by top-level path segment):

- `/dashboard` — Dashboard (configurable widget gallery: watchlist, portfolio, news, TradingView widgets, mini-chart grid). Layout + per-widget settings stored in user preferences; see `pages/Dashboard/widgets/framework/`.
- `/chat`, `/chat/:workspaceId`, `/chat/t/:threadId` — ChatAgent
- `/market` — MarketView (real-time charts)
- `/automations` — Automations
- `/settings` — Settings

### Auth — Dual Mode (`contexts/AuthContext.tsx`)

Controlled by `VITE_SUPABASE_URL`:

- **Production (set):** `SupabaseAuthProvider` — manages Supabase session, listens for auth state changes, calls `/api/v1/auth/sync` on sign-in, seeds React Query cache with user data, wires Bearer token into the axios interceptor via `setTokenGetter()`. On logout: `queryClient.clear()`.
- **Local dev (unset):** Static context — always logged in as `VITE_AUTH_USER_ID` (default `local-dev-user`). No Supabase needed.

### Data Fetching

**REST calls:** Via shared axios instance (`api/client.ts`) with automatic Bearer token injection. Base URL from `VITE_API_BASE_URL` (default `http://localhost:8000`).

**SSE streaming (chat):** Uses raw `fetch()` + `ReadableStream` (not axios — it doesn't support streaming). Implemented as `streamFetch()` in `pages/ChatAgent/utils/api.ts` and `pages/MarketView/utils/api.ts`. Auth tokens for fetch are obtained directly from `supabase.auth.getSession()`.

**File uploads (memo):** Use the same axios instance with `multipart/form-data`. Memo upload accepts PDF, markdown, plain text, CSV, and JSON; the backend extracts text from PDFs, generates metadata asynchronously via an LLM, and streams the original bytes back through `GET /api/v1/memo/user/download?key=...` for in-browser preview. UI lives in `pages/ChatAgent/components/MemoPanel.tsx` + `FilePanelMemo.tsx`, hooks in `pages/ChatAgent/hooks/useMemo.ts`.

**React Query:** Global `QueryClient` in `main.tsx`. Key factory in `lib/queryKeys.ts` — hierarchical keys enabling prefix-based invalidation (e.g., invalidate `queryKeys.user.all` to refresh all user-related data). Shared hooks in `hooks/` (`useUser`, `useWorkspaces`, `useWorkspace`, `usePreferences`, `useUpdatePreferences`, `useNetworkStatus`).

**Dashboard preferences:** `useDashboardPrefs` (in `pages/Dashboard/widgets/framework/`) reads layout + per-widget config from `user.preferences.dashboard`, validates each widget config through a Zod schema (`configSchemas.ts`), and writes back via a guarded writer (`dashboardPrefsWriter.ts`) that survives cross-tab races and cold-cache mounts. Cross-tab updates land via the `usePreferences` query cache; the dashboard re-renders without a network round-trip.

### API Layer Pattern

Each page group owns its API calls in a local `utils/api.ts`:
- `pages/ChatAgent/utils/api.ts` — workspaces, threads, SSE streams, file ops, HITL, feedback, skills, models
- `pages/Dashboard/utils/api.ts` — user profile, dashboard data
- `pages/MarketView/utils/api.ts` — market data, WebSocket
- `pages/Automations/utils/api.ts` — automation CRUD

Cross-page data goes through shared hooks in `hooks/`.

### Styling

- **Tailwind CSS 3** for utility classes
- **CSS custom properties** (`var(--color-*)`) for theme-aware colors — used directly in style props alongside Tailwind
- **Per-component `.css` files** for scoped styles
- **`clsx` + `tailwind-merge`** (`cn()` pattern) for conditional class merging in `components/ui/`

### Layout Alignment

The ChatAgent page has side-by-side panels (ChatView + FilePanel). Their headers must align horizontally. The ChatView top bar uses `px-4 py-2` with `p-2` buttons containing `h-5 w-5` icons (~52px total height). The FilePanel header (`file-panel-header` in `FilePanel.css`) must match this height — currently `padding: 12px 16px` with `6px`-padded buttons containing `h-4 w-4` icons. When modifying either header, verify they still align visually.

### Key Conventions

- **Path alias:** `@` → `src/` (configured in both `vite.config.js` and `vitest.config.js`)
- **Tests:** Co-located in `__tests__/` subdirectories next to the code they test. Vitest + jsdom + Testing Library + `@testing-library/jest-dom`. Global setup mocks `matchMedia`, `IntersectionObserver`, `ResizeObserver` (`src/test/setup.ts`).
- **UI primitives:** `components/ui/` has Radix-based primitives (dialog, toast, button, card, etc.) using `class-variance-authority` for variant props.
- **i18n:** `i18next` + `react-i18next`. Setup in `src/i18n.ts` (cross-tab locale sync via `storage` event). Locale-aware number/date formatting via `createFormatter` / `createDateFormatter` in `src/lib/format.ts` — components MUST also call `useTranslation()` so they re-render on locale switch.
- **WebSocket:** Real-time market data via `pages/MarketView/contexts/MarketDataWSContext.tsx`.

### Env Variables

| Variable | Default | Purpose |
|---|---|---|
| `VITE_API_BASE_URL` | `http://localhost:8000` | Backend API base URL |
| `VITE_SUPABASE_URL` | (unset = local dev) | Supabase project URL — controls auth mode |
| `VITE_SUPABASE_PUBLISHABLE_KEY` | — | Supabase publishable (anon) key |
| `VITE_AUTH_USER_ID` | `local-dev-user` | User ID when Supabase auth is disabled |
| `VITE_CDN_BASE` | `/` | Asset base URL for CDN deployments |
