# langalpha web

Frontend for langalpha — React 19 + Vite + TypeScript SPA. Talks to the FastAPI backend over REST (axios) + SSE (raw fetch). Path alias `@` → `src/` (wired in both `vite.config.js` and `vitest.config.ts`).

> Single source of truth for AI coding agents in `web/`. `CLAUDE.md` imports this via `@AGENTS.md`; Codex/Cursor read it directly. Edit here, not there.

## Commands

```bash
pnpm dev          # dev server on 127.0.0.1:5173 (proxies /api/v1 + /ws/v1 → VITE_PROXY_BACKEND, default :8000)
pnpm build        # tsc --noEmit && vite build — typecheck gates every build
pnpm typecheck    # tsc --noEmit (gated in CI)
pnpm test         # vitest run;  test:e2e = Playwright
pnpm lint         # ESLint 9 flat config (advisory — NOT gated in CI)
```

## Landmines (non-obvious)

- **Dual-mode auth** (`contexts/AuthContext.tsx`), switched by `VITE_HOST_MODE` (`config/hostMode.ts`, default `oss`): `platform` → `SupabaseAuthProvider` (real session; Bearer token wired into the axios interceptor via `setTokenGetter()`); `oss` → static local-dev context, always logged in as `VITE_AUTH_USER_ID` (default `local-dev-user`). `VITE_SUPABASE_URL`/`_KEY` only gate Supabase-*client* construction, NOT the mode — check `isPlatformMode`, never `VITE_SUPABASE_URL`.
- **Chat/market SSE uses raw `fetch()` + `ReadableStream`, NOT axios** (axios can't stream) — `streamFetch()` in `pages/ChatAgent/utils/api.ts` + `pages/MarketView/utils/api.ts`. Its token comes straight from `supabase.auth.getSession()`, not the axios interceptor. Most *authenticated* REST goes through the shared axios instance (`api/client.ts`, auto-Bearer, base `VITE_API_BASE_URL`) — but public/unauthenticated calls (`pages/SharedChat/api.ts`), `auth/sync`, and market-data **WebSocket** use raw `fetch`/`WS`, not axios.
- **Agent artifact path routing has ONE source of truth: `pages/ChatAgent/utils/agentPaths.ts`.** `classifyAgentPath` (→ `memory|memo|user-profile|skill|file`, normalizing `file://`, `/home/(workspace|daytona)/`, `./`, `__wsref__/<wsid>/…` cross-workspace refs) + `computeAgentArtifactRouting` (pure: which panel tab/key/workspace to open). Add new path types here, not in panel components — each new location duplicates the normalization rules.
- **Zod validates untrusted *persisted/user* input at the boundary, not API responses.** Widget prefs (schemas in `configSchemas.ts`, applied via `safeParse` in `migrations.ts`), onboarding prefs, MCP config — all `safeParse` + per-field `.catch()` (never throw). Typed API responses are plain TS interfaces, not runtime-validated.
- **i18n re-render gotcha:** locale lives in a `locale` cookie (cookie → browser → `en-US`), no live cross-tab sync. Components that format numbers/dates via `createFormatter`/`createDateFormatter` (`lib/format.ts`) MUST also call `useTranslation()`, or they won't re-render on a locale switch.

## Conventions

- **API layering:** each page group owns its calls in a local `utils/api.ts` (`ChatAgent`, `Dashboard`, `MarketView`, `Automations`); cross-page data goes through shared hooks in `hooks/`.
- **React Query:** hierarchical key factory in `lib/queryKeys.ts` enables prefix invalidation (e.g. invalidate `queryKeys.user.all`). Dashboard prefs write back through a guarded writer that survives cross-tab races + cold-cache mounts.
- **Styling:** Tailwind 3 + theme-aware CSS custom properties (`var(--color-*)`) used directly in style props; `cn()` (clsx + tailwind-merge) for conditional classes; Radix primitives in `components/ui/` via `class-variance-authority`.
- **Tests:** co-located in `__tests__/` next to the code; Vitest + jsdom + Testing Library. Global setup mocks `matchMedia`/`IntersectionObserver`/`ResizeObserver` (`src/test/setup.ts`).
- **Side-by-side ChatView + FilePanel headers must stay height-aligned** — if you touch either header's padding/icon size, verify they still line up (`FilePanel.css` `file-panel-header`).

## Working principles

- **Keep API calls in the api layer, not in components.** Endpoint/fetch calls belong in a page's `utils/api.ts` or a shared `lib/*` client module (e.g. market data in `lib/bars`, `lib/quotes`) — never inline in a component. Server-state access goes through `hooks/` + React Query; React-lifecycle singletons through `contexts/`. Components compose `components/ui/` primitives (they don't hand-roll Radix).
- **One source of truth — don't duplicate the cross-cutting modules.** Agent-path logic → `agentPaths.ts`; query keys → `queryKeys.ts` (never inline key arrays — it breaks prefix invalidation); locale/formatters → `lib/locale.ts` + `lib/format.ts` (never ad-hoc `Intl.*`); class merging → `cn()` (never concatenate class strings).
- **Server state is React Query; validate untrusted input at the boundary.** Don't mirror server data into local state; invalidate by key prefix. Zod (`safeParse` + `.catch`, never throwing) guards only *persisted/user* input — never runtime-validate trusted API responses, never `.parse()` at a boundary. Module-level singletons that outlive React must be reset on logout.
- **Types are the hard gate, lint is soft.** Keep `tsc --noEmit` green (it gates build + CI); avoid `any` — narrow `unknown` instead — even though ESLint won't stop you.

## Env

| Variable | Default | Purpose |
|---|---|---|
| `VITE_HOST_MODE` | `oss` | `platform` → Supabase auth mode; `oss` → local-dev, no auth |
| `VITE_API_BASE_URL` | (empty = same-origin) | Backend base URL for axios (dev: same-origin, proxied) |
| `VITE_PROXY_BACKEND` | `http://localhost:8000` | Dev-server proxy target for `/api/v1` + `/ws/v1` |
| `VITE_SUPABASE_URL` | — | Supabase project URL (gates client creation, not mode) |
| `VITE_SUPABASE_PUBLISHABLE_KEY` | — | Supabase anon key |
| `VITE_AUTH_USER_ID` | `local-dev-user` | User id in `oss` mode |
| `VITE_CDN_BASE` | `/` | Asset base for CDN builds |
| `VITE_COOKIE_DOMAIN` | (unset = host-only) | Parent domain to share auth/locale cookies across subdomains |
| `VITE_APP_ENTRY_PATH` | `/app` (platform) / `/` (oss) | Where the SPA entry (login + anonymous bounce) mounts; set `/` on a dedicated app subdomain |
| `VITE_PLATFORM_URL` | `/account` | Platform console origin/path for account, plans and integrations links. Absolute origin on a split-host deploy; **no trailing slash** (call sites concatenate) |
