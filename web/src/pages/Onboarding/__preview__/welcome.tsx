/* eslint-disable react-refresh/only-export-components -- entry file, not a module */
import { StrictMode, useEffect, useState } from 'react';
import { createRoot } from 'react-dom/client';
import { MemoryRouter, Route, Routes, useLocation, useNavigate } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import i18n, { initI18n } from '@/i18n';
import { switchLocale } from '@/lib/locale';
import { TRADING_AGREEMENT_VERSION } from '@/lib/tradingPermission';
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext';
import { Toaster } from '@/components/ui/toaster';
import '@/index.css';
import type { Brokerage } from '@/pages/ChatAgent/utils/api/brokerages';
import { catalogProbe, httpCatalogServer } from '@/test/factories';
import { WelcomeFlow } from '@/pages/Onboarding/welcome/Welcome';
import { WELCOME_STEPS, isWelcomeStep, welcomeSteps, type WelcomeStepId } from '@/pages/Onboarding/welcome/paths';
import { emptyProfile, type InvestingProfile } from '@/pages/Onboarding/welcome/profile';

/**
 * Dev-only harness rendering the REAL welcome flow against a stubbed API, for
 * design iteration and screenshots. Open /welcome-preview.html on the dev
 * server. Not bundled in production (vite builds index.html only).
 *
 * Every knob is also a query parameter, so a screenshot is one URL:
 *   ?step=risk&theme=dark&locale=zh-CN&connected=1&holdings=1&mode=edit
 *   &answers=fresh|full&brief=0&bar=0
 * `brief=0` has no market brief today; `bar=0` hides the toolbar.
 */

/** Each broker's groups as the registry curates them, so a row's order steps
 *  read as they will in the app. */
const CAPABILITIES: Record<string, Brokerage['capabilities']> = {
  robinhood: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'watchlists', tone: 'neutral' },
    { key: 'scanners', tone: 'neutral' },
    { key: 'account', tone: 'caution' },
    { key: 'order_preview', tone: 'neutral', rung: true },
    { key: 'trading', tone: 'danger', rung: true },
  ],
  ibkr: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'watchlists', tone: 'neutral' },
    { key: 'alerts', tone: 'neutral' },
    { key: 'account', tone: 'caution' },
    { key: 'staged_orders', tone: 'caution', rung: true },
  ],
  webull: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'watchlists', tone: 'neutral' },
    { key: 'account', tone: 'caution' },
  ],
  moomoo: [
    { key: 'market_data', tone: 'neutral' },
    { key: 'watchlists', tone: 'neutral' },
    { key: 'account', tone: 'caution' },
    { key: 'paper_trading', tone: 'neutral', rung: true },
    { key: 'trading', tone: 'danger', rung: true },
  ],
};

const BROKERAGES: Brokerage[] = [
  ['robinhood', 'Robinhood', 'https://agent.robinhood.com/mcp/trading', 'robinhood.com', true, true],
  ['ibkr', 'Interactive Brokers', 'https://api.ibkr.com/v1/api/mcp-public', 'interactivebrokers.com', false, true],
  ['webull', 'Webull', 'https://api.webull.com/mcp', 'webull.com', false, false],
  ['moomoo', 'moomoo', 'https://mcp.moomoo.com/mcp', 'moomoo.com', false, false],
].map(([name, label, url, site, nativeOnly, exclusive]) => ({
  name: name as string,
  label: label as string,
  url: url as string,
  site: site as string,
  description: '',
  native_callback_only: nativeOnly as boolean,
  exclusive_connection: exclusive as boolean,
  capabilities: CAPABILITIES[name as string],
}));

/** Every search answers with these, whatever was typed. */
const SEARCH = [
  { symbol: 'NVDA', name: 'NVIDIA Corporation', nameLocal: '英伟达', exchangeShortName: 'NASDAQ', currency: 'USD' },
  { symbol: 'NVO', name: 'Novo Nordisk A/S', nameLocal: '诺和诺德', exchangeShortName: 'NYSE', currency: 'USD' },
  { symbol: '0700.HK', name: 'Tencent Holdings', nameLocal: '腾讯控股', exchangeShortName: 'HKSE', currency: 'HKD' },
];

const PROFILES: Record<string, InvestingProfile> = {
  fresh: emptyProfile(),
  full: {
    investorType: 'professional',
    experience: '3_10y',
    helpWith: ['ideas', 'portfolio'],
    riskLevel: 'growth',
    horizon: 'months',
    markets: ['us', 'hk'],
    instruments: ['stock', 'option', 'future'],
    approaches: ['technical', 'fundamental'],
    styles: ['growth', 'value'],
    // `biotech` is not on the list: a sector the agent saved.
    sectors: ['technology', 'semiconductors', 'biotech'],
    brokerages: [],
    depth: 'adaptive',
    deliverable: 'visual',
    proactivity: 'when_needed',
  },
};

/** Today's market brief, as the insights API serves one. */
const BRIEF = {
  market_insight_id: 'brief-1',
  type: 'market_update',
  status: 'completed',
  headline: 'Chipmakers lead a rebound as yields ease',
  summary: 'The Nasdaq rose 1.4% as Treasury yields fell after a softer inflation print.',
  topics: [
    { text: 'Semiconductors', trend: 'up' },
    { text: 'Rates', trend: 'down' },
  ],
  model: 'preview',
  created_at: '2026-10-19T14:00:00Z',
  completed_at: '2026-10-19T14:02:00Z',
  content: [
    {
      title: 'Inflation cools',
      body: 'Core CPI rose 0.2% on the month, under forecasts.',
      url: 'https://example.com/cpi',
    },
    { title: 'Chips rally', body: 'Semiconductor shares led the index higher.', url: 'https://example.com/chips' },
  ],
  sources: [{ url: 'https://example.com/cpi', title: 'Example News' }],
};

interface Knobs {
  step: WelcomeStepId;
  mode: 'new' | 'edit';
  theme: 'light' | 'dark';
  locale: 'en-US' | 'zh-CN';
  connected: boolean;
  holdings: boolean;
  answers: keyof typeof PROFILES;
  brief: boolean;
  bar: boolean;
}

function readKnobs(): Knobs {
  const p = new URLSearchParams(window.location.search);
  const step = p.get('step');
  const answers = p.get('answers');
  return {
    step: isWelcomeStep(step) ? step : 'experience',
    mode: p.get('mode') === 'edit' ? 'edit' : 'new',
    theme: p.get('theme') === 'dark' ? 'dark' : 'light',
    locale: p.get('locale') === 'zh-CN' ? 'zh-CN' : 'en-US',
    connected: p.get('connected') === '1',
    holdings: p.get('holdings') === '1',
    answers: answers && answers in PROFILES ? answers : 'full',
    brief: p.get('brief') !== '0',
    bar: p.get('bar') !== '0',
  };
}

function writeKnobs(k: Knobs) {
  const p = new URLSearchParams();
  for (const [key, value] of Object.entries(k)) {
    p.set(key, typeof value === 'boolean' ? (value ? '1' : '0') : value);
  }
  window.history.replaceState(null, '', `?${p}`);
}

// --- Stub API: what the flow reads and writes, answered in memory -------------

const api = {
  connected: false,
  brief: true,
  holdings: [] as Array<{ user_portfolio_id: string; symbol: string; quantity: number; instrument_type: string }>,
  watchlist: [] as Array<{ watchlist_item_id: string; symbol: string }>,
  trading: {
    level: 'approve_each',
    agreement_version: TRADING_AGREEMENT_VERSION,
    agreed_at: null as string | null,
    updated_at: null as string | null,
  },
};

function moomooRow() {
  return httpCatalogServer({
    name: 'moomoo',
    url: 'https://mcp.moomoo.com/mcp',
    oauth_status: 'connected',
    enabled: true,
    granted_capabilities: ['market_data', 'watchlists', 'account', 'paper_trading'],
    tool_count: 14,
    probe: catalogProbe(),
  });
}

function answer(method: string, path: string, body: unknown): unknown {
  if (path === '/api/v1/mcp/brokerages') return { brokerages: BROKERAGES };
  if (path === '/api/v1/mcp/servers') return { servers: api.connected ? [moomooRow()] : [], max_servers: 50 };
  if (path === '/api/v1/users/me/portfolio') {
    if (method === 'POST') {
      const row = { user_portfolio_id: `p${Date.now()}`, ...(body as { symbol: string; quantity: number; instrument_type: string }) };
      api.holdings.push(row);
      return row;
    }
    return { holdings: api.holdings };
  }
  if (path.startsWith('/api/v1/users/me/portfolio/') && method === 'DELETE') {
    const id = decodeURIComponent(path.split('/').pop() ?? '');
    api.holdings = api.holdings.filter((h) => h.user_portfolio_id !== id);
    return {};
  }
  if (path === '/api/v1/market-data/search/stocks') return { query: '', results: SEARCH, count: SEARCH.length };
  if (path === '/api/v1/users/me/watchlists/default/items') {
    if (method === 'POST') {
      const { symbol } = body as { symbol: string };
      const row = { watchlist_item_id: `w${Date.now()}`, symbol };
      api.watchlist.push(row);
      return row;
    }
    return { items: api.watchlist };
  }
  if (path.startsWith('/api/v1/users/me/watchlists/default/items/') && method === 'DELETE') {
    const id = decodeURIComponent(path.split('/').pop() ?? '');
    api.watchlist = api.watchlist.filter((w) => w.watchlist_item_id !== id);
    return {};
  }
  if (path === '/api/v1/insights/today') {
    const { content: _content, sources: _sources, ...card } = BRIEF;
    return api.brief ? { insights: [card], count: 1 } : { insights: [], count: 0 };
  }
  if (path === `/api/v1/insights/${BRIEF.market_insight_id}`) return BRIEF;
  if (path === '/api/v1/features') return { features: [] };
  if (path === '/api/v1/workspaces/home') return { workspace_id: 'home-preview', name: 'Home', status: 'running', is_home: true };
  if (path === '/api/v1/users/me/preferences') return method === 'PUT' ? body : {};
  if (path === '/api/v1/users/me/trading-permission') {
    if (method === 'PUT') {
      const { level, agreement_version } = body as { level: string; agreement_version?: number };
      const agreed = agreement_version !== undefined;
      api.trading = {
        level,
        agreement_version: agreed ? agreement_version : api.trading.agreement_version,
        agreed_at: agreed ? new Date().toISOString() : api.trading.agreed_at,
        updated_at: new Date().toISOString(),
      };
    }
    return api.trading;
  }
  return {};
}

/** Point the stub at a set of knobs: who is connected, what is held. */
function applyKnobs(k: Knobs) {
  api.connected = k.connected;
  api.brief = k.brief;
  api.holdings = k.holdings
    ? [
        { user_portfolio_id: 'p1', symbol: 'NVDA', quantity: 40, instrument_type: 'stock' },
        { user_portfolio_id: 'p2', symbol: '0700.HK', quantity: 200, instrument_type: 'stock' },
      ]
    : [];
}
applyKnobs(readKnobs());

const realFetch = window.fetch.bind(window);
window.fetch = async (input, init) => {
  const href = typeof input === 'string' ? input : input instanceof URL ? input.href : input.url;
  const url = new URL(href, window.location.origin);
  if (!url.pathname.startsWith('/api/')) return realFetch(input, init);
  const method = (init?.method ?? 'GET').toUpperCase();
  const body = typeof init?.body === 'string' ? (JSON.parse(init.body) as unknown) : null;
  await new Promise((r) => setTimeout(r, 250));
  return new Response(JSON.stringify(answer(method, url.pathname, body)), {
    status: 200,
    headers: { 'Content-Type': 'application/json' },
  });
};

// --- Harness ------------------------------------------------------------------

function Btn({ on, onClick, children }: { on: boolean; onClick: () => void; children: string }) {
  return (
    <button
      type="button"
      onClick={onClick}
      style={{
        font: 'inherit',
        cursor: 'pointer',
        padding: '3px 9px',
        borderRadius: 999,
        border: '1px solid var(--color-border-default)',
        background: on ? 'var(--color-accent-primary)' : 'var(--color-bg-card)',
        color: on ? 'var(--color-text-on-accent)' : 'var(--color-text-secondary)',
      }}
    >
      {children}
    </button>
  );
}

function Toolbar({ knobs, set }: { knobs: Knobs; set: (patch: Partial<Knobs>) => void }) {
  const row = { display: 'flex', flexWrap: 'wrap' as const, gap: 4 };
  return (
    <div
      style={{
        position: 'fixed',
        left: 12,
        bottom: 96,
        zIndex: 2000,
        display: 'flex',
        flexDirection: 'column',
        gap: 6,
        maxWidth: 300,
        padding: 10,
        borderRadius: 12,
        border: '1px solid var(--color-border-default)',
        background: 'var(--color-bg-card)',
        boxShadow: '0 4px 24px rgba(0,0,0,.2)',
        fontFamily: 'system-ui, sans-serif',
        fontSize: '0.6875rem',
      }}
    >
      <div style={row}>
        {WELCOME_STEPS.map((s) => (
          <Btn key={s} on={knobs.step === s} onClick={() => set({ step: s })}>
            {s}
          </Btn>
        ))}
      </div>
      <div style={row}>
        {(['fresh', 'full'] as const).map((a) => (
          <Btn key={a} on={knobs.answers === a} onClick={() => set({ answers: a })}>
            {a}
          </Btn>
        ))}
      </div>
      <div style={row}>
        <Btn on={knobs.connected} onClick={() => set({ connected: !knobs.connected })}>
          connected
        </Btn>
        <Btn on={knobs.holdings} onClick={() => set({ holdings: !knobs.holdings })}>
          holdings
        </Btn>
        <Btn on={knobs.brief} onClick={() => set({ brief: !knobs.brief })}>
          brief
        </Btn>
        <Btn on={knobs.mode === 'edit'} onClick={() => set({ mode: knobs.mode === 'edit' ? 'new' : 'edit' })}>
          edit
        </Btn>
      </div>
      <div style={row}>
        <Btn on={knobs.theme === 'dark'} onClick={() => set({ theme: knobs.theme === 'dark' ? 'light' : 'dark' })}>
          dark
        </Btn>
        <Btn on={knobs.locale === 'zh-CN'} onClick={() => set({ locale: knobs.locale === 'zh-CN' ? 'en-US' : 'zh-CN' })}>
          zh-CN
        </Btn>
      </div>
    </div>
  );
}

/** Where the flow navigated, with the router state it carried. */
function Landed({ onBack }: { onBack: () => void }) {
  const { pathname, search, state } = useLocation();
  return (
    <div style={{ padding: 32, fontFamily: 'ui-monospace, monospace', fontSize: 13, color: 'var(--color-text-primary)' }}>
      <p>navigated to {pathname + search}</p>
      <pre>{JSON.stringify(state, null, 2)}</pre>
      <button type="button" onClick={onBack} style={{ textDecoration: 'underline' }}>
        back to the flow
      </button>
    </div>
  );
}

function Flow({ knobs, client, run }: { knobs: Knobs; client: QueryClient; run: number }) {
  const navigate = useNavigate();
  const steps = welcomeSteps(knobs.mode);
  const initialStep = steps.includes(knobs.step) ? knobs.step : steps[0];
  return (
    <Routes>
      <Route
        path="/welcome"
        element={
          <WelcomeFlow
            key={`${JSON.stringify(knobs)}-${run}`}
            mode={knobs.mode}
            steps={steps}
            initialStep={initialStep}
            initialProfile={PROFILES[knobs.answers]}
            savedProfile={PROFILES[knobs.answers]}
            userId={null}
            userName="Alex Morgan"
            onSkip={() => {}}
          />
        }
      />
      <Route
        path="*"
        element={
          <Landed
            onBack={() => {
              void client.invalidateQueries();
              navigate('/welcome', { replace: true });
            }}
          />
        }
      />
    </Routes>
  );
}

function Preview() {
  const [knobs, setKnobs] = useState(readKnobs);
  const [run, setRun] = useState(0);
  const [client] = useState(
    () => new QueryClient({ defaultOptions: { queries: { retry: false, refetchOnWindowFocus: false } } }),
  );
  const { setTheme } = useTheme();

  useEffect(() => setTheme(knobs.theme), [knobs.theme, setTheme]);
  useEffect(() => {
    void switchLocale(i18n, knobs.locale);
  }, [knobs.locale]);

  const set = (patch: Partial<Knobs>) => {
    const next = { ...knobs, ...patch };
    writeKnobs(next);
    // The stub answers from the new knobs, and the cache is dropped so the
    // flow asks again.
    applyKnobs(next);
    client.clear();
    setKnobs(next);
    setRun((n) => n + 1);
  };

  return (
    <QueryClientProvider client={client}>
      <MemoryRouter initialEntries={['/welcome']}>
        <Flow knobs={knobs} client={client} run={run} />
      </MemoryRouter>
      {knobs.bar && <Toolbar knobs={knobs} set={set} />}
      <Toaster />
    </QueryClientProvider>
  );
}

void initI18n().then(() =>
  createRoot(document.getElementById('root')!).render(
    <StrictMode>
      <ThemeProvider>
        <Preview />
      </ThemeProvider>
    </StrictMode>,
  ),
);
