import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { Grid2x2 } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { fetchStockData } from '@/pages/MarketView/utils/api';
import { DEFAULT_BLUE_CHIPS } from '../framework/defaults';
import { SymbolListField } from '../framework/settings/SymbolListField';
import { SettingsDoneButton } from '../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../types';

interface MiniChartGridConfig {
  symbols: string[];
}

const SPARK_DAYS = 30;
// Render-time hard cap. SymbolListField caps at 18, but prefs are
// server-stored JSON — a corrupted or hand-edited blob with thousands of
// symbols would fan out thousands of OHLC requests every 120s. Clamp here
// so the render path never trusts persisted state past the UI-enforced cap.
const MAX_SYMBOLS = 18;

interface CellData {
  symbol: string;
  closes: number[];
  last: number;
  prev: number;
}

async function loadSpark(symbol: string): Promise<CellData | null> {
  const now = new Date();
  const past = new Date();
  past.setDate(past.getDate() - SPARK_DAYS - 5); // small buffer
  const toISO = now.toISOString().slice(0, 10);
  const fromISO = past.toISOString().slice(0, 10);

  const res = await fetchStockData(symbol, '1day', fromISO, toISO);
  if (!res.data || res.data.length === 0) return null;
  const tail = res.data.slice(-SPARK_DAYS);
  const closes = tail.map((d) => d.close);
  if (closes.length < 2) return null;
  return {
    symbol,
    closes,
    last: closes[closes.length - 1],
    prev: closes[0],
  };
}

function MiniSparkline({ cell }: { cell: CellData }) {
  const w = 110;
  const h = 34;
  const pad = 2;
  const { closes } = cell;
  let min = closes[0];
  let max = closes[0];
  for (const c of closes) {
    if (c < min) min = c;
    else if (c > max) max = c;
  }
  const range = max - min || 1;
  const step = (w - pad * 2) / Math.max(1, closes.length - 1);
  const pts = closes.map((c, i) => {
    const x = pad + i * step;
    const y = pad + (1 - (c - min) / range) * (h - pad * 2);
    return `${x.toFixed(1)},${y.toFixed(1)}`;
  });
  const d = `M${pts[0]} L${pts.slice(1).join(' ')}`;
  const lastX = pad + (closes.length - 1) * step;
  const lastY = pad + (1 - (cell.last - min) / range) * (h - pad * 2);
  const up = cell.last >= cell.prev;
  const stroke = up ? 'var(--color-profit)' : 'var(--color-loss)';

  return (
    <svg width={w} height={h} viewBox={`0 0 ${w} ${h}`} aria-hidden>
      <path d={d} fill="none" stroke={stroke} strokeWidth={1.5} strokeLinejoin="round" strokeLinecap="round" />
      <circle cx={lastX} cy={lastY} r={2} fill={stroke} />
    </svg>
  );
}

function MiniChartGridWidget({ instance }: WidgetRenderProps<MiniChartGridConfig>) {
  const navigate = useNavigate();
  const { watchlist } = useDashboardContext();
  // Memoized so the query key stays reference-stable on unrelated re-renders
  // (dashboard edit-mode toggle, neighbor widget edits). React Query would
  // cache-hit on the stringified key either way, but skipping the IIFE also
  // saves allocating a throwaway array on every parent render.
  const effectiveSymbols = useMemo(() => {
    const configured = (instance.config.symbols ?? []).filter(Boolean);
    if (configured.length > 0) return configured.slice(0, MAX_SYMBOLS);
    const fromWatchlist = watchlist.rows.map((r) => r.symbol).filter(Boolean);
    if (fromWatchlist.length > 0) return fromWatchlist.slice(0, 12);
    return DEFAULT_BLUE_CHIPS;
  }, [instance.config.symbols, watchlist.rows]);

  const { data, isLoading } = useQuery<CellData[]>({
    queryKey: ['mini-chart-grid', effectiveSymbols.join(',')],
    queryFn: async () => {
      // allSettled so one bad symbol (5xx, timeout, throttled) doesn't block
      // the whole grid. Failed cells silently drop out; successful cells
      // still render. Promise.all would reject the whole batch and leave
      // the user staring at the loading skeleton until the next refetch.
      const results = await Promise.allSettled(effectiveSymbols.map(loadSpark));
      const cells: CellData[] = [];
      for (const r of results) {
        if (r.status === 'fulfilled' && r.value) cells.push(r.value);
      }
      return cells;
    },
    staleTime: 60_000,
    refetchInterval: 120_000,
    refetchIntervalInBackground: false,
  });

  return (
    <div className="dashboard-glass-card p-4 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-2 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2">
          <Grid2x2 className="h-3.5 w-3.5 self-center" style={{ color: 'var(--color-text-tertiary)' }} />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            Mini Chart Grid
          </span>
        </div>
        <span className="text-[11px] dashboard-mono" style={{ color: 'var(--color-text-tertiary)' }}>
          {effectiveSymbols.length} symbols · {SPARK_DAYS}d
        </span>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto">
        {isLoading && !data ? (
          <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
            {effectiveSymbols.slice(0, 6).map((s) => (
              <div
                key={s}
                className="h-16 rounded-lg animate-pulse"
                style={{ backgroundColor: 'var(--color-bg-subtle)' }}
              />
            ))}
          </div>
        ) : (data?.length ?? 0) === 0 ? (
          <div
            className="text-center py-8 text-xs"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            No data available
          </div>
        ) : (
          <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
            {data!.map((cell) => {
              const pct = ((cell.last - cell.prev) / cell.prev) * 100;
              const up = pct >= 0;
              return (
                <button
                  key={cell.symbol}
                  type="button"
                  onClick={() => navigate(`/market?symbol=${encodeURIComponent(cell.symbol)}`)}
                  className="widget-drag-cancel p-2 rounded-lg border text-left transition-colors"
                  style={{
                    backgroundColor: 'var(--color-bg-card)',
                    borderColor: 'var(--color-border-muted)',
                  }}
                  onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-default)')}
                  onMouseLeave={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
                >
                  <div className="flex items-center justify-between">
                    <span
                      className="font-bold text-xs"
                      style={{ color: 'var(--color-text-primary)' }}
                    >
                      {cell.symbol}
                    </span>
                    <span
                      className="text-[11px] dashboard-mono"
                      style={{ color: up ? 'var(--color-profit)' : 'var(--color-loss)' }}
                    >
                      {up ? '+' : ''}{pct.toFixed(2)}%
                    </span>
                  </div>
                  <div className="flex items-end justify-between mt-1">
                    <span
                      className="text-[11px] dashboard-mono"
                      style={{ color: 'var(--color-text-secondary)' }}
                    >
                      ${cell.last.toFixed(2)}
                    </span>
                    <MiniSparkline cell={cell} />
                  </div>
                </button>
              );
            })}
          </div>
        )}
      </div>
    </div>
  );
}

function MiniChartGridSettings({
  config,
  onChange,
  onClose,
}: WidgetSettingsProps<MiniChartGridConfig>) {
  const { watchlist } = useDashboardContext();
  // Mirror the render-time precedence: configured symbols > watchlist >
  // defaults. Opening settings is side-effect-free — prefs only commit
  // on the user's first add/remove, which captures the full displayed
  // list as the starting edit state.
  const displayedSymbols = (() => {
    if (config.symbols?.length) return config.symbols;
    const fromWatchlist = watchlist.rows.map((r) => r.symbol).filter(Boolean);
    if (fromWatchlist.length > 0) return fromWatchlist.slice(0, 12);
    return DEFAULT_BLUE_CHIPS;
  })();
  return (
    <div className="space-y-4">
      <SymbolListField
        label="Symbols"
        value={displayedSymbols}
        onChange={(next) => onChange({ symbols: next })}
        placeholder="Add a symbol (Enter)"
        max={18}
      />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<MiniChartGridConfig>({
  type: 'markets.miniChartGrid',
  title: 'Mini Chart Grid',
  description: 'Sparkline grid across multiple symbols — 30-day trend at a glance.',
  category: 'markets',
  icon: Grid2x2,
  component: MiniChartGridWidget,
  settingsComponent: MiniChartGridSettings,
  defaultConfig: { symbols: [] },
  defaultSize: { w: 12, h: 16 },
  minSize: { w: 6, h: 10 },
  initConfig: (ctx) => {
    const fromWatchlist = ctx.watchlist.rows.map((r) => r.symbol).filter(Boolean);
    if (fromWatchlist.length > 0) return { symbols: fromWatchlist.slice(0, 12) };
    return { symbols: DEFAULT_BLUE_CHIPS };
  },
});
