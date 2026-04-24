import { useMemo } from 'react';
import { Activity } from 'lucide-react';
import { TradingViewEmbed } from '../../framework/TradingViewEmbed';
import { registerWidget } from '../../framework/WidgetRegistry';
import { useDashboardContext } from '../../framework/DashboardDataContext';
import { SymbolListField } from '../../framework/settings/SymbolListField';
import { EnumField } from '../../framework/settings/EnumField';
import { TradingViewSettingsFooter } from '../../framework/TradingViewSettingsFooter';
import { SettingsDoneButton } from '../../framework/settings/SettingsDoneButton';
import type { WidgetRenderProps, WidgetSettingsProps } from '../../types';

interface TickerTapeConfig {
  symbols: string[];
  displayMode: 'adaptive' | 'regular' | 'compact';
}

// Macro defaults. Plain US-listed ETFs always resolve on TV's free embed
// tier in every region — the licensed index feeds (`SP:SPX`, `NASDAQ:NDX`)
// fail unauthenticated, and CFDs (`FOREXCOM:*`) are blocked for US retail.
// SPY → S&P 500, QQQ → Nasdaq-100, DIA → Dow, IWM → Russell 2000, GLD →
// gold. Labels in TICKER_LABELS below remap to the names users recognize.
export const DEFAULT_TICKERS = [
  'AMEX:SPY',
  'NASDAQ:QQQ',
  'AMEX:DIA',
  'AMEX:IWM',
  'AMEX:GLD',
];

/**
 * Display labels for well-known tape symbols. Keeps the chip in settings
 * as the canonical `EXCHANGE:SYMBOL` form (what TV actually resolves)
 * while the tape itself shows the short name users recognize.
 * Misses fall back to the last colon-segment.
 */
const TICKER_LABELS: Record<string, string> = {
  'AMEX:SPY': 'SPX',
  'NASDAQ:QQQ': 'NDX',
  'AMEX:DIA': 'DJI',
  'AMEX:IWM': 'RUT',
  'AMEX:GLD': 'GOLD',
  'FOREXCOM:SPXUSD': 'SPX',
  'FOREXCOM:NSXUSD': 'NDX',
  'FOREXCOM:DJI': 'DJI',
};

function titleFor(sym: string): string {
  return TICKER_LABELS[sym] ?? sym.split(':').pop() ?? sym;
}

/** Bare-ticker form ("AMEX:SPY" → "SPY", "NVDA" → "NVDA") for dedup. */
function bareTicker(sym: string): string {
  return (sym.split(':').pop() ?? sym).toUpperCase();
}

/**
 * Macro defaults + watchlist + portfolio, deduped by bare-ticker form so
 * a watchlist "SPY" collapses with the default "AMEX:SPY". First-write
 * wins → defaults stay in their fixed order at the head of the tape, then
 * personal symbols follow in watchlist-then-portfolio order.
 *
 * Exported for unit-testing — the live-seed dedup is the ONE thing every
 * tape user depends on and it's only otherwise exercised through React.
 */
export function buildSeedSymbols(watchlistSyms: string[], portfolioSyms: string[]): string[] {
  const out: string[] = [];
  const seen = new Set<string>();
  const push = (sym: string) => {
    if (!sym) return;
    const key = bareTicker(sym);
    if (seen.has(key)) return;
    seen.add(key);
    out.push(sym);
  };
  for (const s of DEFAULT_TICKERS) push(s);
  for (const s of watchlistSyms) push(s);
  for (const s of portfolioSyms) push(s);
  return out;
}

// Natural iframe heights for each displayMode (measured from TV's default
// `embed-widget-ticker-tape.js` renders). Paired with `fitToContent` so the
// RGL cell adapts to chrome between edit/view modes without clipping the
// marquee or leaving a band of padding.
const TICKER_CONTENT_HEIGHT: Record<TickerTapeConfig['displayMode'], number> = {
  adaptive: 76,
  regular: 76,
  compact: 46,
};

// Intentional iframe fallback: the WC `<tv-ticker-tape>` is a *static
// bordered grid* (verified at /widget-docs/widgets/tickers/ticker-tape/
// — even the `horizontal_no_chart` preset). The iframe `embed-widget-
// ticker-tape.js` is the classic continuous scrolling marquee, which is
// the UX users expect. Tracked in tvConfig.ts under "iframe-only widgets".
function TickerTapeWidget({ instance }: WidgetRenderProps<TickerTapeConfig>) {
  const { symbols, displayMode } = instance.config;
  const { watchlist, portfolio } = useDashboardContext();
  // Reactive seed: when the user hasn't customized the chip list, the
  // tape stays in sync with their watchlist and portfolio. The moment
  // they edit chips in settings, the stored list takes over and the
  // tape stops reacting to watchlist/portfolio adds.
  const list = useMemo(() => {
    if (symbols?.length) return symbols;
    const w = watchlist.rows.map((r) => r.symbol).filter(Boolean);
    const p = portfolio.rows.map((r) => r.symbol).filter(Boolean);
    return buildSeedSymbols(w, p);
  }, [symbols, watchlist.rows, portfolio.rows]);
  return (
    <TradingViewEmbed
      card
      scriptKey="ticker-tape"
      contentHeight={TICKER_CONTENT_HEIGHT[displayMode] ?? TICKER_CONTENT_HEIGHT.adaptive}
      config={{
        symbols: list.map((s) => ({ proName: s, title: titleFor(s) })),
        showSymbolLogo: true,
        displayMode,
      }}
    />
  );
}

function TickerTapeSettings({ config, onChange, onClose }: WidgetSettingsProps<TickerTapeConfig>) {
  const { watchlist, portfolio } = useDashboardContext();
  // Mirror the render-time live seed so the chip list shows what the tape
  // is actually rendering. Opening settings stays side-effect-free — the
  // displayed list only commits when the user adds or removes a chip
  // (their first edit captures the full effective list as the start state).
  const displayedSymbols = useMemo(() => {
    if (config.symbols?.length) return config.symbols;
    const w = watchlist.rows.map((r) => r.symbol).filter(Boolean);
    const p = portfolio.rows.map((r) => r.symbol).filter(Boolean);
    return buildSeedSymbols(w, p);
  }, [config.symbols, watchlist.rows, portfolio.rows]);
  return (
    <div className="space-y-4">
      <SymbolListField
        label="Symbols"
        value={displayedSymbols}
        onChange={(next) => onChange({ symbols: next })}
        placeholder="e.g. NASDAQ:NVDA"
        helper="Use TradingView's exchange-qualified form (e.g. NASDAQ:NVDA) for best results."
      />
      <EnumField
        label="Display mode"
        value={config.displayMode ?? 'adaptive'}
        onChange={(v) => onChange({ displayMode: v as TickerTapeConfig['displayMode'] })}
        options={[
          { value: 'adaptive', label: 'Adaptive' },
          { value: 'regular', label: 'Regular' },
          { value: 'compact', label: 'Compact' },
        ]}
      />
      <TradingViewSettingsFooter />
      <SettingsDoneButton onClick={onClose} />
    </div>
  );
}

registerWidget<TickerTapeConfig>({
  type: 'tv.ticker-tape',
  title: 'Ticker Tape',
  description: 'Scrolling real-time quotes across your chosen symbols.',
  category: 'markets',
  icon: Activity,
  component: TickerTapeWidget,
  settingsComponent: TickerTapeSettings,
  defaultConfig: { symbols: [], displayMode: 'adaptive' },
  // fitToContent: cell height tracks the embed's natural 76px iframe plus
  // chrome. View mode → ~6 rows (128px, card hugs the ticker). Edit mode →
  // ~9 rows (200px, includes 40px header + 24px body padding). maxSize.h
  // must clear the edit-mode measurement (~9 rows) otherwise the clamp
  // caps the fit and the iframe gets clipped. Width stays user-resizable.
  fitToContent: true,
  defaultSize: { w: 12, h: 6 },
  minSize: { w: 6, h: 3 },
  maxSize: { w: 12, h: 12 },
  source: 'tradingview',
  // Empty `symbols` triggers the render-time live seed (defaults +
  // watchlist + portfolio, deduped). The tape stays reactive to watchlist
  // and portfolio adds until the user explicitly customizes via settings.
  initConfig: () => ({ symbols: [], displayMode: 'adaptive' }),
});
