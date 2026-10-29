/**
 * The cards' typed boundary. Each reader takes a tool's artifact as it came
 * off the wire and returns the shape a card and its detail panel both draw, so
 * neither casts the wire and the two cannot read one artifact differently.
 * Formatting is not done here: what a figure is called and how it prints is
 * `cardWords`.
 */
import type { RefObject } from 'react';
import { resolveCurrency, isIndexListing, quoteCurrency, timezoneForSymbol, type ChartBar } from '@/lib/bars';
import { resolveDualName } from '@/lib/displayName';
import { parseCalendarDay } from '@/lib/format';
import { asQuoteTier, hasFreshnessFields, isLiveRow, type FreshnessFields } from '@/lib/freshness';
import { dateStrInTz, utcMsToChartSec } from '@/lib/utils';
import { isPriceTriggerConfig } from '@/pages/Automations/utils/price';
import type { PriceTriggerConfig } from '@/types/automation';
import type { Freshness } from '@/types/market';
import { percentMove } from './termChange';

/** What every inline card is handed: the artifact, which both render sites
 *  only draw a card for when there is one. */
export interface InlineCardProps {
  artifact: Record<string, unknown>;
  onClick?: () => void;
  /** The call's own arguments, for a card whose result does not restate its
   *  input: a delivery receipt carries the outcome, not the message sent. */
  toolArgs?: Record<string, unknown>;
}

/** What every detail panel is handed: the artifact (as `data`), the call's
 *  arguments, and what a panel needs of its host. */
export interface PanelProps {
  data: Record<string, unknown>;
  toolArgs?: Record<string, unknown>;
  onOpenFile?: (filePath: string, workspaceId?: string) => void;
  /** The panel's scroller, for a section drawn only once it nears the view. */
  scrollContainerRef?: RefObject<HTMLDivElement | null>;
}

// ─── Narrowing ──────────────────────────────────────────────────────

export function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v);
}

export const record = (v: unknown): Record<string, unknown> | undefined => (isRecord(v) ? v : undefined);
export const finite = (v: unknown): number | undefined => (typeof v === 'number' && Number.isFinite(v) ? v : undefined);
export const text = (v: unknown): string | undefined => (typeof v === 'string' && v ? v : undefined);

/** A list's objects, each in its place: an entry that is not one reads as empty. */
export function records(v: unknown): Record<string, unknown>[] {
  return Array.isArray(v) ? v.map((x) => record(x) ?? {}) : [];
}

function strings(v: unknown): string[] {
  return Array.isArray(v) ? v.filter((x): x is string => typeof x === 'string') : [];
}

const FRESHNESS_LABELS: ReadonlySet<unknown> = new Set<Freshness['label']>(['live', 'delayed', 'stale', 'incomplete', 'unknown']);

function isFreshness(v: unknown): v is Freshness {
  return isRecord(v) && FRESHNESS_LABELS.has(v.label) && typeof v.measured === 'boolean';
}

export const asFreshness = (v: unknown): Freshness | null => (isFreshness(v) ? v : null);

// ─── Market overview: indices and sectors ───────────────────────────

export interface MarketOverviewUnwrap {
  indicesArtifact: Record<string, unknown> | undefined;
  sectorsArtifact: Record<string, unknown> | undefined;
  hasIndices: boolean;
  hasSectors: boolean;
}

/**
 * The composite `market_overview` artifact's nested market_indices and
 * sector_performance artifacts (carried verbatim under `indices` and
 * `sectors`), and whether each holds anything to draw.
 */
export function unwrapMarketOverview(artifact: Record<string, unknown>): MarketOverviewUnwrap {
  const indicesArtifact = record(artifact.indices);
  const sectorsArtifact = record(artifact.sectors);
  const indices = record(indicesArtifact?.indices);
  const sectors = sectorsArtifact?.sectors;
  return {
    indicesArtifact,
    sectorsArtifact,
    hasIndices: !!indices && Object.keys(indices).length > 0,
    hasSectors: Array.isArray(sectors) && sectors.length > 0,
  };
}

export interface IndexRow extends FreshnessFields {
  sym: string;
  name: string;
  last?: number;
  changePct?: number;
  source?: string;
  stats?: Record<string, unknown>;
  /** The bars the panel draws: intraday when the tool sent them, else daily. */
  bars: ChartBar[];
  /** The bars carry a time of day: an epoch, or a datetime rather than a day. */
  intraday: boolean;
}

export function indexRows(artifact: Record<string, unknown> | undefined): IndexRow[] {
  return Object.entries(record(artifact?.indices) ?? {}).map(([sym, raw]) => {
    const data = record(raw) ?? {};
    const ohlcv = records(data.ohlcv);
    const chartOhlcv = records(data.chart_ohlcv);
    const stats = record(data.stats);
    const rows = chartOhlcv.length ? chartOhlcv : ohlcv;
    const first = rows[0]?.time ?? rows[0]?.date;
    return {
      sym,
      name: text(data.name) ?? sym,
      last: finite(ohlcv.at(-1)?.close),
      changePct: finite(stats?.period_change_pct),
      tier: asQuoteTier(data.tier),
      freshness: asFreshness(data.freshness),
      source: text(data.source),
      stats,
      bars: inTimeOrder(chartBars(rows, timezoneForSymbol(sym))),
      intraday: typeof first === 'number' || (typeof first === 'string' && (first.includes(' ') || first.includes('T'))),
    };
  });
}

const SECTOR_ABBREVIATIONS: Record<string, string> = {
  'Consumer Cyclical': 'Cons. Cyclical',
  'Consumer Defensive': 'Cons. Defensive',
  'Communication Services': 'Comm. Services',
  'Financial Services': 'Financial Svcs',
};

/** The sectors as `TermBars` draws them, best first. Under half a hundredth is
 *  no move: no bar, and an unsigned 0.00%. */
export function sectorRows(artifact: Record<string, unknown> | undefined): { label: string; value: number; text: string }[] {
  return records(artifact?.sectors)
    .map((s) => ({ name: text(s.sector) ?? 'N/A', value: finite(s.changePercentage) ?? 0 }))
    .sort((a, b) => b.value - a.value)
    .map(({ name, value }) => {
      const { dir, sign, size } = percentMove(value);
      return { label: SECTOR_ABBREVIATIONS[name] || name, value: dir === 'flat' ? 0 : value, text: `${sign}${size}` };
    });
}

/** A bar's day on its venue's calendar: a Shanghai or Hong Kong daily bar is
 *  stamped at local midnight, which is the previous afternoon in New York. */
export function barDay(bar: Record<string, unknown> | undefined, tz: string): string {
  const val = bar?.time ?? bar?.date;
  if (typeof val === 'number') return dateStrInTz(val, tz);
  return typeof val === 'string' ? val.slice(0, 10) : '';
}

// ─── Price history ──────────────────────────────────────────────────

/**
 * A bar's time in chart seconds, the venue's wall clock written as UTC: an
 * epoch in ms through the venue's zone, a daily `YYYY-MM-DD` at its UTC
 * midnight (which `chartSecToDateStr` reads back as the same day), a
 * datetime the tool wrote in the venue's clock read as UTC, and one that
 * names its zone as the instant it is, through the venue's zone.
 */
function chartSeconds(raw: unknown, tz: string): number | undefined {
  if (typeof raw === 'number') return Number.isFinite(raw) ? utcMsToChartSec(raw, tz) : undefined;
  if (typeof raw !== 'string') return undefined;
  const day = parseCalendarDay(raw);
  if (day) return day.getTime() / 1000;
  const iso = raw.replace(' ', 'T');
  if (/(?:Z|[+-]\d{2}:?\d{2})$/.test(iso)) {
    const ms = Date.parse(iso);
    return Number.isFinite(ms) ? utcMsToChartSec(ms, tz) : undefined;
  }
  const ms = Date.parse(`${iso}Z`);
  return Number.isFinite(ms) ? Math.floor(ms / 1000) : undefined;
}

/** The bars a row list holds, in the order sent; one with no time or close is no bar. */
function chartBars(v: unknown, tz: string): ChartBar[] {
  return records(v).flatMap((b): ChartBar[] => {
    const time = chartSeconds(b.time ?? b.date, tz);
    const close = finite(b.close);
    if (time == null || close == null) return [];
    return [{
      time,
      open: finite(b.open) ?? close,
      high: finite(b.high) ?? close,
      low: finite(b.low) ?? close,
      close,
      volume: finite(b.volume) ?? 0,
    }];
  });
}

/** Bars as a chart takes them: in time order, one to a time (the first sent). */
function inTimeOrder(bars: ChartBar[]): ChartBar[] {
  return [...bars].sort((a, b) => a.time - b.time).filter((b, i, sorted) => i === 0 || b.time !== sorted[i - 1].time);
}

export interface StockPrices {
  symbol?: string;
  /** What the figures print in, or null for an index, whose level is points. */
  currency: string | null;
  /** The venue's zone, the clock every bar's time is written in. */
  tz: string;
  /** What `chartBars` are drawn at, as the bars API names it: `1day`, `1hour`. */
  interval: string;
  /** The daily bars that have a time and a close: the card's line. */
  days: ChartBar[];
  /** The daily series as sent, unread: the days the card and panel are dated
   *  by and the close their headline prints, which a bar still forming may
   *  not have yet. Null when no daily bar was sent. */
  sent: { first: string; mid: string; last: string; lastClose?: number } | null;
  /** The bars a chart draws, in time order: intraday when the tool sent them, else the daily ones. */
  chartBars: ChartBar[];
  /** The period's figures, as the tool's stats measure them. */
  changePct?: number;
  high?: number;
  low?: number;
  avgVolume?: number;
  /** The daily return's standard deviation, as a fraction. */
  volatility?: number;
  ma20?: number;
  ma50?: number;
  /** Measured freshness of the daily series, and who filled it. */
  freshness: Freshness | null;
  source?: string;
  /** The same for `chartBars`, which the tool may fill from another
   *  publisher at another delay: what the detail chart credits. */
  chartFreshness: Freshness | null;
  chartSource?: string;
}

export function readStockPrices(artifact: Record<string, unknown>): StockPrices {
  const symbol = text(artifact.symbol);
  const tz = timezoneForSymbol(symbol);
  const stats = record(artifact.stats);
  const sentRows = records(artifact.ohlcv);
  const days = chartBars(sentRows, tz);
  const intraday = chartBars(artifact.chart_ohlcv, tz);
  const interval = text(artifact.chart_interval) ?? 'daily';
  return {
    symbol,
    // The tool stamps its currency; an older artifact falls back to the suffix.
    currency: quoteCurrency(text(artifact.price_currency), symbol),
    tz,
    // The tool says `daily` where the bars API says `1day`.
    interval: interval === 'daily' ? '1day' : interval,
    days,
    sent: sentRows.length
      ? {
        first: barDay(sentRows[0], tz),
        mid: barDay(sentRows[Math.floor((sentRows.length - 1) / 2)], tz),
        last: barDay(sentRows[sentRows.length - 1], tz),
        lastClose: finite(sentRows[sentRows.length - 1].close),
      }
      : null,
    chartBars: inTimeOrder(intraday.length ? intraday : days),
    changePct: finite(stats?.period_change_pct),
    high: finite(stats?.period_high),
    low: finite(stats?.period_low),
    avgVolume: finite(stats?.avg_volume),
    volatility: finite(stats?.volatility),
    ma20: finite(stats?.ma_20),
    ma50: finite(stats?.ma_50),
    freshness: asFreshness(artifact.freshness),
    source: text(artifact.source),
    // A chart drawn from the daily bars is described by theirs.
    chartFreshness: asFreshness(artifact.chart_freshness) ?? (intraday.length ? null : asFreshness(artifact.freshness)),
    chartSource: text(artifact.chart_source) ?? text(artifact.source),
  };
}

// ─── Quotes ─────────────────────────────────────────────────────────

export interface QuoteDisplay extends FreshnessFields {
  symbol: string;
  /** Local-first display name (贵州茅台 before Kweichow Moutai). */
  name?: string;
  /** The other spelling, shown only where it adds information. */
  secondaryName?: string;
  price?: number;
  change?: number;
  changePct?: number;
  low?: number;
  high?: number;
  open?: number;
  prevClose?: number;
  volume?: number;
  status?: string;
  /** Venue-local clock ('2026-07-14 15:32:05 HKT'), set only for non-US
   *  listings: the provider's print time when it gave one, else the time the
   *  row was retrieved. */
  asOfLocal?: string;
  /** True when `asOfLocal` is the print time, false when it is the retrieval clock. */
  printed: boolean;
  extPrice?: number;
  extChange?: number;
  extChangePct?: number;
  /** What the figures print in: the row's own code, or null for a
   *  pre-contract row, which prints bare as it always did, and for an index,
   *  whose level is points whatever its venue stamps. */
  currency: string | null;
  isIndex: boolean;
}

/**
 * One unified snapshot (snake_case) as the cards read it. During pre- and
 * after-hours, and once the venue has closed, the main price is the regular
 * close and the extended move splits onto the ext* fields, as the company
 * overview's badge names that price the last close. On the FMP fallback the
 * extended fields are absent, so the blended change shows.
 */
function quoteDisplay(q: Record<string, unknown>): QuoteDisplay {
  const status = text(q.market_status);
  const symbol = text(q.symbol) ?? '?';
  const dual = resolveDualName({ name: text(q.name), nameLocal: text(q.name_local), nameEn: text(q.name_en) }, symbol);
  const isIndex = isIndexListing(symbol, text(q.asset_class));
  const regularClose = finite(q.regular_close);
  const lastTrade = finite(q.last_trade_price);
  const d: QuoteDisplay = {
    symbol,
    name: dual.named ? dual.primary : undefined,
    secondaryName: dual.secondary ?? undefined,
    low: finite(q.low),
    high: finite(q.high),
    open: finite(q.open),
    prevClose: finite(q.previous_close),
    volume: finite(q.volume),
    status,
    asOfLocal: text(q.as_of_local),
    printed: (finite(q.as_of) ?? 0) > 0,
    currency: isIndex ? null : (text(q.currency) ?? null),
    tier: asQuoteTier(q.tier),
    freshness: asFreshness(q.freshness),
    isIndex,
  };
  const leadsWithClose = status === 'early_trading' || status === 'late_trading' || status === 'closed';
  if (leadsWithClose && regularClose != null) {
    d.price = regularClose;
    d.change = finite(q.regular_trading_change) ?? finite(q.change);
    d.changePct = finite(q.regular_trading_change_percent) ?? finite(q.change_percent);
    if (lastTrade != null && lastTrade !== regularClose) {
      const session = status === 'early_trading' ? 'early' : 'late';
      d.extPrice = lastTrade;
      d.extChange = finite(q[`${session}_trading_change`]);
      d.extChangePct = finite(q[`${session}_trading_change_percent`]);
    }
  } else {
    d.price = lastTrade ?? finite(q.price);
    d.change = finite(q.change);
    d.changePct = finite(q.change_percent);
  }
  return d;
}

export interface Quotes {
  displays: QuoteDisplay[];
  /** The block's time in the clock its rows share; a mixed block has none. */
  asOf?: string;
  asOfTs?: number;
  /** Every row is live, so the block may say so. */
  allLive: boolean;
}

export function readQuotes(artifact: Record<string, unknown>): Quotes {
  const displays = records(artifact.quotes).map(quoteDisplay);
  // "Live Quotes" is a claim, so make it only when every row supports it. A
  // pre-contract artifact carries no freshness at all and keeps the old
  // heading; anything else has to earn it row by row.
  const anyFreshness = displays.some(hasFreshnessFields);
  const allLive =
    !anyFreshness ||
    (artifact.all_realtime !== false && displays.every((d) => !hasFreshnessFields(d) || isLiveRow(d)));
  return { displays, asOf: text(artifact.as_of), asOfTs: finite(artifact.as_of_ts), allLive };
}

// ─── Options chains and movers ──────────────────────────────────────

export type OptionType = 'call' | 'put';

export interface OptionRow {
  /** The OCC ticker, `O:NVDA261026C00120000`. */
  ticker: string;
  type?: OptionType;
  strike?: number;
  /** `YYYY-MM-DD`. */
  expiry?: string;
  /** The session's figures; a chain saved before the artifact carried them has none. */
  last?: number;
  change?: number;
  changePct?: number;
  volume?: number;
  low?: number;
  high?: number;
}

export interface OptionsChain {
  underlying?: string;
  rows: OptionRow[];
  currency: string;
  status?: string;
  asOfTs?: number;
  /** Each type present, calls first. */
  types: OptionType[];
  /** Each expiry present, soonest first. */
  expiries: string[];
  failed: boolean;
}

const asOptionType = (v: unknown): OptionType | undefined => (v === 'call' || v === 'put' ? v : undefined);

/** The rows of a `get_options_chain` artifact, in either shape it has had. */
export function readOptionsChain(artifact: Record<string, unknown>, toolArgs?: Record<string, unknown>): OptionsChain {
  const raw = records(artifact.results);
  const rows = raw.map((c): OptionRow => {
    const session = record(c.session);
    return {
      ticker: text(c.ticker) ?? '',
      type: asOptionType(c.contract_type) ?? asOptionType(artifact.contract_type),
      strike: finite(c.strike_price),
      expiry: text(c.expiration_date),
      last: finite(session?.close),
      change: finite(session?.change),
      changePct: finite(session?.change_percent),
      volume: finite(session?.volume),
      low: finite(session?.low),
      high: finite(session?.high),
    };
  });
  const underlying = text(artifact.underlying) ?? text(raw[0]?.underlying_ticker) ?? text(toolArgs?.underlying)?.toUpperCase();
  return {
    underlying,
    rows,
    currency: resolveCurrency(text(artifact.currency), underlying),
    status: text(artifact.market_status),
    asOfTs: finite(artifact.as_of_ts),
    types: (['call', 'put'] as const).filter((type) => rows.some((r) => r.type === type)),
    expiries: [...new Set(rows.map((r) => r.expiry).filter((e): e is string => !!e))].sort(),
    failed: typeof artifact.error === 'string',
  };
}

export interface MoverRow {
  symbol: string;
  name?: string;
  price?: number;
  change?: number;
  changePct?: number;
  volume?: number;
  currency: string;
}

export interface MarketMovers {
  direction: 'gainers' | 'losers' | null;
  rows: MoverRow[];
  asOfTs?: number;
  failed: boolean;
}

/**
 * The rows of a `get_market_movers` artifact. A thread saved before the tool
 * shaped its rows holds the vendor's own: the price is the last trade, else the
 * day's close, and the volume is the day's.
 */
export function readMarketMovers(artifact: Record<string, unknown>, toolArgs?: Record<string, unknown>): MarketMovers {
  const rows = records(artifact.results).map((s): MoverRow => {
    const day = record(s.day);
    const trade = record(s.last_trade ?? s.lastTrade);
    const symbol = text(s.symbol) ?? text(s.ticker) ?? '';
    return {
      symbol,
      name: text(s.name) ?? text(s.companyName),
      price: finite(s.price) ?? finite(trade?.price) ?? finite(trade?.p) ?? finite(day?.close) ?? finite(day?.c),
      change: finite(s.change) ?? finite(s.todaysChange),
      changePct: finite(s.change_percent) ?? finite(s.todaysChangePerc) ?? finite(s.changesPercentage),
      volume: finite(s.volume) ?? finite(day?.volume) ?? finite(day?.v),
      currency: resolveCurrency(text(s.currency), symbol),
    };
  });
  const direction = artifact.direction ?? toolArgs?.direction;
  return {
    direction: direction === 'gainers' || direction === 'losers' ? direction : null,
    rows,
    asOfTs: finite(artifact.as_of_ts),
    failed: typeof artifact.error === 'string',
  };
}

// ─── Screens ────────────────────────────────────────────────────────

export interface ScreenerRow {
  symbol?: string;
  name?: string;
  price?: number;
  marketCap?: number;
  /** Percent, as the producer prints it (yfinance's regularMarketChangePercent);
   *  FMP's screen sends none. */
  changePct?: number;
  volume?: number;
  beta?: number;
  dividend?: number;
  sector?: string;
  industry?: string;
  exchange?: string;
  country?: string;
  currency: string;
}

export interface Screen {
  rows: ScreenerRow[];
  /** The producer's filter names, a threshold with its operator (`Mkt Cap >`). */
  filters: Array<[name: string, value: unknown]>;
  /** Every match, of which `rows` are the first. */
  count: number;
}

export function readScreener(artifact: Record<string, unknown>): Screen {
  return {
    rows: records(artifact.results).map((s): ScreenerRow => {
      const symbol = text(s.symbol);
      return {
        symbol,
        name: text(s.companyName),
        price: finite(s.price),
        marketCap: finite(s.marketCap),
        changePct: finite(s.change),
        volume: finite(s.volume),
        beta: finite(s.beta),
        dividend: finite(s.lastAnnualDividend),
        sector: text(s.sector),
        industry: text(s.industry),
        exchange: text(s.exchangeShortName),
        country: text(s.country),
        // A screen can mix venues, so money resolves per row.
        currency: resolveCurrency(null, symbol),
      };
    }),
    filters: Object.entries(record(artifact.filters) ?? {}),
    count: finite(artifact.count) ?? 0,
  };
}

// ─── Company overview ───────────────────────────────────────────────

export const RATING_KEYS = ['strongBuy', 'buy', 'hold', 'sell', 'strongSell'] as const;

/** Each analyst rating's count, strongest buy first, a missing one as none. */
export function ratingCounts(ratings: Record<string, unknown>): Array<{ key: (typeof RATING_KEYS)[number]; value: number }> {
  return RATING_KEYS.map((key) => ({ key, value: (ratings[key] as number) || 0 }));
}

// ─── Filings ────────────────────────────────────────────────────────

export interface CurrentReport {
  filingDate?: string;
  /** `Item 2.02`, with each one's description where the artifact carries it. */
  items: string[];
  descs: string[];
  pressRelease: boolean;
  /** The filing on EDGAR. */
  sourceUrl?: string;
}

export type SecFiling =
  | {
      kind: '8-K';
      symbol?: string;
      /** Every filing in the window, of which `filings` are the first. */
      count: number;
      days?: number;
      filings: CurrentReport[];
    }
  | {
      kind: 'periodic';
      symbol?: string;
      /** `10-K`, `10-Q`. */
      form?: string;
      filingDate?: string;
      periodEnd?: string;
      cik?: string;
      sections?: number;
      recent8Ks?: number;
      sourceUrl?: string;
      earningsCall: boolean;
    };

export function readSecFiling(artifact: Record<string, unknown>): SecFiling | null {
  if (artifact.type !== 'sec_filing') return null;
  const symbol = text(artifact.symbol);
  if (artifact.filing_type === '8-K') {
    const filings = records(artifact.filings).map((f): CurrentReport => ({
      filingDate: text(f.filing_date),
      items: strings(f.items),
      // Older artifacts carry the numbers alone.
      descs: strings(f.items_desc),
      pressRelease: !!f.has_press_release,
      sourceUrl: text(f.source_url),
    }));
    return { kind: '8-K', symbol, count: finite(artifact.filing_count) ?? filings.length, days: finite(artifact.days_range), filings };
  }
  return {
    kind: 'periodic',
    symbol,
    form: text(artifact.filing_type),
    filingDate: text(artifact.filing_date),
    periodEnd: text(artifact.period_end),
    cik: text(artifact.cik),
    sections: finite(artifact.sections_extracted),
    recent8Ks: finite(artifact.recent_8k_count),
    sourceUrl: text(artifact.source_url),
    earningsCall: !!artifact.has_earnings_call,
  };
}

// ─── Web search ─────────────────────────────────────────────────────

export interface WebSearchHit {
  title?: string;
  /** The page's host without its `www.`, or empty for an address that is not one. */
  domain: string;
}

/** The hostname alone, never a port or a `user:password@`, so it is safe to
 *  hand a favicon service; empty for an address that is not one. */
export function hostOf(url: string | undefined): string {
  try {
    return url ? new URL(url).hostname.replace(/^www\./, '') : '';
  } catch {
    return '';
  }
}

export interface WebSearch {
  query?: string;
  /** The provider's direct answer, from whichever box it sent one in. */
  answer?: string;
  hits: WebSearchHit[];
}

export function readWebSearch(artifact: Record<string, unknown> | undefined): WebSearch {
  const box = record(artifact?.answer_box);
  return {
    query: text(artifact?.query),
    answer: text(artifact?.answer) ?? text(box?.answer) ?? text(box?.snippet) ?? text(record(artifact?.knowledge_graph)?.description),
    hits: records(artifact?.results).map((r) => ({ title: text(r.title), domain: hostOf(text(r.url)) })),
  };
}

// ─── Automations ────────────────────────────────────────────────────

export interface Automation {
  id?: string;
  name: string;
  status: string;
  triggerType?: string;
  /** The price trigger, checked; null when the config is not one. */
  priceTrigger: PriceTriggerConfig | null;
  /** A cron line, or a one-time trigger's own words. */
  schedule?: string;
  nextRunAt?: string;
  lastRunAt?: string;
  /** The agent it runs on: `ptc` in a workspace, any other outside one. */
  agentMode?: string;
  instruction?: string;
}

/** One automation as the tools send it, the list's rows and a detail alike. */
export function readAutomation(raw: Record<string, unknown>): Automation {
  return {
    id: text(raw.automation_id),
    name: text(raw.name) ?? '',
    status: text(raw.status) ?? '',
    triggerType: text(raw.trigger_type),
    priceTrigger: isPriceTriggerConfig(raw.trigger_config) ? raw.trigger_config : null,
    schedule: text(raw.schedule),
    nextRunAt: text(raw.next_run_at),
    lastRunAt: text(raw.last_run_at),
    agentMode: text(raw.agent_mode),
    instruction: text(raw.instruction),
  };
}

export type AutomationsArtifact =
  | { mode: 'list'; automations: Automation[]; total: number }
  | { mode: 'detail'; automation: Automation | null; totalExecutions: number }
  | { mode: 'created'; automation: Automation };

/** The automation tools' artifact, by the mode it was answered in. */
export function readAutomations(artifact: Record<string, unknown>): AutomationsArtifact | null {
  if (artifact.type !== 'automations') return null;
  switch (artifact.mode) {
    case 'list':
      return { mode: 'list', automations: records(artifact.automations).map(readAutomation), total: finite(artifact.total) ?? 0 };
    case 'detail': {
      const automation = record(artifact.automation);
      return {
        mode: 'detail',
        automation: automation ? readAutomation(automation) : null,
        totalExecutions: finite(artifact.total_executions) ?? 0,
      };
    }
    case 'created':
      return { mode: 'created', automation: readAutomation(artifact) };
    default:
      return null;
  }
}
