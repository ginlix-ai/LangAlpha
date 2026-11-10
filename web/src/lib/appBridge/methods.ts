import type { QueryClient } from '@tanstack/react-query';
import type { TFunction } from 'i18next';
import i18n from '@/i18n';
import { fetchStockData } from '@/lib/bars/legacyBars';
import {
  asQuoteTier,
  FRESHNESS_TONE,
  freshnessSpecText,
  resolveChartFreshness,
  resolveFreshnessBadge,
  type FreshnessSpec,
} from '@/lib/freshness';
import { providerLabel } from '@/lib/providerLabels';
import { queryKeys } from '@/lib/queryKeys';
import { quoteKey, quoteQueryOptions, type QuoteRow } from '@/lib/quotes';
import { localDateStr } from '@/lib/utils';
import { getRecentThreads } from '@/pages/ChatAgent/utils/api/threads';
import {
  fetchEarningsCalendar,
  fetchTodayInsights,
  getCompanyProfiles,
  getNews,
  listPortfolio,
  listWatchlistItems,
  listWatchlists,
} from '@/pages/Dashboard/utils/api';
import type { Freshness as WireFreshness } from '@/types/market';
import type {
  Bar,
  BarSeries,
  BriefItem,
  EarningsItem,
  Freshness,
  Holding,
  NewsItem,
  Profile,
  Quote,
  Scope,
  ThreadItem,
  Watchlist,
} from './protocol';

/** A request the page refuses as malformed, answered `bad_request`. */
export class BadRequest extends Error {}

type Params = Record<string, unknown>;
type Row = Record<string, unknown>;

/** One method as the bridge runs it: checked, then read. */
export type BridgeMethod = (params: Params, queryClient: QueryClient) => Promise<unknown>;

const isRow = (v: unknown): v is Row => typeof v === 'object' && v !== null;

/** The list under `key` in a response the api layer leaves untyped, keeping only its objects. */
function listOf(data: unknown, key: string): Row[] {
  const list = isRow(data) ? data[key] : undefined;
  return Array.isArray(list) ? list.filter(isRow) : [];
}

const SYMBOL = /^\^?[A-Za-z0-9.=:-]{1,20}$/;
const DATE = /^\d{4}-\d{2}-\d{2}$/;
const INTERVALS = new Set(['1min', '5min', '15min', '30min', '1hour', '4hour', '1day']);
/** How long a quote row answers a read before it is fetched again. */
const QUOTE_STALE_MS = 10_000;

function symbols(raw: unknown, max: number): string[] {
  const list = typeof raw === 'string' ? raw.split(',') : raw;
  if (!Array.isArray(list)) throw new BadRequest('symbols must be a list');
  const out = [...new Set(list.map((s) => String(s).trim().toUpperCase()).filter(Boolean))];
  if (!out.length || out.length > max || !out.every((s) => SYMBOL.test(s))) {
    throw new BadRequest(`symbols must be 1 to ${max} tickers`);
  }
  return out;
}

function symbol(params: Params): string {
  const s = String(params.symbol ?? '').trim().toUpperCase();
  if (!SYMBOL.test(s)) throw new BadRequest('symbol is not a ticker');
  return s;
}

function date(params: Params, key: string): string | null {
  const v = params[key];
  if (v === undefined || v === null || v === '') return null;
  if (typeof v !== 'string' || !DATE.test(v)) throw new BadRequest(`${key} must be YYYY-MM-DD`);
  return v;
}

function limit(params: Params, fallback: number, max: number): number {
  const n = Number(params.limit ?? fallback);
  return Number.isFinite(n) ? Math.min(Math.max(Math.trunc(n), 1), max) : fallback;
}

const num = (v: unknown): number | null => {
  if (v === null || v === undefined || v === '') return null;
  const n = Number(v);
  return Number.isFinite(n) ? n : null;
};
const str = (v: unknown): string | null => (typeof v === 'string' && v ? v : null);

/** Unix ms as ISO, or null for a missing or unrepresentable time. */
const isoMs = (ms: unknown): string | null =>
  (typeof ms === 'number' && ms > 0 && ms < 8.64e15 ? new Date(ms).toISOString() : null);

/**
 * The app's freshness decision in the app's words. These methods run outside
 * render, so the catalog is read per call and follows a language switch.
 */
function toFreshness(spec: FreshnessSpec | null, asOfMs: unknown, source: string | null | undefined): Freshness | null {
  if (!spec) return null;
  const t: TFunction = i18n.getFixedT(i18n.language);
  return {
    state: spec.state,
    label: freshnessSpecText(t, spec) ?? '',
    tone: FRESHNESS_TONE[spec.state],
    asOf: isoMs(asOfMs),
    source: providerLabel(source, t),
  };
}

/**
 * A read reused across requests for `staleTime`, keyed on its checked
 * arguments so two spellings of one request share an answer. Every app on the
 * page asks through the one query cache.
 */
function cached<A>(
  method: string,
  staleTime: number,
  parse: (params: Params) => A,
  run: (args: A) => Promise<unknown>,
): BridgeMethod {
  return (params, queryClient) => {
    const args = parse(params);
    return queryClient.fetchQuery({ queryKey: queryKeys.appBridge.method(method, args), queryFn: () => run(args), staleTime });
  };
}

// -- Quotes ------------------------------------------------------------------

export interface QuoteArgs {
  /** Quote-cache keys: upper case, an index without its `^`. */
  keys: string[];
  isIndex: boolean;
}

/** A read names at most a screen of tickers; a stream follows a watchlist's worth. */
export function parseQuotes(params: Params, max = 50): QuoteArgs {
  const isIndex = params.kind === 'index';
  return { keys: [...new Set(symbols(params.symbols, max).map((s) => quoteKey(s, isIndex)))], isIndex };
}

/**
 * The app's shape of a quote row, under the key the app asked by. Before the
 * open the day has no regular print, so the row's `price` is empty while its
 * change from the previous close is not: the price is then the latest
 * minute's, which that change is measured to.
 */
export function toQuote(key: string, row: QuoteRow): Quote {
  return {
    symbol: key,
    name: str(row.name),
    price: num(row.price) ?? num(row.last_minute_close),
    change: num(row.change),
    changePercent: num(row.change_percent),
    previousClose: num(row.previous_close),
    open: num(row.open),
    high: num(row.high),
    low: num(row.low),
    volume: num(row.volume),
    currency: str(row.currency),
    freshness: toFreshness(
      resolveFreshnessBadge({ tier: asQuoteTier(row.tier), freshness: row.freshness }),
      row.as_of ?? row.freshness?.actual_latest,
      str(row.source) ?? row.freshness?.source,
    ),
  };
}

/**
 * Quotes are the page's own quote rows: batched with every other quote the
 * page asks for, and kept current by the market socket while one is open.
 */
export async function readQuoteArgs({ keys, isIndex }: QuoteArgs, queryClient: QueryClient): Promise<Quote[]> {
  const rows = await Promise.all(keys.map((key) =>
    queryClient.fetchQuery(quoteQueryOptions(queryClient, key, { isIndex, staleTime: QUOTE_STALE_MS }))));
  return rows.flatMap((row, i) => (row ? [toQuote(keys[i], row)] : []));
}

const readQuotes: BridgeMethod = (params, queryClient) => readQuoteArgs(parseQuotes(params), queryClient);

// -- Everything else -----------------------------------------------------------

interface BarsArgs { series: string; interval: string; from: string | null; to: string | null }

function parseBars(params: Params): BarsArgs {
  const interval = String(params.interval ?? '5min');
  if (!INTERVALS.has(interval)) throw new BadRequest(`interval must be one of ${[...INTERVALS].join(', ')}`);
  const sym = symbol(params);
  // The bars routes key indexes on the `^` spelling the snapshot routes drop.
  const series = params.kind === 'index' && !sym.startsWith('^') ? `^${sym}` : sym;
  return { series, interval, from: date(params, 'from'), to: date(params, 'to') };
}

/** A series as cached: the wire freshness, so its words are read in the language of each answer. */
interface CachedSeries { bars: Bar[]; freshness: WireFreshness | null }

const BARS_STALE_MS = 30_000;
/** Most series one `bars.batch` fetches at once, so fifty symbols are not fifty open requests. */
const BARS_BATCH_POOL = 6;

async function fetchSeries({ series, interval, from, to }: BarsArgs): Promise<CachedSeries> {
  const res = await fetchStockData(series, interval, from ?? undefined, to ?? undefined);
  if (res.error) throw new Error(res.error);
  // The daily route answers its cached window whatever `from` says, so the
  // range the app asked for is cut here. Bar times are wall clock read as UTC.
  const lo = from ? Date.parse(from) / 1000 : -Infinity;
  const hi = to ? Date.parse(to) / 1000 + 86_400 : Infinity;
  return { bars: res.data.filter((b) => b.time >= lo && b.time < hi), freshness: res.meta?.freshness ?? null };
}

/** One series through the cache `bars.get` and `bars.batch` share. */
const readSeries = (args: BarsArgs, queryClient: QueryClient): Promise<CachedSeries> =>
  queryClient.fetchQuery({
    queryKey: queryKeys.appBridge.method('bars.get', args),
    queryFn: () => fetchSeries(args),
    staleTime: BARS_STALE_MS,
  });

const readBars: BridgeMethod = async (params, queryClient) => (await readSeries(parseBars(params), queryClient)).bars;

const seriesFreshness = (freshness: WireFreshness | null): Freshness | null =>
  toFreshness(resolveChartFreshness(freshness), freshness?.actual_latest, freshness?.source);

/**
 * Many symbols' bars over one range in one call, in the order asked. Every
 * argument is checked before anything is fetched; a symbol whose fetch fails
 * carries its error and leaves the rest standing.
 */
const readBarsBatch: BridgeMethod = async (params, queryClient) => {
  const all = symbols(params.symbols, 50).map((s) => ({ symbol: s, args: parseBars({ ...params, symbol: s }) }));
  const out: BarSeries[] = new Array(all.length);
  let next = 0;
  const worker = async () => {
    while (next < all.length) {
      const i = next++;
      const { symbol: s, args } = all[i];
      try {
        const { bars, freshness } = await readSeries(args, queryClient);
        out[i] = { symbol: s, bars, freshness: seriesFreshness(freshness), error: null };
      } catch (e) {
        out[i] = { symbol: s, bars: [], freshness: null, error: (e as Error).message || 'Request failed' };
      }
    }
  };
  await Promise.all(Array.from({ length: Math.min(BARS_BATCH_POOL, all.length) }, worker));
  return out;
};

const PROFILES_STALE_MS = 3_600_000;

/**
 * The server's profile rows are the app's shape already; the symbol is the
 * spelling asked by. An answer some symbol failed in is given but not kept:
 * held for the hour a whole one is, the symbol would read as having no
 * profile long after the vendor answered again.
 */
const readProfiles: BridgeMethod = async (params, queryClient) => {
  const list = symbols(params.symbols, 50);
  const queryKey = queryKeys.appBridge.method('profiles.get', list);
  const { results, errors } = await queryClient.fetchQuery({
    queryKey,
    queryFn: () => getCompanyProfiles(list),
    staleTime: PROFILES_STALE_MS,
  });
  if (Object.keys(errors).length) void queryClient.invalidateQueries({ queryKey, exact: true });
  return list.flatMap((s): Profile[] => (results[s] ? [{ ...results[s], symbol: s }] : []));
};

async function readWatchlists(): Promise<Watchlist[]> {
  return Promise.all(listOf(await listWatchlists(), 'watchlists').map(async (w) => {
    const id = String(w.watchlist_id);
    return {
      id,
      name: String(w.name ?? ''),
      items: listOf(await listWatchlistItems(id), 'items').map((i) => ({ symbol: String(i.symbol), name: str(i.name) })),
    };
  }));
}

interface NewsArgs { tickers: string[] | null; limit: number; language: string }

const parseNews = (params: Params): NewsArgs => ({
  tickers: params.tickers === undefined ? null : symbols(params.tickers, 20),
  limit: limit(params, 10, 50),
  // The feed is picked by the language it is asked in (`getNews`), so each
  // language is an answer of its own.
  language: i18n.language,
});

async function readNews({ tickers, limit: count }: NewsArgs): Promise<NewsItem[]> {
  const { results } = await getNews({ tickers: tickers ?? undefined, limit: count });
  return results.map((n) => ({
    id: String(n.id),
    title: String(n.title ?? ''),
    url: str(n.article_url),
    source: (isRow(n.source) ? str(n.source.name) : null) ?? str(n.source),
    publishedAt: str(n.published_at),
    tickers: Array.isArray(n.tickers) ? n.tickers.map(String) : [],
    imageUrl: str(n.image_url),
  }));
}

// The market's briefs only. The same read carries the user's personalized
// brief, which is written from their holdings and so is not `brief`'s to give.
async function readBrief(): Promise<BriefItem[]> {
  return (await fetchTodayInsights()).filter((i) => i.type !== 'personalized').map((i) => ({
    id: String(i.market_insight_id ?? ''),
    title: String(i.headline ?? ''),
    summary: str(i.summary),
    createdAt: str(i.completed_at),
  }));
}

interface EarningsArgs { from: string; to: string; symbols: string[] | null; limit: number }

/** The default window is the user's own days, as the dashboard's calendar reads them, not UTC's. */
function parseEarnings(params: Params): EarningsArgs {
  const now = Date.now();
  return {
    from: date(params, 'from') ?? localDateStr(now),
    to: date(params, 'to') ?? localDateStr(now + 7 * 86_400_000),
    symbols: params.symbols === undefined ? null : symbols(params.symbols, 200),
    limit: limit(params, 200, 500),
  };
}

async function readEarnings(args: EarningsArgs): Promise<EarningsItem[]> {
  const only = args.symbols ? new Set(args.symbols) : null;
  const { data } = await fetchEarningsCalendar({ from: args.from, to: args.to });
  // Unfiltered, the calendar is every venue's reporters; keep the US
  // listings the dashboard's own calendar shows, soonest first.
  return data
    .filter((e) => (only ? only.has(String(e.symbol).toUpperCase()) : !String(e.symbol).includes('.')))
    .sort((a, b) => String(a.date).localeCompare(String(b.date)))
    .slice(0, args.limit)
    .map((e) => ({
      symbol: String(e.symbol),
      date: String(e.date),
      time: str(e.time),
      epsEstimated: num(e.epsEstimated),
      revenueEstimated: num(e.revenueEstimated),
    }));
}

async function readHoldings(): Promise<Holding[]> {
  return listOf(await listPortfolio(), 'holdings').map((h) => ({
    symbol: String(h.symbol),
    name: str(h.name),
    quantity: num(h.quantity) ?? 0,
    averageCost: num(h.average_cost),
    currency: str(h.currency),
    account: str(h.account_name),
  }));
}

/** Through the recent-threads widget's own query, so a thread the user just renamed or started shows here too. */
const readThreads: BridgeMethod = async (params, queryClient) => {
  const count = limit(params, 10, 50);
  const data: unknown = await queryClient.fetchQuery({
    queryKey: queryKeys.threads.recent(count),
    queryFn: () => getRecentThreads(count, 0),
    staleTime: 30_000,
  });
  return listOf(data, 'threads').map((t): ThreadItem => ({
    id: String(t.thread_id),
    title: String(t.title ?? ''),
    workspaceId: String(t.workspace_id),
    updatedAt: str(t.updated_at),
  }));
};

const noArgs = (): null => null;

// -- The registry ---------------------------------------------------------------

/** A method answered once per `req`. */
interface ReadEntry { scope: Scope; kind: 'read'; run: BridgeMethod }
/** A method held open by a `sub`: its arguments, checked, and the rows as they stand when it starts. */
interface StreamEntry {
  scope: Scope;
  kind: 'stream';
  parse: (params: Params) => QuoteArgs;
  first: (args: QuoteArgs, queryClient: QueryClient) => Promise<Quote[]>;
}
export type MethodEntry = ReadEntry | StreamEntry;

const read = (scope: Scope, run: BridgeMethod): ReadEntry => ({ scope, kind: 'read', run });

/** A stream follows a watchlist's worth of names; a read, a screen's. */
const STREAM_MAX_SYMBOLS = 200;

/**
 * Every method an app may call, the scope that gates it, and whether a `req`
 * or a `sub` calls it. The kit names these too (pinned by
 * `tests/unit/plugins/test_dashboard_kit_protocol.py`).
 */
export const METHODS = {
  'quotes.get': read('quotes', readQuotes),
  'bars.get': read('quotes', readBars),
  'bars.batch': read('quotes', readBarsBatch),
  'profiles.get': read('quotes', readProfiles),
  'quotes.stream': {
    scope: 'quotes',
    kind: 'stream',
    parse: (params) => parseQuotes(params, STREAM_MAX_SYMBOLS),
    first: (args, queryClient) => readQuoteArgs(args, queryClient),
  },
  'watchlists.list': read('watchlists', cached('watchlists.list', 60_000, noArgs, readWatchlists)),
  'news.list': read('news', cached('news.list', 120_000, parseNews, readNews)),
  'brief.today': read('brief', cached('brief.today', 300_000, noArgs, readBrief)),
  'earnings.calendar': read('earnings', cached('earnings.calendar', 600_000, parseEarnings, readEarnings)),
  'portfolio.holdings': read('portfolio', cached('portfolio.holdings', 60_000, noArgs, readHoldings)),
  'threads.recent': read('threads', readThreads),
} as const satisfies Record<string, MethodEntry>;

const REGISTRY: Readonly<Record<string, MethodEntry | undefined>> = METHODS;

/** The method a frame named, by own key only: `toString` names nothing. */
export function methodEntry(name: string): MethodEntry | null {
  return Object.hasOwn(REGISTRY, name) ? REGISTRY[name] ?? null : null;
}
