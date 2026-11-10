/**
 * The messages between an app the user's agent built and the LangAlpha page
 * framing it. The app holds no credential: it asks, and this page reads what
 * it asks for as the signed-in user, inside the scopes the user granted. The
 * dashboard kit (`dashboard-page/references/kit.js`) is the app side.
 *
 * Every message carries `la: 1`. A change that breaks the app side takes a new
 * version, and this page keeps answering the old one: a kit is copied into the
 * app, so the copies in the wild are never updated.
 */

export const PROTOCOL_VERSION = 1;

/**
 * What an app may read. The first five are granted on asking: they are the
 * market and the user's own lists, which nothing outside the frame learns
 * much from. The last two are the user's money and their work, so the page
 * asks the user first.
 */
export const SCOPES = ['quotes', 'watchlists', 'news', 'brief', 'earnings', 'portfolio', 'threads'] as const;
export type Scope = (typeof SCOPES)[number];
export const ASK_FIRST: ReadonlySet<Scope> = new Set<Scope>(['portfolio', 'threads']);

export interface Theme {
  mode: 'light' | 'dark';
  /** CSS custom properties by name (`--color-text-primary`), resolved. */
  vars: Record<string, string>;
}

export type OpenTarget =
  | { thread: string; workspace?: string }
  | { workspace: string }
  | { symbol: string };

export type ErrorCode = 'scope' | 'rate' | 'bad_request' | 'unavailable' | 'failed';

/**
 * App → page, as `parseAppMessage` reads it: every field checked. `doc` is the
 * id a kit picks once per document and puts on every message, so the page
 * answers only the document it welcomed; a kit from before it sends none.
 * `bye` is that document going away. A method name is checked against the
 * methods the page has (`methodEntry`), which this file does not hold.
 */
export type AppMessage = { doc: string | null } & (
  | { type: 'hello'; scopes: Scope[] }
  | { type: 'req' | 'sub'; id: string; method: string; params: Record<string, unknown> }
  | { type: 'unsub'; id: string }
  | { type: 'open'; target: OpenTarget }
  | { type: 'bye' }
);

type Fields = Record<string, unknown>;

const isFields = (v: unknown): v is Fields => typeof v === 'object' && v !== null;
const text = (v: unknown): string | null => (typeof v === 'string' && v ? v : null);
const isScope = (s: unknown): s is Scope => (SCOPES as readonly unknown[]).includes(s);

/** A call's id as the kit numbers it; the page echoes it back as a string. */
function callId(v: unknown): string | null {
  if (typeof v === 'number' && Number.isFinite(v)) return String(v);
  return text(v);
}

function openTarget(v: unknown): OpenTarget | null {
  if (!isFields(v)) return null;
  const thread = text(v.thread);
  const workspace = text(v.workspace);
  const symbol = text(v.symbol);
  if (thread) return workspace ? { thread, workspace } : { thread };
  if (workspace) return { workspace };
  if (symbol) return { symbol };
  return null;
}

/**
 * A message from a frame, or null for anything that is not one this page
 * speaks: another protocol version, an unknown type, a call with no id. Scopes
 * the page does not know are dropped rather than refusing the hello, so a kit
 * newer than the page still gets what both know.
 */
export function parseAppMessage(data: unknown): AppMessage | null {
  if (!isFields(data) || data.la !== PROTOCOL_VERSION) return null;
  const doc = text(data.doc)?.slice(0, 64) ?? null;
  switch (data.type) {
    case 'hello': {
      const scopes = Array.isArray(data.scopes) ? [...new Set(data.scopes.filter(isScope))] : [];
      return { doc, type: 'hello', scopes };
    }
    case 'req':
    case 'sub': {
      const id = callId(data.id);
      const method = text(data.method);
      if (!id || !method) return null;
      const params = isFields(data.params) && !Array.isArray(data.params) ? data.params : {};
      return { doc, type: data.type, id, method, params };
    }
    case 'unsub': {
      const id = callId(data.id);
      return id ? { doc, type: 'unsub', id } : null;
    }
    case 'open': {
      const target = openTarget(data.target);
      return target ? { doc, type: 'open', target } : null;
    }
    case 'bye':
      return { doc, type: 'bye' };
    default:
      return null;
  }
}

/**
 * The kit's own words in the user's language, by dotted key (`ago.minutes`),
 * with `{{name}}` placeholders the kit fills in. A key it is not sent keeps
 * its English.
 */
export type KitStrings = Record<string, string>;

/** Page → app. `doc` echoes the welcomed document's, for a kit to drop what was meant for another. */
export type PageMessage = { la: 1; doc?: string } & (
  | { type: 'welcome'; scopes: Scope[]; pending: Scope[]; theme: Theme; locale: string; strings?: KitStrings }
  /** Everything granted now, whole: a scope no longer on it was taken away. */
  | { type: 'grant'; scopes: Scope[] }
  | { type: 'theme'; theme: Theme }
  | { type: 'locale'; locale: string; strings: KitStrings }
  | { type: 'res'; id: string; ok: true; data: unknown }
  | { type: 'res'; id: string; ok: false; error: { code: ErrorCode; message: string } }
  /** A `quotes.stream` subscription's quote, whole, each time it changes. */
  | { type: 'evt'; id: string; data: Quote }
);

// Shapes of `res.data` / `evt.data`, normalized here so the app side never
// sees a backend response and a backend change never reaches a built app.

/**
 * How current a quote or bar series is, in the words and color the app itself
 * shows it with, so a built page never words a delay differently from the
 * page around it.
 */
export interface Freshness {
  state: 'live' | 'delayed' | 'eod' | 'stale' | 'incomplete';
  /** The app's own words for the state, in the user's language ("Realtime", "Delayed 15 min", "Last close"). */
  label: string;
  /** The color it takes: FRESHNESS_TONE in web/src/lib/freshness.ts. */
  tone: 'live' | 'neutral' | 'warning';
  /** ISO time of the newest print or bar, when known. */
  asOf: string | null;
  /** The data provider's display name, when known. */
  source: string | null;
}

export interface Quote {
  symbol: string;
  name: string | null;
  price: number | null;
  change: number | null;
  changePercent: number | null;
  previousClose: number | null;
  open: number | null;
  high: number | null;
  low: number | null;
  volume: number | null;
  currency: string | null;
  /** Null when the row carries nothing that can be stated. */
  freshness: Freshness | null;
}

/** `time` is seconds on the market's wall clock, read as UTC: format it in UTC to show market time. */
export interface Bar { time: number; open: number; high: number; low: number; close: number; volume: number }

/** One symbol's answer to `bars.batch`: a symbol that failed carries `error` and no bars. */
export interface BarSeries { symbol: string; bars: Bar[]; freshness: Freshness | null; error: string | null }

export interface Profile {
  symbol: string;
  name: string | null;
  sector: string | null;
  industry: string | null;
  /** In `currency`. */
  marketCap: number | null;
  currency: string | null;
  exchange: string | null;
  country: string | null;
}

export interface Watchlist { id: string; name: string; items: { symbol: string; name: string | null }[] }

export interface NewsItem {
  id: string;
  title: string;
  url: string | null;
  source: string | null;
  publishedAt: string | null;
  tickers: string[];
  imageUrl: string | null;
}

export interface BriefItem { id: string; title: string; summary: string | null; createdAt: string | null }

export interface EarningsItem {
  symbol: string;
  date: string;
  time: string | null;
  epsEstimated: number | null;
  revenueEstimated: number | null;
}

export interface Holding {
  symbol: string;
  name: string | null;
  quantity: number;
  averageCost: number | null;
  currency: string | null;
  account: string | null;
}

export interface ThreadItem { id: string; title: string; workspaceId: string; updatedAt: string | null }
