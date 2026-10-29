/**
 * What the cards and their panels call things and how they print a figure,
 * from what `cardReaders` read: the market's session, a chain's edge, a
 * movers list's title, an automation's schedule. One place, so a card and the
 * panel it opens say the same words.
 */
import type { TFunction } from 'i18next';
import {
  compactNumber,
  createDateFormatter,
  formatCalendarDay,
  formatCalendarDayRange,
  formatCalendarMonthDay,
  integer,
  relativeTime,
} from '@/lib/format';
import { cronToHuman } from '@/pages/Automations/utils/cron';
import { formatPriceTrigger } from '@/pages/Automations/utils/price';
import type { Automation, MarketMovers, OptionsChain } from './cardReaders';

// ─── The market's session ───────────────────────────────────────────

/** The fallback for a status with no `toolArtifact.marketStatus.*` key. */
const MARKET_STATUS_LABELS: Record<string, string> = {
  early_trading: 'Pre-Market',
  open: 'Regular',
  late_trading: 'After-Hours',
  closed: 'Closed',
};

/** A session's badge word; an unknown status stays raw. */
export function marketStatusLabel(t: TFunction, status: string | undefined): string {
  if (!status) return '';
  return t(`toolArtifact.marketStatus.${status}`, MARKET_STATUS_LABELS[status] || status);
}

/**
 * The extended session, abbreviated. Only `early_trading` and `late_trading`
 * carry an extended move, so anything else reads as after-hours. `long` is
 * a hero's line ("Pre-Mkt", "After-Hrs"), `short` a table's cell ("Pre", "AH").
 */
export function extendedHoursLabel(t: TFunction, status: string | undefined, variant: 'long' | 'short'): string {
  const session = status === 'early_trading' ? 'preMarket' : 'afterHours';
  return t(`toolArtifact.extendedHours.${session}.${variant}`);
}

// ─── Figures ────────────────────────────────────────────────────────

/**
 * The places a price keeps: two, or up to four under a dollar, where a US
 * stock quotes in hundredths of a cent and a 0.017 warrant would print as
 * $0.02. Only the places the figure uses, so a 0.24 stays $0.24.
 */
export function moneyPlaces(value: number | null | undefined): number {
  if (value == null || !Number.isFinite(value) || Math.abs(value) >= 1) return 2;
  for (let places = 2; places < 4; places++) {
    const scaled = value * 10 ** places;
    if (Math.abs(scaled - Math.round(scaled)) < 1e-6) return places;
  }
  return 4;
}

/** A day's volume as the other cards print one, `98.2K`, `6.0M`; a count under
 *  a thousand stays whole, where a contract's 4 would read `4.00`. */
export function formatVolume(value: number, locale: string): string {
  return Math.abs(value) < 1000 ? integer(value, locale) : compactNumber(value, locale);
}

const retrievedFormat = createDateFormatter({
  year: 'numeric', month: 'short', day: 'numeric', hour: 'numeric', minute: '2-digit',
});

/** When the tool fetched the rows, in the reader's clock: the tooltip on the edge. */
export function retrievedTitle(t: TFunction, asOfTs: number | undefined, locale: string): string | undefined {
  return asOfTs != null ? t('toolArtifact.retrievedAt', { time: retrievedFormat(asOfTs, locale) }) : undefined;
}

// ─── Options chains and movers ──────────────────────────────────────

/**
 * The words on a chain's edge, the card's and the panel's: `NVDA`, `OPTIONS ·
 * CALLS` (`CALLS` where the edge is short), `EXP OCT 26, 2026`, and the session
 * when it is not the regular one. A short edge has no room for a range of
 * expiries beside the session, and the expiry column already lists them.
 */
export function optionsEdge(
  chain: OptionsChain,
  t: TFunction,
  locale: string,
  short: boolean,
): { title: string; sub?: string; expiry?: string; status?: string } {
  const type = chain.types.length === 1 ? chain.types[0] : undefined;
  // With no underlying the title already says options; `CALLS` alone says it
  // on a short edge. The catalog phrases both, since zh says it in one word.
  let sub: string | undefined;
  if (type) sub = short || !chain.underlying ? t(`toolArtifact.optionTypes.${type}`) : t(`toolArtifact.optionsOfType.${type}`);
  else if (chain.underlying) sub = t('toolArtifact.options');
  const { expiries } = chain;
  const date = expiries.length > 1
    ? formatCalendarDayRange(expiries[0], expiries[expiries.length - 1], locale)
    : expiries.length ? formatCalendarDay(expiries[0], locale) : undefined;
  return {
    title: chain.underlying ?? t('toolArtifact.optionsChain'),
    sub,
    expiry: date && !(short && expiries.length > 1) ? t('toolArtifact.expiresOn', { date }) : undefined,
    // The regular session is the default reading, as on a quote.
    status: chain.status && chain.status !== 'open' ? marketStatusLabel(t, chain.status) : undefined,
  };
}

/** A row's expiry in its column: the day alone when every expiry shares a year, since the edge names it. */
export function expiryLabeler(expiries: readonly string[], locale: string): (day: string | undefined) => string {
  const format = new Set(expiries.map((e) => e.slice(0, 4))).size <= 1 ? formatCalendarMonthDay : formatCalendarDay;
  return (day = '') => format(day, locale);
}

/** `Top Gainers`, `Top Losers`, or `Market Movers` for a call that named neither. */
export function moversTitle(t: TFunction, direction: MarketMovers['direction']): string {
  if (direction === 'gainers') return t('toolArtifact.topGainers');
  if (direction === 'losers') return t('toolArtifact.topLosers');
  return t('toolArtifact.marketMovers');
}

// ─── Company overview ───────────────────────────────────────────────

const PERF_LABELS: Record<string, string> = { '1D': '1D', '5D': '5D', '1M': '1M', '3M': '3M', '6M': '6M', 'ytd': 'YTD', '1Y': '1Y', '3Y': '3Y', '5Y': '5Y' };

/** Each period a company's performance carries, in order, under its label. */
export function performancePoints(performance: Record<string, number> | undefined): Array<{ name: string; value: number }> {
  if (!performance) return [];
  return Object.entries(PERF_LABELS)
    .filter(([key]) => performance[key] != null)
    .map(([key, name]) => ({ name, value: performance[key] }));
}

/** A period's move, signed: `+1.25%`. */
export function formatPct(val: number | null | undefined): string {
  if (val == null) return 'N/A';
  const sign = val >= 0 ? '+' : '';
  return `${sign}${val.toFixed(2)}%`;
}

// ─── Filings ────────────────────────────────────────────────────────

/** `Item 2.02` → `2.02`; any other spelling stays whole. */
export function itemNumber(item: string): string {
  return item.replace(/^item\s+/i, '');
}

// ─── Automations: the card and its panel ────────────────────────────

const TRIGGER_KIND_KEYS: Record<string, string> = {
  cron: 'toolArtifact.recurring',
  once: 'toolArtifact.oneTime',
  price: 'toolArtifact.priceTrigger',
};

/** The kind of trigger in a word, or undefined for one the catalog has no word for. */
export function triggerKind(automation: Automation, t: TFunction): string | undefined {
  const key = automation.triggerType && TRIGGER_KIND_KEYS[automation.triggerType];
  return key ? t(key) : undefined;
}

/** When it runs, in words: the price it waits for, the schedule, or the next run. */
export function scheduleLabel(automation: Automation, t: TFunction, locale: string, now: number): string {
  if (automation.triggerType === 'price') return formatPriceTrigger(automation.priceTrigger, t);
  if (automation.triggerType === 'cron' && automation.schedule) return cronToHuman(automation.schedule, t, locale);
  if (automation.nextRunAt) return relativeTime(automation.nextRunAt, locale, now);
  return automation.schedule ?? '';
}
