/**
 * How current a quote or chart series is, decided once. The inline artifact
 * badge and the MarketView header both read from here, so the same row never
 * reads "Last close" in one place and "Delayed 1440 min" in the other.
 */
import type { TFunction } from 'i18next';
import { createDateFormatter } from '@/lib/format';
import { timezoneForSymbol } from '@/lib/bars/exchanges';
import { dateStrInTz } from '@/lib/utils';
import { providerLabel } from '@/lib/providerLabels';
import type { Freshness, QuoteTier } from '@/types/market';

/** The two freshness fields a quote-bearing row may carry. Both are optional:
 *  rows produced before the backend added them have neither. */
export interface FreshnessFields {
  tier?: QuoteTier | null;
  freshness?: Freshness | null;
}

export type FreshnessState = 'live' | 'delayed' | 'stale' | 'eod' | 'incomplete';

/**
 * The color a freshness state carries, one table for every surface that shows
 * one (DESIGN.md: semantic color is for meaning). A delay or a last close is a
 * tier working as sold and stays neutral; only stale or short-of-close data is
 * a fault, and only a fault takes the warning color.
 */
export type FreshnessTone = 'live' | 'neutral' | 'warning';

export const FRESHNESS_TONE: Record<FreshnessState, FreshnessTone> = {
  live: 'live',
  delayed: 'neutral',
  eod: 'neutral',
  stale: 'warning',
  incomplete: 'warning',
};

export interface FreshnessSpec {
  /** i18n key under `marketView.header.*`. */
  key: string;
  /** Interpolation for the minute-count labels; absent for fixed copy. */
  minutes?: number;
  state: FreshnessState;
}

/** Whole minutes in a lag, floored at 1 so a sub-minute lag never reads "0 min". */
export function lagMinutes(seconds: number | null | undefined): number {
  return Math.max(1, Math.round((seconds ?? 0) / 60));
}

export function isDailyInterval(interval: string | null | undefined): boolean {
  return interval === '1day' || interval === 'daily' || interval === '1d';
}

/**
 * Resolve a row's declared tier plus measured freshness into the one label it
 * deserves, or null when the row carries nothing that can be stated.
 *
 * A measurement beats a declaration, with two deliberate exceptions: a measured
 * `live` reads live whatever the tier claims, and an `eod` tier reads "Last
 * close" rather than "Stale", because end-of-day data being hours behind the
 * venue clock is the product, not a fault. Current data on a closed venue is
 * the session's final price, so it reads "Last close" too, never live.
 */
export function resolveFreshnessBadge({ tier, freshness }: FreshnessFields): FreshnessSpec | null {
  // A declared 15-minute delay is the final price once the venue has closed too;
  // only a measurement can say a closed venue's row fell short.
  const current =
    (freshness?.measured && freshness.label === 'live') ||
    (!freshness?.measured && (tier === 'realtime' || tier === 'delayed_15m' || freshness?.label === 'delayed'));
  if (current && freshness?.closed) return { key: 'marketView.header.statusLastClose', state: 'eod' };
  if (freshness?.measured && freshness.label === 'live') {
    return { key: 'marketView.header.statusRealtime', state: 'live' };
  }
  if (tier === 'eod') return { key: 'marketView.header.statusLastClose', state: 'eod' };
  if (freshness?.label === 'stale') return { key: 'marketView.header.statusStale', state: 'stale' };
  if (freshness?.measured && freshness.label === 'delayed') {
    // A daily series one session behind is yesterday's close, which is what a
    // daily publisher shows all session long: "Last close", not "1440 min".
    if (isDailyInterval(freshness.interval)) return { key: 'marketView.header.statusLastClose', state: 'eod' };
    return { key: 'marketView.header.statusDelayedMin', minutes: lagMinutes(freshness.lag_s), state: 'delayed' };
  }
  if (freshness?.measured && freshness.label === 'incomplete') {
    return { key: 'marketView.header.statusIncomplete', state: 'incomplete' };
  }
  // A declared delay the measurement could not see (bars wider than the delay).
  if (freshness && !freshness.measured && freshness.label === 'delayed') {
    return { key: 'marketView.header.statusDelayed15m', state: 'delayed' };
  }
  if (tier === 'realtime') return { key: 'marketView.header.statusRealtime', state: 'live' };
  if (tier === 'delayed_15m') return { key: 'marketView.header.statusDelayed15m', state: 'delayed' };
  return null;
}

/**
 * The same decision for a chart series, in the chart line's own phrasing: a
 * bar series has no tier, and a closed venue's shortfall is minutes short of
 * the close rather than an incomplete quote.
 */
export function resolveChartFreshness(freshness: Freshness | null | undefined): FreshnessSpec | null {
  const spec = resolveFreshnessBadge({ freshness });
  if (!spec) return null;
  if (spec.state === 'live') return { ...spec, key: 'marketView.header.chartLive' };
  if (spec.state === 'incomplete') {
    return { ...spec, key: 'marketView.header.chartShortOfClose', minutes: lagMinutes(freshness?.lag_s) };
  }
  if (spec.key === 'marketView.header.statusDelayedMin') return { ...spec, key: 'marketView.header.chartDelayedMin' };
  return spec;
}

/** True when the row is honestly live: measured live, or declared realtime
 *  with nothing measured against it. */
export function isLiveRow(fields: FreshnessFields): boolean {
  return resolveFreshnessBadge(fields)?.state === 'live';
}

/** True when the row carries any freshness information, so callers can keep
 *  rendering a pre-contract row exactly as they always did. */
export function hasFreshnessFields({ tier, freshness }: FreshnessFields): boolean {
  return tier != null || freshness != null;
}

export function freshnessSpecText(t: TFunction, spec: FreshnessSpec | null): string | null {
  if (!spec) return null;
  return spec.minutes != null ? t(spec.key, { minutes: spec.minutes }) : t(spec.key);
}

export function freshnessBadgeText(t: TFunction, fields: FreshnessFields): string | null {
  return freshnessSpecText(t, resolveFreshnessBadge(fields));
}

type VenueShape = 'clock' | 'date' | 'dateClock';

const CLOCK: Intl.DateTimeFormatOptions = { hour: '2-digit', minute: '2-digit', hour12: false };
const VENUE_SHAPES: Record<VenueShape, Intl.DateTimeFormatOptions> = {
  clock: CLOCK,
  date: { month: 'short', day: 'numeric' },
  dateClock: { month: 'short', day: 'numeric', ...CLOCK },
};
const venueFormatters = new Map<string, (d: number, locale: string) => string>();

/** A stamp on the symbol's venue clock, in the app's language rather than the
 *  browser's, so a zh session never reads "Sep 24". */
function venueFormat(ms: number, symbol: string, shape: VenueShape, locale: string): string {
  const timeZone = timezoneForSymbol(symbol);
  const key = `${shape}|${timeZone}`;
  let format = venueFormatters.get(key);
  if (!format) {
    format = createDateFormatter({ ...VENUE_SHAPES[shape], timeZone });
    venueFormatters.set(key, format);
  }
  return format(ms, locale);
}

/** `HH:MM` on the symbol's venue clock, prefixed with the date when that is not
 *  the venue's today (a Thursday close read on Sunday), or null for no stamp. */
export function venueTime(
  ms: number | null | undefined,
  symbol: string,
  locale: string,
  now: number,
): string | null {
  if (typeof ms !== 'number' || ms <= 0) return null;
  const tz = timezoneForSymbol(symbol);
  const sameDay = dateStrInTz(ms, tz) === dateStrInTz(now, tz);
  return venueFormat(ms, symbol, sameDay ? 'clock' : 'dateClock', locale);
}

/** The last bar's stamp in venue time: a date for a daily series, a time otherwise. */
export function lastBarLabel(
  freshness: Freshness | null | undefined,
  symbol: string,
  locale: string,
  now: number,
): string | null {
  const ms = freshness?.actual_latest;
  if (typeof ms !== 'number' || ms <= 0) return null;
  return isDailyInterval(freshness?.interval)
    ? venueFormat(ms, symbol, 'date', locale)
    : venueTime(ms, symbol, locale, now);
}

export interface ChartFreshnessParts {
  source: string | null;
  state: string | null;
  lastBar: string | null;
}

/** The chart series' source, state and last bar as separate strings, so the
 *  header (joined on one line) and the badge tooltip (one line each) cannot
 *  order or word them differently. */
export function chartFreshnessParts(
  t: TFunction,
  freshness: Freshness | null | undefined,
  symbol: string,
  locale: string,
  now: number,
): ChartFreshnessParts {
  const bar = lastBarLabel(freshness, symbol, locale, now);
  return {
    source: providerLabel(freshness?.source, t),
    state: freshnessSpecText(t, resolveChartFreshness(freshness)),
    lastBar: bar ? t('marketView.header.chartLastBar', { time: bar }) : null,
  };
}

const QUOTE_TIERS: ReadonlySet<string> = new Set<QuoteTier>(['realtime', 'delayed_15m', 'eod']);

/** Narrow an artifact row's untyped `tier` to a declared tier, or null. */
export function asQuoteTier(value: unknown): QuoteTier | null {
  return typeof value === 'string' && QUOTE_TIERS.has(value) ? (value as QuoteTier) : null;
}
