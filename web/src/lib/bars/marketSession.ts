/**
 * The market-session presentation model.
 *
 * "What does the price on screen represent right now?" used to be answered
 * four different ways (client ET clock, bar-date arithmetic, server phase,
 * WS liveness), each feeding a different surface — every mismatch between two
 * of them was a shipped bug (live-styled closed venues, after-hours prices
 * folded into daily candles, sticky price-line labels). This module is the
 * single derivation all of those surfaces consume:
 *
 * - the server's calendar-derived `market_phase` is the venue authority;
 * - the head bar's timestamp keeps the extended-hours label data-driven (the
 *   label matches the bar on screen, not the wall clock);
 * - WS liveness only ever upgrades the badge — it never overrides the phase.
 *
 * `deriveMarketSession` is pure; every consumer decision is a table test.
 */
import { isUSEquity, timezoneForSymbol } from './exchanges';
import { isSettledDailyHeadTime } from './formingBar';

export type ExtendedHoursType = 'pre' | 'post';

/** US intervals that render pre/post extended-hours data. */
export const EXTENDED_HOURS_INTERVALS = new Set(['1min', '5min', '15min', '30min', '1hour']);

/**
 * Classify a chart timestamp (venue wall-clock seconds, fake-UTC encoding)
 * against the US regular session: 'pre' before 09:30, 'post' at/after 16:00,
 * null inside regular hours. Only meaningful for US-listed equities — other
 * venues have no upstream extended-hours data.
 */
export function getExtendedHoursType(timeSec: number): ExtendedHoursType | null {
  const d = new Date(timeSec * 1000);
  const mins = d.getUTCHours() * 60 + d.getUTCMinutes();
  if (mins < 570) return 'pre'; // before 9:30
  if (mins >= 960) return 'post'; // 16:00 or later
  return null;
}

/**
 * What the series' last value represents, driving the last-value price-line
 * styling: live extended-session tape (amber/blue "Pre"/"After"), a settled
 * close (neutral grey — `settled-ext-*` when the head bar is an extended-hours
 * bar, so the label reads "AH Close"/"PM Close" instead of claiming the
 * official close), or a live regular-session price (null → default
 * bar-colored styling).
 */
export type PriceMark =
  | 'ext-pre'
  | 'ext-post'
  | 'settled-close'
  | 'settled-ext-pre'
  | 'settled-ext-post'
  | null;

/** Header venue-status badge. `live` = WS delivering; phase never overrides it. */
export type VenueBadge = 'live' | 'closed' | 'delayed';

export interface MarketSessionInputs {
  symbol: string | null | undefined;
  /** Legacy interval key (`1min` … `1day`). */
  interval: string;
  /** Server calendar phase (`pre|open|post|closed`); null = unknown (legacy backend). */
  phase: string | null;
  /** Chart fake-UTC seconds of the series head bar; null = empty series. */
  headBarTime: number | null;
  /** WS currently delivering ticks for this symbol. */
  wsLive?: boolean;
  /** Injection point for tests. */
  now?: Date;
}

export interface MarketSession {
  priceMark: PriceMark;
  badge: VenueBadge;
  /** 1day head-bar live-quote folding allowed (venue actually trading). */
  foldDailyQuote: boolean;
  /** Show the dashed official-close reference line (US after-hours only). */
  showRegularCloseLine: boolean;
}

/**
 * Derive the session presentation state for one (symbol, interval) series.
 *
 * Price-mark precedence: a `closed` phase settles everything (a closed venue
 * must never read as live, whatever window the head bar sits in) — but the
 * head bar's window still picks WHICH settled label applies: an ext-hours
 * head bar settles as the extended close, not the official one. Otherwise
 * the head bar's own timestamp picks the extended-hours label on US ext
 * intervals; otherwise a daily series whose head bar predates the venue's
 * current date is a settled close. A null phase falls back to the date-based
 * rules alone — pre-CMDP behavior.
 */
export function deriveMarketSession({
  symbol,
  interval,
  phase,
  headBarTime,
  wsLive = false,
  now = new Date(),
}: MarketSessionInputs): MarketSession {
  const daily = interval === '1day';
  const extInterval = !daily && isUSEquity(symbol) && EXTENDED_HOURS_INTERVALS.has(interval);
  const extType = extInterval && headBarTime != null ? getExtendedHoursType(headBarTime) : null;
  const settledDailyHead =
    daily && headBarTime != null && isSettledDailyHeadTime(headBarTime, timezoneForSymbol(symbol), now);

  let priceMark: PriceMark = null;
  if (headBarTime != null) {
    if (phase === 'closed') {
      priceMark = extType ? (extType === 'pre' ? 'settled-ext-pre' : 'settled-ext-post') : 'settled-close';
    } else if (extType) priceMark = extType === 'pre' ? 'ext-pre' : 'ext-post';
    else if (settledDailyHead) priceMark = 'settled-close';
  }

  return {
    priceMark,
    badge: wsLive ? 'live' : phase === 'closed' ? 'closed' : 'delayed',
    // Fold live quotes into the daily head bar only while the venue is
    // actually trading: post-close folding would stamp after-hours prices
    // into the settled daily candle (its close must stay the official close).
    // A null phase keeps the pre-CMDP date-only gate.
    foldDailyQuote:
      daily && headBarTime != null && !settledDailyHead && (phase == null || phase === 'open'),
    // Whenever the head bar is an after-hours bar — live or settled — the
    // last-value line marks the extended tape, so the official close gets
    // its own reference line.
    showRegularCloseLine: extType === 'post',
  };
}
