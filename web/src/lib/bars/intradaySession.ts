/**
 * The latest trading session in a run of intraday bars: the regular-hours
 * bars of the most recent day that has any, and the close of the session
 * before it. Bars are chart bars (`rowsToChartBars`): times are the venue's
 * wall clock, in seconds, encoded as UTC, so a day and a time of day are read
 * straight off them. Hours come from the venue registry (`regularHoursForSymbol`).
 */
import type { SessionHours } from './exchanges';

interface Bar {
  time: number;
  close: number;
}

const wallDate = (sec: number) => new Date(sec * 1000).toISOString().slice(0, 10);
/** Minutes past midnight of a bar's time, in the venue's wall clock. */
const wallMinutes = (sec: number) => Math.floor((((sec % 86_400) + 86_400) % 86_400) / 60);

/** Each span's end is inclusive: the bar stamped at the close carries the closing print. */
const inHours = (sec: number, hours: SessionHours | null) => {
  if (!hours) return true;
  const minutes = wallMinutes(sec);
  return hours.some(([open, close]) => minutes >= open && minutes <= close);
};

export interface SessionSlice<B extends Bar> {
  /** The session's day, `YYYY-MM-DD` on the venue's calendar. */
  date: string;
  /** Its bars, oldest first. */
  bars: B[];
  /** The last regular-hours close before it, when the bars reach back that far. */
  previousClose: number | null;
}

/**
 * The regular-hours bars of the latest day that has any. Some series (VIX)
 * carry overnight bars, so the last bar's own day may have none on a pre-open
 * morning; the last day that does is the session. With no regular-hours bar
 * at all, the last day's bars stand in.
 */
export function latestSession<B extends Bar>(bars: readonly B[], hours: SessionHours | null): SessionSlice<B> | null {
  if (bars.length === 0) return null;
  const sorted = [...bars].sort((a, b) => a.time - b.time);
  const regular = sorted.filter((bar) => inHours(bar.time, hours));
  const pool = regular.length > 0 ? regular : sorted;
  const date = wallDate(pool[pool.length - 1].time);
  const earlier = regular.filter((bar) => wallDate(bar.time) < date);
  return {
    date,
    bars: pool.filter((bar) => wallDate(bar.time) === date),
    previousClose: earlier.length > 0 ? earlier[earlier.length - 1].close : null,
  };
}

/** `HH:MM` of a bar's time, in the venue's wall clock. */
export function wallClock(sec: number): string {
  return new Date(sec * 1000).toISOString().slice(11, 16);
}

export interface SessionLine {
  date: string;
  /** Closes, oldest first. */
  values: number[];
  /** Where each close sits across the session, 0 at its open and 1 at its close. */
  at: number[];
  previousClose: number | null;
  /** The session's open and close, in minutes of the venue's day. */
  opens: number;
  closes: number;
}

/**
 * The latest session as a line across its hours: a session still trading
 * reaches only as far as the time it has reached, and a lunch break takes no
 * width. A venue with no hours listed spans its first bar to its last. A
 * session with one price so far (its first five minutes) has no line yet, so
 * the one before it stands. Null with fewer than two prices to draw between.
 */
export function sessionLine(bars: readonly Bar[], hours: SessionHours | null): SessionLine | null {
  const pricedOf = (slice: SessionSlice<Bar> | null) => slice?.bars.filter((bar) => bar.close > 0) ?? [];
  let session = latestSession(bars, hours);
  if (session && pricedOf(session).length < 2) {
    const opened = session.date;
    session = latestSession(bars.filter((bar) => wallDate(bar.time) < opened), hours);
  }
  const priced = pricedOf(session);
  if (!session || priced.length < 2) return null;
  const minutes = priced.map((bar) => wallMinutes(bar.time));
  const spans: SessionHours = hours ?? [[minutes[0], minutes[minutes.length - 1]]];
  const total = spans.reduce((sum, [open, close]) => sum + (close - open), 0) || 1;
  const fraction = (minute: number) => {
    let elapsed = 0;
    for (const [open, close] of spans) {
      if (minute <= close) return Math.max(0, elapsed + Math.max(0, minute - open)) / total;
      elapsed += close - open;
    }
    return 1;
  };
  return {
    date: session.date,
    values: priced.map((bar) => bar.close),
    at: minutes.map((minute) => Math.min(1, fraction(minute))),
    previousClose: session.previousClose,
    opens: spans[0][0],
    closes: spans[spans.length - 1][1],
  };
}
