import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * Convert UTC Unix milliseconds to "market-local-as-UTC" Unix seconds for
 * lightweight-charts.
 *
 * lightweight-charts renders timestamps as UTC. To display market-local
 * wall-clock values we extract the venue-timezone wall-clock components and
 * build a fake UTC timestamp from them. This works regardless of the
 * browser's local timezone. Defaults to ET (US venues); pass the symbol's
 * venue tz (`timezoneForSymbol`) for foreign listings.
 */
const _wallClockFmts = new Map<string, Intl.DateTimeFormat>();

function wallClockFmt(tz: string): Intl.DateTimeFormat {
  let fmt = _wallClockFmts.get(tz);
  if (!fmt) {
    fmt = new Intl.DateTimeFormat('en-US', {
      timeZone: tz,
      year: 'numeric', month: '2-digit', day: '2-digit',
      hour: '2-digit', minute: '2-digit', second: '2-digit',
      hour12: false,
    });
    _wallClockFmts.set(tz, fmt);
  }
  return fmt;
}

/** Convert UTC Unix ms (or a Date) to a YYYY-MM-DD date string in `tz`. */
export const dateStrInTz = (d: number | Date, tz: string): string =>
  new Date(d).toLocaleDateString('en-CA', { timeZone: tz });

/** YYYY-MM-DD on the reader's own calendar. `toISOString()` gives the UTC date,
 *  which is already tomorrow (or still yesterday) for part of every day outside
 *  UTC, so "today" comparisons built on it put today's rows on the wrong side. */
export const localDateStr = (d: number | Date): string => new Date(d).toLocaleDateString('en-CA');

/** Convert UTC Unix ms to ET date string (YYYY-MM-DD). */
export const utcMsToETDate = (ms: number): string => dateStrInTz(ms, 'America/New_York');

/** Convert UTC Unix ms to ET time string (HH:MM, 24h). */
export const utcMsToETTime = (ms: number): string =>
  new Date(ms).toLocaleTimeString('en-US', {
    timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', hour12: false,
  });

export function utcMsToChartSec(utcMs: number | null | undefined, tz: string = 'America/New_York'): number {
  if (utcMs == null || isNaN(utcMs)) return 0;
  const parts = wallClockFmt(tz).formatToParts(new Date(utcMs));
  const get = (type: Intl.DateTimeFormatPartTypes) => parseInt(parts.find((p) => p.type === type)!.value);
  return Date.UTC(get('year'), get('month') - 1, get('day'),
    get('hour'), get('minute'), get('second')) / 1000;
}

/**
 * Decode a chart timestamp (market-local wall clock encoded as fake UTC, per
 * {@link utcMsToChartSec}) back to its venue-local YYYY-MM-DD date string.
 * Reading the fake time in UTC IS the decode — no timezone math belongs here;
 * formatting a chart time in any real timezone double-shifts it.
 */
export function chartSecToDateStr(sec: number): string {
  return new Date(sec * 1000).toISOString().slice(0, 10);
}

export const safeLocalStorage = {
  getItem: (key: string): string | null => {
    try {
      return localStorage.getItem(key);
    } catch (e) {
      if (import.meta.env.DEV) console.warn('safeLocalStorage.getItem failed:', e);
      return null;
    }
  },
  setItem: (key: string, value: string): void => {
    try {
      localStorage.setItem(key, value);
    } catch (e) {
      if (import.meta.env.DEV) console.warn('safeLocalStorage.setItem failed:', e);
    }
  },
  removeItem: (key: string): void => {
    try {
      localStorage.removeItem(key);
    } catch (e) {
      if (import.meta.env.DEV) console.warn('safeLocalStorage.removeItem failed:', e);
    }
  },
};


