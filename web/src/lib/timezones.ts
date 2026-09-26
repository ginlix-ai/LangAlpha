import { zoneClock } from './deviceTimezone';

/** How many minutes a clock in `tz` runs ahead of UTC at `at`: its wall clock
 *  read as though it were UTC, less the instant. 0 for a zone this browser
 *  does not know. */
export function utcOffsetMinutes(tz: string, at: Date = new Date()): number {
  try {
    const f: Partial<Record<Intl.DateTimeFormatPartTypes, number>> = {};
    for (const p of zoneClock(tz).formatToParts(at)) f[p.type] = Number(p.value);
    // Some engines write midnight as 24 even on a 23-hour clock.
    const wall = Date.UTC(f.year!, f.month! - 1, f.day, f.hour! % 24, f.minute, f.second);
    return Math.round((wall - at.getTime()) / 60_000) || 0;
  } catch {
    return 0;
  }
}

/** "UTC−4", "UTC+5:30", "UTC": a zone's distance from UTC, with a true minus
 *  sign so a column of offsets lines up. */
export function formatUtcOffset(minutes: number): string {
  if (minutes === 0) return 'UTC';
  const abs = Math.abs(minutes);
  const rest = abs % 60;
  return `UTC${minutes < 0 ? '−' : '+'}${Math.floor(abs / 60)}${rest ? `:${String(rest).padStart(2, '0')}` : ''}`;
}
