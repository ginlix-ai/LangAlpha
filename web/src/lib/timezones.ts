import { offsetFormat } from './deviceTimezone';

/** How many minutes a clock in `tz` runs ahead of UTC at `at`. Callers on a
 *  clock tick call this every second, so the formatter is cached per zone. */
export function utcOffsetMinutes(tz: string, at: Date = new Date()): number {
  try {
    // Bare "GMT" is UTC itself; some ICU builds spell it "GMT+00:00".
    const name = offsetFormat(tz).formatToParts(at).find((p) => p.type === 'timeZoneName')?.value ?? '';
    const m = /([+\-−])(\d{2}):(\d{2})/.exec(name);
    return m ? (m[1] === '+' ? 1 : -1) * (Number(m[2]) * 60 + Number(m[3])) : 0;
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
