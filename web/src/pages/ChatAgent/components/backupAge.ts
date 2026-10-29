import { relativeTime } from '@/lib/format';

/**
 * How long ago a backup was taken, for "the backup taken {{time}}". Empty
 * under a minute, where it would read "taken now": the sentence then goes
 * without the time.
 */
export function backupAge(at: string | null | undefined, locale: string, now: number): string {
  const ms = at ? new Date(at).getTime() : Number.NaN;
  if (Number.isNaN(ms) || Math.abs(now - ms) < 60_000) return '';
  return relativeTime(ms, locale, now);
}
