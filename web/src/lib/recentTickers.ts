import * as z from 'zod/mini';
import { userLocalStorage } from '@/lib/userStorage';

/**
 * The tickers this user opened a chart on, newest first, for the command
 * palette's recents. Kept in the browser: it is a convenience, and the market
 * page already keeps its last symbol there.
 */
export interface RecentTicker {
  symbol: string;
  /** When the chart was last opened on it (Unix ms). */
  at: number;
}

const KEY = 'palette:recent-tickers';
const KEEP = 8;

// Anything on the origin can write the slot, so each row is checked on the
// way in: a hand edit or a corrupt write loses that row, not the list.
const rowSchema = z.object({
  symbol: z.string().check(z.trim(), z.minLength(1), z.maxLength(32)),
  at: z.number().check(z.gte(0)),
});

export function readRecentTickers(): RecentTicker[] {
  let stored: unknown;
  try {
    stored = JSON.parse(userLocalStorage.getItem(KEY) ?? '[]');
  } catch {
    return [];
  }
  if (!Array.isArray(stored)) return [];
  return stored.flatMap((row) => {
    const parsed = rowSchema.safeParse(row);
    return parsed.success ? [parsed.data] : [];
  });
}

export function recordRecentTicker(symbol: string, now = Date.now()): void {
  if (!symbol) return;
  const rest = readRecentTickers().filter((row) => row.symbol !== symbol);
  userLocalStorage.setItem(KEY, JSON.stringify([{ symbol, at: now }, ...rest].slice(0, KEEP)));
}
