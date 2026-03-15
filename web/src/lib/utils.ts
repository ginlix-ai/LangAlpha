import { type ClassValue, clsx } from "clsx"
import { twMerge } from "tailwind-merge"

export function cn(...inputs: ClassValue[]) {
  return twMerge(clsx(inputs))
}

/**
 * Convert UTC Unix milliseconds to "ET-as-UTC" Unix seconds for lightweight-charts.
 *
 * lightweight-charts renders timestamps as UTC. To display Eastern Time
 * wall-clock values we extract the ET wall-clock components and build a
 * fake UTC timestamp from them. This works regardless of the browser's
 * local timezone.
 */
const _etParts = new Intl.DateTimeFormat('en-US', {
  timeZone: 'America/New_York',
  year: 'numeric', month: '2-digit', day: '2-digit',
  hour: '2-digit', minute: '2-digit', second: '2-digit',
  hour12: false,
});

/** Convert UTC Unix ms to ET date string (YYYY-MM-DD). */
export const utcMsToETDate = (ms: number): string =>
  new Date(ms).toLocaleDateString('en-CA', { timeZone: 'America/New_York' });

/** Convert UTC Unix ms to ET time string (HH:MM, 24h). */
export const utcMsToETTime = (ms: number): string =>
  new Date(ms).toLocaleTimeString('en-US', {
    timeZone: 'America/New_York', hour: '2-digit', minute: '2-digit', hour12: false,
  });

export function utcMsToChartSec(utcMs: number | null | undefined): number {
  if (utcMs == null || isNaN(utcMs)) return 0;
  const parts = _etParts.formatToParts(new Date(utcMs));
  const get = (type: Intl.DateTimeFormatPartTypes) => parseInt(parts.find((p) => p.type === type)!.value);
  return Date.UTC(get('year'), get('month') - 1, get('day'),
    get('hour'), get('minute'), get('second')) / 1000;
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


