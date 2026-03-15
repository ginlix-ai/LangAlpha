import { safeLocalStorage } from '@/lib/utils';

// --- localStorage persistence helpers (shared across MarketView components) ---
const STORAGE_PREFIX = 'market-chart:';

export function loadPref<T>(key: string, fallback: T): T {
  const raw = safeLocalStorage.getItem(STORAGE_PREFIX + key);
  try {
    return raw !== null ? (JSON.parse(raw) as T) : fallback;
  } catch { return fallback; }
}

export function savePref(key: string, value: unknown): void {
  safeLocalStorage.setItem(STORAGE_PREFIX + key, JSON.stringify(value));
}

