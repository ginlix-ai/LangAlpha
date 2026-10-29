import { userLocalStorage } from '@/lib/userStorage';

// --- localStorage persistence helpers (shared across MarketView components) ---
const STORAGE_PREFIX = 'market-chart:';

export function loadPref<T>(key: string, fallback: T): T {
  const raw = userLocalStorage.getItem(STORAGE_PREFIX + key);
  try {
    return raw !== null ? (JSON.parse(raw) as T) : fallback;
  } catch { return fallback; }
}

export function savePref(key: string, value: unknown): void {
  userLocalStorage.setItem(STORAGE_PREFIX + key, JSON.stringify(value));
}

