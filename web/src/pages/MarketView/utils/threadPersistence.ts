import { safeLocalStorage } from '@/lib/utils';

const PREFIX = 'marketview_thread_id_';

function normalizeSymbol(symbol: string): string {
  return symbol.trim().toUpperCase();
}

function keyFor(workspaceId: string, symbol: string): string {
  return `${PREFIX}${workspaceId}_${normalizeSymbol(symbol)}`;
}

export function getMarketThreadId(
  workspaceId: string | null | undefined,
  symbol: string,
): string | null {
  if (!workspaceId || !symbol) return null;
  const raw = safeLocalStorage.getItem(keyFor(workspaceId, symbol));
  if (!raw) return null;
  if (raw === '__default__') {
    safeLocalStorage.removeItem(keyFor(workspaceId, symbol));
    return null;
  }
  return raw;
}

export function setMarketThreadId(
  workspaceId: string | null | undefined,
  symbol: string,
  threadId: string | null | undefined,
): void {
  if (!workspaceId || !symbol) return;
  if (!threadId || threadId === '__default__') {
    safeLocalStorage.removeItem(keyFor(workspaceId, symbol));
    return;
  }
  safeLocalStorage.setItem(keyFor(workspaceId, symbol), threadId);
}

export function clearMarketThreadId(
  workspaceId: string | null | undefined,
  symbol: string,
): void {
  if (!workspaceId || !symbol) return;
  safeLocalStorage.removeItem(keyFor(workspaceId, symbol));
}

/**
 * Removes every marketview_thread_id entry for the given workspace, across all
 * symbols. Mirrors ChatAgent's `removeStoredThreadId(workspaceId)` cleanup —
 * call when a workspace is deleted so its symbol-specific pointers don't
 * outlive it.
 */
export function clearAllMarketThreadsForWorkspace(
  workspaceId: string | null | undefined,
): void {
  if (!workspaceId) return;
  const prefix = `${PREFIX}${workspaceId}_`;
  // safeLocalStorage doesn't expose iteration; access the underlying store
  // defensively. In SSR/no-localStorage environments this is a no-op.
  try {
    const keys: string[] = [];
    for (let i = 0; i < localStorage.length; i += 1) {
      const k = localStorage.key(i);
      if (k && k.startsWith(prefix)) keys.push(k);
    }
    keys.forEach((k) => safeLocalStorage.removeItem(k));
  } catch {
    // no-op
  }
}
