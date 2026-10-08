import { userLocalStorage } from '@/lib/userStorage';
import { displaySpelling } from '@/lib/bars/exchanges';

const PREFIX = 'marketview_thread_id_';

function keyFor(workspaceId: string, symbol: string): string {
  return `${PREFIX}${workspaceId}_${displaySpelling(symbol)}`;
}

/** Pointers saved before keys took the display spelling sit under the symbol
 *  as it arrived: Shanghai as `.SS`, a Hong Kong code bare or at HKEX's five
 *  digits (`700.HK`, `00700.HK`). A candidate counts only when it folds to
 *  this key's spelling; read once, it moves to the current key. */
function legacyKeysFor(workspaceId: string, symbol: string): string[] {
  const sym = displaySpelling(symbol);
  const dot = sym.lastIndexOf('.');
  if (dot === -1) return [];
  const code = sym.slice(0, dot);
  const venue = sym.slice(dot);
  const bare = code.replace(/^0+/, '') || '0';
  return [`${code}.SS`, `${bare}${venue}`, `${bare.padStart(5, '0')}${venue}`]
    .filter((s) => s !== sym && displaySpelling(s) === sym)
    .map((s) => `${PREFIX}${workspaceId}_${s}`);
}

function adoptLegacy(workspaceId: string, symbol: string): string | null {
  const legacy = legacyKeysFor(workspaceId, symbol);
  const raw = legacy.map((key) => userLocalStorage.getItem(key)).find((v) => v) ?? null;
  if (!raw) return null;
  legacy.forEach((key) => userLocalStorage.removeItem(key));
  userLocalStorage.setItem(keyFor(workspaceId, symbol), raw);
  return raw;
}

export function getMarketThreadId(
  workspaceId: string | null | undefined,
  symbol: string,
): string | null {
  if (!workspaceId || !symbol) return null;
  const raw = userLocalStorage.getItem(keyFor(workspaceId, symbol)) ?? adoptLegacy(workspaceId, symbol);
  if (!raw) return null;
  if (raw === '__default__') {
    userLocalStorage.removeItem(keyFor(workspaceId, symbol));
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
    userLocalStorage.removeItem(keyFor(workspaceId, symbol));
    return;
  }
  userLocalStorage.setItem(keyFor(workspaceId, symbol), threadId);
}

export function clearMarketThreadId(
  workspaceId: string | null | undefined,
  symbol: string,
): void {
  if (!workspaceId || !symbol) return;
  userLocalStorage.removeItem(keyFor(workspaceId, symbol));
  legacyKeysFor(workspaceId, symbol).forEach((key) => userLocalStorage.removeItem(key));
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
  userLocalStorage.removeByPrefix(`${PREFIX}${workspaceId}_`);
}
