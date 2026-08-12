import { safeLocalStorage } from '@/lib/utils';
import { ONBOARDING_PREFS_VERSION, type OnboardingPrefs } from './types';

/**
 * Non-authoritative localStorage projection of the server prefs, read
 * synchronously before the prefs GET resolves. Invariant: it can only
 * SUPPRESS a page intro (already seen), never trigger one — so a stale
 * mirror can at worst delay a popup, never spuriously launch it.
 *
 * Keyed per user: on a shared browser, user B must not inherit user A's
 * seen-state (logout clears the query cache but not localStorage). A falsy
 * userId is a no-op/null read — suppress-only means that's always safe.
 */

// Versioned with the prefs shape: bumping ONBOARDING_PREFS_VERSION changes the
// key, so a stale mirror from an older shape is silently ignored (read as null)
// rather than mis-suppressing — safe because the mirror is suppress-only.
const MIRROR_KEY_PREFIX = `langalpha-onboarding-v${ONBOARDING_PREFS_VERSION}`;

function mirrorKey(userId: string): string {
  return `${MIRROR_KEY_PREFIX}:${userId}`;
}

export interface OnboardingMirror {
  pageIntrosSeen: Record<string, number>;
  // Only pageIntrosSeen is read today; the two below are mirrored for a future
  // anti-flash gate on What's-New. Don't treat them as load-bearing.
  lastSeenReleaseVersion: string | null;
  firstRunAt: number | null;
}

function coerceEpochMap(value: unknown): Record<string, number> {
  if (!value || typeof value !== 'object') return {};
  const out: Record<string, number> = {};
  for (const [k, v] of Object.entries(value as Record<string, unknown>)) {
    if (typeof v === 'number') out[k] = v;
  }
  return out;
}

export function readMirror(userId: string | null | undefined): OnboardingMirror | null {
  if (!userId) return null;
  const raw = safeLocalStorage.getItem(mirrorKey(userId));
  if (!raw) return null;
  try {
    const parsed = JSON.parse(raw) as Partial<OnboardingMirror> | null;
    if (!parsed || typeof parsed !== 'object') return null;
    return {
      pageIntrosSeen: coerceEpochMap(parsed.pageIntrosSeen),
      lastSeenReleaseVersion:
        typeof parsed.lastSeenReleaseVersion === 'string' ? parsed.lastSeenReleaseVersion : null,
      firstRunAt: typeof parsed.firstRunAt === 'number' ? parsed.firstRunAt : null,
    };
  } catch {
    return null;
  }
}

export function writeMirror(userId: string | null | undefined, prefs: OnboardingPrefs): void {
  if (!userId) return;
  const mirror: OnboardingMirror = {
    pageIntrosSeen: prefs.pageIntrosSeen,
    lastSeenReleaseVersion: prefs.lastSeenReleaseVersion,
    firstRunAt: prefs.firstRunAt,
  };
  safeLocalStorage.setItem(mirrorKey(userId), JSON.stringify(mirror));
}

export function clearMirror(userId: string | null | undefined): void {
  if (!userId) return;
  safeLocalStorage.removeItem(mirrorKey(userId));
}
