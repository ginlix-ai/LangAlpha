import { z } from 'zod';
import { ONBOARDING_PREFS_VERSION, type OnboardingPrefs } from './types';

/**
 * Zod-at-the-boundary, mirroring the dashboard prefs precedent
 * (`widgets/framework/configSchemas.ts`): per-field `.catch()` recovers
 * individual bad values, and a non-object blob falls back to empty prefs. Never
 * throws.
 */

// Per-entry recovery: one corrupt value drops only its key. A whole-record
// `.catch({})` would discard the entire map on a single bad entry — re-popping
// every page intro / un-checking every task a user has already cleared.
const epochMap = z
  .record(z.string(), z.unknown())
  .catch({})
  .transform(
    (m) =>
      Object.fromEntries(
        Object.entries(m).filter(
          ([, v]) => typeof v === 'number' && Number.isInteger(v) && v >= 0
        )
      ) as Record<string, number>
  );

const OnboardingPrefsSchema = z.object({
  version: z.literal(ONBOARDING_PREFS_VERSION).catch(ONBOARDING_PREFS_VERSION),
  pageIntrosSeen: epochMap,
  gettingStartedDoneAt: epochMap,
  gettingStartedDismissedAt: z.number().int().nonnegative().nullable().catch(null),
  lastSeenReleaseVersion: z.string().min(1).nullable().catch(null),
  firstRunAt: z.number().int().nonnegative().nullable().catch(null),
});

export function emptyOnboardingPrefs(): OnboardingPrefs {
  return {
    version: ONBOARDING_PREFS_VERSION,
    pageIntrosSeen: {},
    gettingStartedDoneAt: {},
    gettingStartedDismissedAt: null,
    lastSeenReleaseVersion: null,
    firstRunAt: null,
  };
}

/**
 * Bring any stored onboarding prefs up to the current shape. Never throws;
 * returns a complete, valid object even from `undefined` or garbage.
 */
export function migrateOnboardingPrefs(raw: unknown): OnboardingPrefs {
  const parsed = OnboardingPrefsSchema.safeParse(raw ?? {});
  return parsed.success ? (parsed.data as OnboardingPrefs) : emptyOnboardingPrefs();
}
