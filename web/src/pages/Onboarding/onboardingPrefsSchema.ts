import * as z from 'zod/mini';
import { ONBOARDING_PREFS_VERSION, type OnboardingPrefs } from './types';

/**
 * Zod-at-the-boundary, mirroring the dashboard prefs precedent
 * (`widgets/framework/configSchemas.ts`): per-field `catch` recovers
 * individual bad values, and a non-object blob falls back to empty prefs. Never
 * throws. `zod/mini` rather than classic zod because this module is on the
 * entry's critical path, and only mini tree-shakes down to what it uses.
 */

// Per-entry recovery: one corrupt value drops only its key. A whole-record
// `catch({})` would discard the entire map on a single bad entry — re-popping
// every page intro / un-checking every task a user has already cleared.
const epochMap = z.pipe(
  z.catch(z.record(z.string(), z.unknown()), {}),
  z.transform(
    (m) =>
      Object.fromEntries(
        Object.entries(m).filter(
          ([, v]) => typeof v === 'number' && Number.isInteger(v) && v >= 0
        )
      ) as Record<string, number>
  )
);

const epochOrNull = z.catch(z.nullable(z.int().check(z.nonnegative())), null);

const OnboardingPrefsSchema = z.object({
  version: z.catch(z.literal(ONBOARDING_PREFS_VERSION), ONBOARDING_PREFS_VERSION),
  pageIntrosSeen: epochMap,
  gettingStartedDoneAt: epochMap,
  gettingStartedDismissedAt: epochOrNull,
  lastSeenReleaseVersion: z.catch(z.nullable(z.string().check(z.minLength(1))), null),
  firstRunAt: epochOrNull,
  welcomeSkippedAt: epochOrNull,
});

export function emptyOnboardingPrefs(): OnboardingPrefs {
  return {
    version: ONBOARDING_PREFS_VERSION,
    pageIntrosSeen: {},
    gettingStartedDoneAt: {},
    gettingStartedDismissedAt: null,
    lastSeenReleaseVersion: null,
    firstRunAt: null,
    welcomeSkippedAt: null,
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
