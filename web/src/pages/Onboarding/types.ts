/** Persisted onboarding state. Lives in `user_preferences.other_preference.onboarding`. */

export const ONBOARDING_PREFS_VERSION = 1 as const;

export interface OnboardingPrefs {
  version: typeof ONBOARDING_PREFS_VERSION;
  /** Per-page intro popups already seen: intro id → epoch ms. */
  pageIntrosSeen: Record<string, number>;
  /** Getting-started checklist tasks completed: task id → epoch ms. */
  gettingStartedDoneAt: Record<string, number>;
  /** When the user hid the getting-started card; null = still visible. */
  gettingStartedDismissedAt: number | null;
  /** Highest release version acknowledged in the What's-New modal. */
  lastSeenReleaseVersion: string | null;
  /** First time the engine ran for this user; only its presence is used, to gate the one-time caught-up stamp. */
  firstRunAt: number | null;
}
