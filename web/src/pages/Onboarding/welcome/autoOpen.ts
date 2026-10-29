/** How recent an account has to be for its first visit to open the welcome
 *  flow by itself. */
export const NEW_ACCOUNT_WINDOW_MS = 7 * 24 * 60 * 60 * 1000;

export interface AutoOpenInput {
  /** Where the visit landed. */
  pathname: string;
  /**
   * The onboarding engine had never run for this account when this session's
   * preferences loaded: no `firstRunAt` stamp yet. That stamp lands on the
   * first visit, so this is what makes it the first.
   */
  firstRun: boolean;
  onboardingCompleted: boolean;
  welcomeSkippedAt: number | null;
  /** The account's `created_at`, as the server sent it. */
  createdAt: unknown;
  now: number;
}

/**
 * Whether this visit opens the welcome flow full screen, unasked.
 *
 * Only a new account's first visit, and only when it lands on the dashboard,
 * which is where signing in and finishing setup both go: a deep link (a shared
 * thread, a sign-in coming back to Plugins) is somewhere the user meant to be.
 *
 * `firstRun` alone would also catch an old account that has not been back
 * since the engine started stamping it, which is exactly the user who should
 * get the dashboard banner rather than a takeover; the account's age rules
 * that one out. Without a readable `created_at` there is no telling, so it
 * stays closed and the banner still offers the flow.
 */
export function shouldAutoOpenWelcome(input: AutoOpenInput): boolean {
  if (input.pathname !== '/dashboard') return false;
  if (!input.firstRun || input.onboardingCompleted || input.welcomeSkippedAt !== null) return false;
  const created = typeof input.createdAt === 'string' ? Date.parse(input.createdAt) : Number.NaN;
  if (!Number.isFinite(created)) return false;
  return input.now - created < NEW_ACCOUNT_WINDOW_MS;
}
