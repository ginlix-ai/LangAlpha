import { isWelcomeStep, type WelcomeStepId } from './paths';
import {
  HOLDINGS_METHODS,
  isOneOf,
  profileRow,
  readProfile,
  type HoldingsMethod,
  type InvestingProfile,
} from './profile';
import type { Screenshot } from './screenshots';

/**
 * The answers given so far and the screen the user was on, kept in this
 * browser until the flow finishes. Connecting a brokerage leaves the app for
 * the vendor and comes back as a fresh page load, and a user who leaves halfway
 * resumes from the dashboard banner; neither should have to answer twice.
 *
 * A convenience, not a record: per user and per browser, and anything the
 * storage refuses (a private window, blocked site data) only means starting
 * over. The profile is kept in the shape the server stores, so one reader
 * checks both. The saved profile it was seeded from is kept beside it, so a
 * resume can tell an answer from a value the user was only shown.
 *
 * The brokerage step's second half is kept too: whether the user went
 * elsewhere, the name of a broker the list does not have, and how the holdings
 * are to reach the agent. None of it is in the profile, and a reload that
 * dropped it would send the opening message without it. A desktop sign-in that
 * runs out reloads the window wherever the user has got to by then, so this is
 * not only a return to the brokerage step. Screenshots are not kept: they are
 * too large for this storage, and they ride only the opening message.
 */

const PREFIX = 'langalpha-welcome-draft:';
/** A draft older than this is a different visit; start that one fresh. */
const MAX_AGE_MS = 30 * 24 * 60 * 60 * 1000;

/** The brokerage step's second half, for a user who connects none. */
export interface ElsewhereAnswers {
  /** Went past the brokers that can be connected. */
  went: boolean;
  /** The broker named under "other". */
  otherName: string;
  /** How the holdings reach the agent instead. */
  method: HoldingsMethod | null;
  /** For the visit only: not kept here (see above). */
  shots: Screenshot[];
}

export interface WelcomeDraft {
  step: WelcomeStepId;
  profile: InvestingProfile;
  /** The saved profile the answers were given on top of. */
  base: InvestingProfile;
  elsewhere: Omit<ElsewhereAnswers, 'shots'>;
}

/** Longer than any broker's name; anything past it is not one. */
const MAX_OTHER_NAME = 200;

/**
 * The brokerage step's answers out of a stored draft, each one only when it
 * reads as itself: storage is the user's to edit, and a value of the wrong
 * shape reads as not given rather than being handed to the flow.
 */
function readElsewhere(parsed: Record<string, unknown>): WelcomeDraft['elsewhere'] {
  return {
    went: parsed.elsewhere === true,
    otherName: typeof parsed.otherName === 'string' ? parsed.otherName.slice(0, MAX_OTHER_NAME) : '',
    method: isOneOf(HOLDINGS_METHODS, parsed.method) ? parsed.method : null,
  };
}

export function readWelcomeDraft(userId: string | null | undefined): WelcomeDraft | null {
  if (!userId) return null;
  try {
    const raw = localStorage.getItem(PREFIX + userId);
    if (!raw) return null;
    const parsed = JSON.parse(raw) as {
      step?: unknown;
      savedAt?: unknown;
      profile?: unknown;
      base?: unknown;
      elsewhere?: unknown;
      otherName?: unknown;
      method?: unknown;
    } | null;
    const step = parsed?.step;
    if (!parsed || !isWelcomeStep(step)) return null;
    if (typeof parsed.savedAt !== 'number' || Date.now() - parsed.savedAt > MAX_AGE_MS) return null;
    return {
      step,
      profile: readProfile(parsed.profile),
      base: readProfile(parsed.base),
      elsewhere: readElsewhere(parsed),
    };
  } catch {
    return null;
  }
}

export function writeWelcomeDraft(userId: string | null | undefined, draft: WelcomeDraft): void {
  if (!userId) return;
  try {
    localStorage.setItem(
      PREFIX + userId,
      JSON.stringify({
        step: draft.step,
        savedAt: Date.now(),
        profile: profileRow(draft.profile),
        base: profileRow(draft.base),
        elsewhere: draft.elsewhere.went,
        otherName: draft.elsewhere.otherName,
        method: draft.elsewhere.method,
      }),
    );
  } catch {
    // Nowhere to keep it; the flow still works, it just will not resume.
  }
}

export function clearWelcomeDraft(userId: string | null | undefined): void {
  if (!userId) return;
  try {
    localStorage.removeItem(PREFIX + userId);
  } catch {
    // Nothing was kept to clear.
  }
}
