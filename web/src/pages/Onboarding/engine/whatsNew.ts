import type { AnnouncementDef } from '../registry/types';
import type { OnboardingPrefs } from '../types';

/** Parse CalVer 'YYYY.MM.DD[.N]' into a numeric tuple for total ordering. */
function parseVersion(v: string): number[] {
  // Only pure-digit segments count — a malformed part like '04beta' is treated
  // as 0, not silently parsed to 4 (parseInt would stop at the first letter).
  return v.split('.').map((part) => (/^\d+$/.test(part) ? parseInt(part, 10) : 0));
}

/** Total order over calendar-ish release strings. <0, 0, >0 like a comparator. */
export function compareReleaseVersions(a: string, b: string): number {
  const pa = parseVersion(a);
  const pb = parseVersion(b);
  const len = Math.max(pa.length, pb.length);
  for (let i = 0; i < len; i++) {
    const diff = (pa[i] ?? 0) - (pb[i] ?? 0);
    if (diff !== 0) return diff < 0 ? -1 : 1;
  }
  return 0;
}

/** Highest release version across all announcements, or null if none. */
export function latestReleaseVersion(announcements: AnnouncementDef[]): string | null {
  let max: string | null = null;
  for (const a of announcements) {
    if (max === null || compareReleaseVersions(a.releaseVersion, max) > 0) max = a.releaseVersion;
  }
  return max;
}

/**
 * Announcements newer than the user's acknowledged version, sorted newest-first.
 * A user with `lastSeenReleaseVersion === null` (pre-stamp window) gets nothing —
 * `ensureFirstRun` stamps new users caught-up so they see the page intros, not
 * a backlog. Acknowledging the modal bumps `lastSeenReleaseVersion`, which is the
 * only dismissal mechanism (no per-announcement state needed).
 */
export function unseenReleases(
  prefs: OnboardingPrefs,
  announcements: AnnouncementDef[]
): AnnouncementDef[] {
  const since = prefs.lastSeenReleaseVersion;
  return announcements
    .filter((a) => (since === null ? false : compareReleaseVersions(a.releaseVersion, since) > 0))
    .sort((x, y) => compareReleaseVersions(y.releaseVersion, x.releaseVersion));
}
