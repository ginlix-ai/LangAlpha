import { describe, it, expect } from 'vitest';
import { migrateOnboardingPrefs, emptyOnboardingPrefs } from '../onboardingPrefsSchema';
import { ONBOARDING_PREFS_VERSION } from '../types';

describe('migrateOnboardingPrefs', () => {
  it('returns a complete empty object from undefined', () => {
    expect(migrateOnboardingPrefs(undefined)).toEqual(emptyOnboardingPrefs());
  });

  it('returns empty from a non-object blob', () => {
    expect(migrateOnboardingPrefs('garbage')).toEqual(emptyOnboardingPrefs());
    expect(migrateOnboardingPrefs(42)).toEqual(emptyOnboardingPrefs());
  });

  it('keeps valid values, recovers bad fields, drops unknown keys', () => {
    const result = migrateOnboardingPrefs({
      version: 99, // wrong literal → coerced to current
      pageIntrosSeen: { chat: 1700 },
      gettingStartedDoneAt: 'garbage', // wrong type → {}
      gettingStartedDismissedAt: 9,
      lastSeenReleaseVersion: 123, // wrong type → null
      firstRunAt: 42,
      foo: 'ignored',
    });

    expect(result.version).toBe(ONBOARDING_PREFS_VERSION);
    expect(result.pageIntrosSeen).toEqual({ chat: 1700 });
    expect(result.gettingStartedDoneAt).toEqual({});
    expect(result.gettingStartedDismissedAt).toBe(9);
    expect(result.lastSeenReleaseVersion).toBeNull();
    expect(result.firstRunAt).toBe(42);
    expect(result).not.toHaveProperty('foo');
  });

  it('coerces a negative dismissal timestamp to null', () => {
    const result = migrateOnboardingPrefs({ gettingStartedDismissedAt: -5 });
    expect(result.gettingStartedDismissedAt).toBeNull();
  });

  // One corrupt entry must not wipe the whole map — that would re-pop every
  // intro / un-check every task the user already cleared.
  it('a single corrupt epoch-map entry drops only that key', () => {
    const result = migrateOnboardingPrefs({
      pageIntrosSeen: { chat: 1700, thread: 'bad', dashboard: -1, market: 2.5 },
      gettingStartedDoneAt: { stocks: 100, firstChat: null },
    });
    expect(result.pageIntrosSeen).toEqual({ chat: 1700 });
    expect(result.gettingStartedDoneAt).toEqual({ stocks: 100 });
  });
});
