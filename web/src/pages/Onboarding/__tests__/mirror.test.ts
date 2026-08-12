import { describe, it, expect, beforeEach } from 'vitest';
import { readMirror, writeMirror, clearMirror } from '../mirror';
import { emptyOnboardingPrefs } from '../onboardingPrefsSchema';
import type { OnboardingPrefs } from '../types';

const USER = 'u1';
const KEY = `langalpha-onboarding-v1:${USER}`;

describe('onboarding mirror', () => {
  beforeEach(() => localStorage.clear());

  it('returns null when absent', () => {
    expect(readMirror(USER)).toBeNull();
  });

  it('round-trips pageIntrosSeen, lastSeen, and firstRunAt', () => {
    const prefs: OnboardingPrefs = {
      ...emptyOnboardingPrefs(),
      pageIntrosSeen: { chat: 555, dashboard: 600 },
      lastSeenReleaseVersion: '2026.5',
      firstRunAt: 123,
    };
    writeMirror(USER, prefs);
    expect(readMirror(USER)).toEqual({
      pageIntrosSeen: { chat: 555, dashboard: 600 },
      lastSeenReleaseVersion: '2026.5',
      firstRunAt: 123,
    });
  });

  it('is scoped per user — another user reads nothing, and a falsy id is a safe no-op', () => {
    writeMirror(USER, { ...emptyOnboardingPrefs(), pageIntrosSeen: { chat: 5 } });
    expect(readMirror('someone-else')).toBeNull();
    expect(readMirror(null)).toBeNull();
    writeMirror(null, emptyOnboardingPrefs()); // must not throw or write anything
    expect(localStorage.length).toBe(1);
    clearMirror(null); // must not clear another user's entry
    expect(readMirror(USER)?.pageIntrosSeen).toEqual({ chat: 5 });
  });

  it('clears', () => {
    writeMirror(USER, emptyOnboardingPrefs());
    clearMirror(USER);
    expect(readMirror(USER)).toBeNull();
  });

  it('returns null on malformed JSON instead of throwing', () => {
    localStorage.setItem(KEY, '{not json');
    expect(readMirror(USER)).toBeNull();
  });

  it('returns null for non-object JSON values', () => {
    localStorage.setItem(KEY, '"a string"');
    expect(readMirror(USER)).toBeNull();
    localStorage.setItem(KEY, '42');
    expect(readMirror(USER)).toBeNull();
    localStorage.setItem(KEY, 'null');
    expect(readMirror(USER)).toBeNull();
  });

  it('coerces wrong-typed fields instead of trusting them', () => {
    localStorage.setItem(
      KEY,
      JSON.stringify({
        pageIntrosSeen: { chat: '5', dashboard: 7 }, // string entry dropped
        lastSeenReleaseVersion: 7,
        firstRunAt: true,
      })
    );
    expect(readMirror(USER)).toEqual({
      pageIntrosSeen: { dashboard: 7 },
      lastSeenReleaseVersion: null,
      firstRunAt: null,
    });
  });

  it('treats a pre-rewrite mirror (old shape) as nothing seen', () => {
    localStorage.setItem(
      KEY,
      JSON.stringify({ welcomeSeenAt: 5, lastSeenReleaseVersion: '2026.4', firstRunAt: 1 })
    );
    expect(readMirror(USER)).toEqual({
      pageIntrosSeen: {},
      lastSeenReleaseVersion: '2026.4',
      firstRunAt: 1,
    });
  });
});
