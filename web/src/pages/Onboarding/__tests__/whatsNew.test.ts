import { describe, it, expect } from 'vitest';
import { compareReleaseVersions, latestReleaseVersion, unseenReleases } from '../engine/whatsNew';
import { emptyOnboardingPrefs } from '../onboardingPrefsSchema';
import { ANNOUNCEMENTS } from '../registry/announcements';
import type { AnnouncementDef } from '../registry/types';
import type { OnboardingPrefs } from '../types';

function ann(key: string, releaseVersion: string): AnnouncementDef {
  return { key, releaseVersion, modalTitleKey: 't', modalBodyKey: 'b' };
}

describe('compareReleaseVersions', () => {
  it('orders by major then minor then patch', () => {
    expect(compareReleaseVersions('2026.4', '2026.5')).toBeLessThan(0);
    expect(compareReleaseVersions('2026.5', '2026.4')).toBeGreaterThan(0);
    expect(compareReleaseVersions('2026.5', '2026.5')).toBe(0);
    expect(compareReleaseVersions('2026.10', '2026.9')).toBeGreaterThan(0); // numeric, not lexical
    expect(compareReleaseVersions('2026.5.1', '2026.5')).toBeGreaterThan(0);
  });

  it('absorbs malformed segments (NaN → 0) and pads missing segments', () => {
    expect(compareReleaseVersions('2026.x', '2026.0')).toBe(0); // non-numeric → 0
    expect(compareReleaseVersions('2026', '2026.0')).toBe(0); // missing segment padded
    expect(compareReleaseVersions('2026.x', '2026.1')).toBeLessThan(0);
  });

  it('treats zero-padded and bare CalVer segments as equal', () => {
    expect(compareReleaseVersions('2026.04.26', '2026.4.26')).toBe(0);
    expect(compareReleaseVersions('2026.04.26.1', '2026.04.26')).toBeGreaterThan(0);
  });
});

describe('ANNOUNCEMENTS registry convention', () => {
  it('every releaseVersion is CalVer YYYY.MM.DD[.N] (release tag minus the v)', () => {
    for (const a of ANNOUNCEMENTS) {
      expect(a.releaseVersion).toMatch(/^\d{4}\.\d{2}\.\d{2}(\.\d+)?$/);
    }
  });

  it('keys are unique', () => {
    const keys = ANNOUNCEMENTS.map((a) => a.key);
    expect(new Set(keys).size).toBe(keys.length);
  });
});

describe('latestReleaseVersion', () => {
  it('returns the highest version', () => {
    expect(latestReleaseVersion([ann('a', '2026.4'), ann('b', '2026.6'), ann('c', '2026.5')])).toBe('2026.6');
    expect(latestReleaseVersion([])).toBeNull();
  });
});

describe('unseenReleases', () => {
  const list = [ann('older', '2026.4'), ann('newer', '2026.6')];

  it('returns nothing for a caught-up / brand-new user (lastSeen null)', () => {
    const prefs: OnboardingPrefs = emptyOnboardingPrefs();
    expect(unseenReleases(prefs, list)).toEqual([]);
  });

  it('returns only releases newer than lastSeen, newest first', () => {
    const prefs: OnboardingPrefs = { ...emptyOnboardingPrefs(), lastSeenReleaseVersion: '2026.4' };
    const result = unseenReleases(prefs, list);
    expect(result.map((a) => a.key)).toEqual(['newer']);
  });

  it('returns all releases newer than an old lastSeen, newest first', () => {
    const prefs: OnboardingPrefs = { ...emptyOnboardingPrefs(), lastSeenReleaseVersion: '2026.0' };
    expect(unseenReleases(prefs, list).map((a) => a.key)).toEqual(['newer', 'older']);
  });

  it('returns nothing when lastSeen equals the latest', () => {
    const prefs: OnboardingPrefs = { ...emptyOnboardingPrefs(), lastSeenReleaseVersion: '2026.6' };
    expect(unseenReleases(prefs, list)).toEqual([]);
  });

  it('handles an empty announcement registry', () => {
    const prefs: OnboardingPrefs = { ...emptyOnboardingPrefs(), lastSeenReleaseVersion: '2026.0' };
    expect(unseenReleases(prefs, [])).toEqual([]);
  });
});
