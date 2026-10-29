import { describe, it, expect, beforeEach } from 'vitest';
import { clearWelcomeDraft, readWelcomeDraft, writeWelcomeDraft } from '../welcome/draft';
import { emptyProfile } from '../welcome/profile';

const answered = { ...emptyProfile(), riskLevel: 'moderate' as const, markets: ['us' as const] };
const base = emptyProfile();
/** The brokerage step's second half, not gone to. */
const stayed = { went: false, otherName: '', method: null };

describe('welcome draft', () => {
  beforeEach(() => localStorage.clear());

  it('keeps the screen, the answers and what they were given on top of, for the user who gave them', () => {
    // Every screen's answers, the optional lists included, so a sign-in round
    // trip from the brokerage screen loses none of them.
    const own = {
      ...answered,
      investorType: 'student' as const,
      experience: 'under_1y' as const,
      helpWith: ['learning' as const],
      horizon: 'years' as const,
      instruments: ['stock' as const, 'crypto' as const],
      approaches: ['macro' as const],
      styles: ['index' as const],
      sectors: ['biotech'],
      depth: 'thorough' as const,
      deliverable: 'markdown' as const,
      proactivity: 'proactive' as const,
    };
    const seeded = { ...emptyProfile(), sectors: ['biotech'] };
    writeWelcomeDraft('u1', { step: 'markets', profile: own, base: seeded, elsewhere: stayed });
    expect(readWelcomeDraft('u1')).toEqual({ step: 'markets', profile: own, base: seeded, elsewhere: stayed });
    expect(readWelcomeDraft('u2')).toBeNull();
  });

  it("keeps how the agent should work, the answer at the head of the row's output format ranking", () => {
    const stored = () => JSON.parse(localStorage.getItem('langalpha-welcome-draft:u1') ?? 'null');
    const file = {
      ...answered,
      depth: 'light' as const,
      deliverable: 'markdown' as const,
      proactivity: 'on_request' as const,
    };
    writeWelcomeDraft('u1', { step: 'agent', profile: file, base, elsewhere: stayed });
    // In the shape the server stores.
    expect(stored().profile.agent_preference).toEqual({
      research_depth: 'light',
      proactivity: 'on_request',
      output_format: ['markdown', 'text', 'visual'],
    });
    expect(readWelcomeDraft('u1')).toEqual({ step: 'agent', profile: file, base, elsewhere: stayed });

    const visual = { ...file, deliverable: 'visual' as const };
    writeWelcomeDraft('u1', { step: 'agent', profile: visual, base, elsewhere: stayed });
    expect(stored().profile.agent_preference).toMatchObject({ output_format: ['visual', 'markdown', 'text'] });
    expect(readWelcomeDraft('u1')).toEqual({ step: 'agent', profile: visual, base, elsewhere: stayed });
  });

  it('keeps how the user said their holdings reach the agent, which the profile has no place for', () => {
    // A reload that dropped these sent the opening message without the broker
    // named under "other" or the way picked instead of connecting.
    const elsewhere = { went: true, otherName: 'Northwind Securities', method: 'search' as const };
    writeWelcomeDraft('u1', { step: 'risk', profile: answered, base, elsewhere });
    expect(readWelcomeDraft('u1')).toEqual({ step: 'risk', profile: answered, base, elsewhere });
    // Beside the profile, as the keys they always were.
    expect(JSON.parse(localStorage.getItem('langalpha-welcome-draft:u1') ?? 'null')).toMatchObject({
      elsewhere: true,
      otherName: 'Northwind Securities',
      method: 'search',
    });
  });

  it('reads a brokerage answer of the wrong shape as not given, and keeps the rest of the draft', () => {
    const key = 'langalpha-welcome-draft:u1';
    localStorage.setItem(
      key,
      JSON.stringify({ step: 'risk', savedAt: Date.now(), elsewhere: 'yes', otherName: 7, method: 'fax' }),
    );
    expect(readWelcomeDraft('u1')).toEqual({ step: 'risk', profile: base, base, elsewhere: stayed });
    // Longer than any broker's name: kept only as far as one could run.
    localStorage.setItem(key, JSON.stringify({ step: 'risk', savedAt: Date.now(), otherName: 'x'.repeat(500) }));
    expect(readWelcomeDraft('u1')?.elsewhere.otherName).toHaveLength(200);
  });

  it('clears on finish', () => {
    writeWelcomeDraft('u1', { step: 'style', profile: answered, base, elsewhere: stayed });
    clearWelcomeDraft('u1');
    expect(readWelcomeDraft('u1')).toBeNull();
  });

  it('starts over from a stale, unreadable or unknown draft', () => {
    const key = 'langalpha-welcome-draft:u1';
    localStorage.setItem(key, JSON.stringify({ step: 'risk', savedAt: Date.now() - 31 * 24 * 3600 * 1000 }));
    expect(readWelcomeDraft('u1')).toBeNull();
    localStorage.setItem(key, '{not json');
    expect(readWelcomeDraft('u1')).toBeNull();
    localStorage.setItem(key, JSON.stringify({ step: 'interview', savedAt: Date.now() }));
    expect(readWelcomeDraft('u1')).toBeNull();
  });

  it('does nothing without a user', () => {
    writeWelcomeDraft(null, { step: 'risk', profile: answered, base, elsewhere: stayed });
    expect(localStorage.length).toBe(0);
    expect(readWelcomeDraft(undefined)).toBeNull();
  });
});
