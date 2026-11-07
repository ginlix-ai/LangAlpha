import { beforeEach, describe, expect, it, vi } from 'vitest';
import { applyCardStyle, getCardStyle, setCardStyle } from '../cardStyle';

beforeEach(() => {
  localStorage.clear();
  document.documentElement.removeAttribute('data-card-style');
});

describe('card style', () => {
  it('reads Balanced when nothing, or something unknown, is stored', () => {
    expect(getCardStyle()).toBe('balanced');
    localStorage.setItem('cardStyle', 'classic');
    expect(getCardStyle()).toBe('balanced');
  });

  it('keeps a choice for the next load and stamps it on the root', () => {
    setCardStyle('balanced');
    expect(getCardStyle()).toBe('balanced');
    expect(document.documentElement.dataset.cardStyle).toBe('balanced');
  });

  it('stamps nothing for Terminal, whose look is the stylesheets themselves', () => {
    applyCardStyle('soft');
    applyCardStyle('terminal');
    expect(document.documentElement.hasAttribute('data-card-style')).toBe(false);
  });
});

describe('a choice stored under the old ids', () => {
  async function fresh() {
    vi.resetModules();
    return import('../cardStyle');
  }

  it.each([
    ['quiet', 'balanced'],
    ['balanced', 'soft'],
    ['terminal', 'terminal'],
  ])('reads an old %s as %s and keeps it under the new key', async (old, now) => {
    localStorage.setItem('cardStyle', old);
    const m = await fresh();
    expect(m.getCardStyle()).toBe(now);
    expect(m.hasStoredCardStyle()).toBe(true);
    expect(localStorage.getItem('cardStyle.v2')).toBe(now);
    expect(localStorage.getItem('cardStyle')).toBeNull();
  });

  it('prefers the new key, so a choice is carried over only once', async () => {
    localStorage.setItem('cardStyle', 'balanced');
    localStorage.setItem('cardStyle.v2', 'balanced');
    const m = await fresh();
    expect(m.getCardStyle()).toBe('balanced');
    expect(localStorage.getItem('cardStyle')).toBeNull();
  });

  it('offers nothing to the account when neither key holds a style', async () => {
    localStorage.setItem('cardStyle', 'constructor');
    const m = await fresh();
    expect(m.getCardStyle()).toBe('balanced');
    expect(m.hasStoredCardStyle()).toBe(false);
  });
});
