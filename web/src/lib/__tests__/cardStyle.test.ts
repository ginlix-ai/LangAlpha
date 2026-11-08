import { beforeEach, describe, expect, it, vi } from 'vitest';
import { applyCardStyle, getCardStyle, restoreCardStyle, setCardStyle, storedCardStyle } from '../cardStyle';

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

describe('putting the page back', () => {
  it('shows a style without storing one when the browser held none', () => {
    setCardStyle('terminal');
    restoreCardStyle('soft', null);
    expect(getCardStyle()).toBe('soft');
    expect(document.documentElement.dataset.cardStyle).toBe('soft');
    expect(localStorage.getItem('cardStyle.v2')).toBeNull();
    expect(storedCardStyle()).toBeNull();
  });
});

describe('another tab of this browser', () => {
  function fromOtherTab(newValue: string | null, key: string | null = 'cardStyle.v2') {
    window.dispatchEvent(new StorageEvent('storage', { key, newValue }));
  }

  it('restyles this one, without writing back what it heard', () => {
    restoreCardStyle('balanced', null);
    const setItem = vi.spyOn(Storage.prototype, 'setItem');
    fromOtherTab('terminal');
    expect(getCardStyle()).toBe('terminal');
    expect(storedCardStyle()).toBe('terminal');
    expect(document.documentElement.hasAttribute('data-card-style')).toBe(false);
    expect(setItem).not.toHaveBeenCalled();
    setItem.mockRestore();
  });

  it('puts the default back when the style is removed there, and ignores other keys', () => {
    setCardStyle('soft');
    fromOtherTab('terminal', 'theme');
    expect(getCardStyle()).toBe('soft');
    fromOtherTab(null);
    expect(getCardStyle()).toBe('balanced');
    expect(storedCardStyle()).toBeNull();
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
    expect(m.storedCardStyle()).toBe(now);
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
    expect(m.storedCardStyle()).toBeNull();
  });
});
