import { describe, expect, it } from 'vitest';
import { showsDepth } from '../subagentText';

/**
 * Depth is the one control that clears a saved level, so it shows whenever a
 * saved level is one the provider lacks, even on a provider with no choice of
 * levels to offer.
 */

const levels = (...names: string[]) => names.map((name) => ({ name }));

describe('showsDepth', () => {
  it('shows for a provider with levels to choose from', () => {
    expect(showsDepth(levels('fast', 'deep'), null)).toBe(true);
    expect(showsDepth(levels('fast', 'deep'), 'deep')).toBe(true);
  });

  it('hides for a provider with one level or none while nothing stale is saved', () => {
    expect(showsDepth(levels('fast'), null)).toBe(false);
    expect(showsDepth(levels('fast'), 'fast')).toBe(false);
    expect(showsDepth([], null)).toBe(false);
  });

  it('shows to clear a saved level the provider lacks', () => {
    expect(showsDepth(levels('fast'), 'deep')).toBe(true);
    expect(showsDepth([], 'deep')).toBe(true);
  });
});
