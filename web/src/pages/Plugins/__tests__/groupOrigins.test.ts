// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { matchesFilter } from '../utils/groupOrigins';

describe('matchesFilter', () => {
  it('matches case-insensitively across any provided field', () => {
    expect(matchesFilter('CTX', 'other', null, 'my-ctx-server')).toBe(true);
    expect(matchesFilter('missing', 'alpha', 'beta')).toBe(false);
  });

  it('treats an empty or whitespace filter as match-all', () => {
    expect(matchesFilter('', 'anything')).toBe(true);
    expect(matchesFilter('   ', 'anything')).toBe(true);
    expect(matchesFilter('  x ', 'x')).toBe(true);
  });

  it('never matches on null/undefined fields', () => {
    expect(matchesFilter('x', null, undefined)).toBe(false);
  });
});
