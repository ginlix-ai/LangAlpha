// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { matchRank, matchSpans, matchesAllWords, queryWords } from '../textMatch';

describe('textMatch', () => {
  it('splits a query into lowercased words', () => {
    expect(queryWords('  Dark   Theme ')).toEqual(['dark', 'theme']);
    expect(queryWords('   ')).toEqual([]);
  });

  it('matches when every word appears, in any order', () => {
    expect(matchesAllWords('Theme: dark', ['dark', 'theme'])).toBe(true);
    expect(matchesAllWords('Theme: dark', ['dark', 'mode'])).toBe(false);
  });

  it('ranks a leading match over a word start over a substring over the extra text', () => {
    const words = queryWords('model');
    expect(matchRank('model-update', words)).toBe(0);
    expect(matchRank('Default model', words)).toBe(1);
    expect(matchRank('dcf-model', words)).toBe(2);
    expect(matchRank('xlsx', words, 'excel models')).toBe(3);
    expect(matchRank('xlsx', words)).toBeNull();
  });

  it('finds the spans the words cover, merged', () => {
    expect(matchSpans('NVDA Q3 nvda', ['nvda'])).toEqual([[0, 4], [8, 12]]);
    expect(matchSpans('dark theme', ['dar', 'ark'])).toEqual([[0, 4]]);
  });
});
