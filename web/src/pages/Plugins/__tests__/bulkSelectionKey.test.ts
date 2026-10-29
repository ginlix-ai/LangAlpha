import { describe, expect, it } from 'vitest';
import { bulkSelectionKey } from '../components/useBulkSelection';

describe('bulkSelectionKey', () => {
  it('follows the rows on screen, not the retained selection', () => {
    // A and B stay selected across a filter change; only A is visible under
    // the first filter and only B under the second, so a confirm armed on A
    // must not survive into a bar whose action now reaches B.
    const retained = new Set(['a', 'b']);
    const underFirst = ['a'].filter((k) => retained.has(k));
    const underSecond = ['b'].filter((k) => retained.has(k));
    expect(bulkSelectionKey(underFirst)).not.toBe(bulkSelectionKey(underSecond));
  });

  it('is order-insensitive and empty for no targets', () => {
    expect(bulkSelectionKey(['b', 'a'])).toBe(bulkSelectionKey(['a', 'b']));
    expect(bulkSelectionKey([])).toBe('');
  });
});
