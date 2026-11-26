// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { djb2 } from '../hash';

// A widget's remembered height and its draft card ids are filed under this
// hash, so a change to its output strands what is stored under the old one.
describe('djb2', () => {
  it('keeps its output', () => {
    expect(djb2('')).toBe(5381);
    expect(djb2('<div>hi</div>')).toBe(154973279);
    expect((djb2('<div>hi</div>') >>> 0).toString(36)).toBe('2k9m5b');
  });

  it('wraps to a signed 32-bit integer', () => {
    const h = djb2('x'.repeat(300));
    expect(Number.isInteger(h)).toBe(true);
    expect(h).toBe(h | 0);
  });
});
