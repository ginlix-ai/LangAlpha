import { describe, expect, it } from 'vitest';
import type { TFunction } from 'i18next';
import { forkTitle } from '../useForkThread';

const t = ((_key: string, o: { title: string }) => `${o.title} (fork)`) as unknown as TFunction;

describe('forkTitle', () => {
  it('marks a title and leaves an empty one unasked', () => {
    expect(forkTitle('Plan', t)).toBe('Plan (fork)');
    expect(forkTitle('  ', t)).toBeUndefined();
  });

  it('does not mark twice', () => {
    expect(forkTitle('Plan (fork)', t)).toBe('Plan (fork)');
  });

  it('keeps the marked title within 255 code points', () => {
    const out = forkTitle('😀'.repeat(400), t)!;
    expect([...out].length).toBeLessThanOrEqual(255);
    expect(out.endsWith(' (fork)')).toBe(true);
    expect(forkTitle('a'.repeat(247) + ' ' + 'b'.repeat(10), t)).toBe('a'.repeat(247) + ' (fork)');
  });

  it('truncates an already marked title over the limit', () => {
    expect([...forkTitle('a'.repeat(300) + ' (fork)', t)!].length).toBe(255);
  });
});
