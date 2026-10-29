import { describe, it, expect } from 'vitest';
import { retryDelayMs } from '../lifecycle';

describe('retryDelayMs: the wait before the reconnect loop tries again', () => {
  it('doubles from one second when the server asks for nothing', () => {
    expect([1, 2, 3, 4].map((failed) => retryDelayMs(failed, null))).toEqual([1000, 2000, 4000, 8000]);
  });

  it('waits what a 429 asks, up to the longest step of the backoff', () => {
    expect(retryDelayMs(1, 2000)).toBe(2000);
    expect(retryDelayMs(1, 60_000)).toBe(8000);
  });
});
