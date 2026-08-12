import { describe, it, expect } from 'vitest';
import { isNearBottom } from '../scrollHelpers';

describe('isNearBottom', () => {
  it('is true at the very bottom', () => {
    expect(isNearBottom({ scrollTop: 900, scrollHeight: 1000, clientHeight: 100 })).toBe(true);
  });
  it('is false far from the bottom', () => {
    expect(isNearBottom({ scrollTop: 0, scrollHeight: 1000, clientHeight: 100 })).toBe(false);
  });
  it('respects the threshold boundary (default 120)', () => {
    // distance = scrollHeight - scrollTop - clientHeight
    expect(isNearBottom({ scrollTop: 781, scrollHeight: 1000, clientHeight: 100 })).toBe(true); // 119
    expect(isNearBottom({ scrollTop: 780, scrollHeight: 1000, clientHeight: 100 })).toBe(true); // 120 (inclusive)
    expect(isNearBottom({ scrollTop: 779, scrollHeight: 1000, clientHeight: 100 })).toBe(false); // 121
  });
  it('honors a custom threshold', () => {
    expect(isNearBottom({ scrollTop: 870, scrollHeight: 1000, clientHeight: 100 }, 40)).toBe(true); // 30
    expect(isNearBottom({ scrollTop: 850, scrollHeight: 1000, clientHeight: 100 }, 40)).toBe(false); // 50
  });
});
