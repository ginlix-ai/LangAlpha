// @vitest-environment node
import { describe, it, expect } from 'vitest';
import { parseErrorMessage } from '../parseErrorMessage';

describe('parseErrorMessage — rate limit passthrough', () => {
  it('passes through "Daily credit limit" without redundant prefix', () => {
    const result = parseErrorMessage('Daily credit limit reached (80/100 credits). Resets at midnight UTC.');
    expect(result.title).toBe('Daily credit limit reached (80/100 credits). Resets at midnight UTC.');
    expect(result.detail).toBeNull();
  });

  it('passes through "Active workspace limit" without redundant prefix', () => {
    const result = parseErrorMessage('Active workspace limit reached (3/3).');
    expect(result.title).toBe('Active workspace limit reached (3/3).');
    expect(result.detail).toBeNull();
  });

  // "Too many concurrent requests" never enters the rate-limit branch
  // (outer regex matches "too many requests", not "too many concurrent"),
  // so no passthrough guard is needed — it falls through to the short-string fallback.

  it('wraps generic rate limit messages with "Rate limit exceeded" title', () => {
    const result = parseErrorMessage('You exceeded your current quota');
    expect(result.title).toBe('Rate limit exceeded');
    expect(result.detail).toBe('You exceeded your current quota');
    expect(result.statusCode).toBe(429);
  });

  it('wraps "too many requests" with "Rate limit exceeded" title', () => {
    const result = parseErrorMessage('429 too many requests');
    expect(result.title).toBe('Rate limit exceeded');
    expect(result.detail).toBe('429 too many requests');
    expect(result.statusCode).toBe(429);
  });
});
