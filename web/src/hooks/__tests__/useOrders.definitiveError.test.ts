/**
 * Which order-read failures are worth asking about again.
 *
 * The retry and the poll both read this one predicate rather than each judging
 * for itself, because they disagreed once: the retry stopped only on a 404
 * while the poll stopped on any failure at all, so a transient 500 cleared the
 * interval and left an open order frozen on screen for as long as the tab
 * stayed focused.
 */
import { describe, expect, it } from 'vitest';
import { isDefinitiveOrderError } from '../useOrders';

function httpError(status: number) {
  return { response: { status } };
}

describe('isDefinitiveOrderError', () => {
  // Nothing about the row changes by asking again: it is absent, it belongs to
  // someone else, or it is gone.
  it.each([404, 403, 410])('stops asking on %i', (status) => {
    expect(isDefinitiveOrderError(httpError(status))).toBe(true);
  });

  // The backend or the link had a bad moment, which the next ask can survive.
  it.each([500, 502, 503, 429, 408])('keeps asking on %i', (status) => {
    expect(isDefinitiveOrderError(httpError(status))).toBe(false);
  });

  // A thrown network error carries no response at all, and that is the case
  // most worth retrying rather than the least.
  it('keeps asking when the failure names no status', () => {
    expect(isDefinitiveOrderError(new Error('Network Error'))).toBe(false);
    expect(isDefinitiveOrderError(null)).toBe(false);
    expect(isDefinitiveOrderError(undefined)).toBe(false);
  });
});
