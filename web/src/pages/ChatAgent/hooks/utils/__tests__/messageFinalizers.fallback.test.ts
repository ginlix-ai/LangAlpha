// @vitest-environment node
/**
 * The transcript's notice of a model switch keeps the switch as the server
 * reported it, for the divider to name when shown (see
 * `NotificationDivider.modelFallback.test.tsx`).
 */
import { describe, it, expect } from 'vitest';
import { buildModelFallbackSegment } from '../messageFinalizers';

const t = (key: string, opts?: Record<string, unknown>) => `${key} ${JSON.stringify(opts ?? {})}`;

describe('buildModelFallbackSegment', () => {
  it('keeps the models and who pays for the one it moved to', () => {
    const segment = buildModelFallbackSegment(
      { from_model: 'alpha-oauth', to_model: 'beta', to_paid_by: 'platform' }, t, 3,
    );
    expect(segment).toEqual({
      type: 'notification',
      fallback: { from: 'alpha-oauth', to: 'beta', paidBy: 'platform' },
      order: 3,
    });
  });

  it('leaves the payer out when the server did not say', () => {
    const segment = buildModelFallbackSegment({ from_model: 'alpha', to_model: 'beta' }, t, 3);
    expect(segment.fallback).toEqual({ from: 'alpha', to: 'beta' });
  });

  it('carries the failure as the error detail', () => {
    const segment = buildModelFallbackSegment(
      { from_model: 'alpha', to_model: 'beta', status_code: 503, attempts_on_from: 2, error: ' upstream down ' }, t, 3,
    );
    expect(segment.detail).toBe('HTTP 503 · chat.modelFallbackAttempts {"count":2}\nupstream down');
    expect(segment.detailKind).toBe('error');
  });
});
