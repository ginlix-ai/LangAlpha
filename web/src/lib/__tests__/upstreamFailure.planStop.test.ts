// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { isReportable, planStopOf } from '@/lib/upstreamFailure';
import type { StructuredError } from '@/utils/rateLimitError';

const limit = { plan: 'ChatGPT Plus', window: 'five_hour' as const, until: '2026-11-06T20:40:00Z' };
const refusal = { reason: 'plan_limited' as const, window: null, until: null, creditsBilled: false };

describe("a plan's limit", () => {
  it("is the Claude plan's refusal or another subscription's limit", () => {
    expect(planStopOf({ message: '', planRefusal: refusal })).toEqual({ kind: 'claude', refusal });
    expect(planStopOf({ message: '', kind: 'upstream', planLimit: limit })).toEqual({ kind: 'subscription', limit });
    expect(planStopOf({ message: '', kind: 'upstream' })).toBeNull();
  });

  it('is the plan working, so neither card offers to report it', () => {
    const failed: StructuredError = { message: 'boom', kind: 'upstream', statusCode: 500 };
    expect(isReportable(failed)).toBe(true);
    expect(isReportable({ message: 'boom', kind: 'internal' })).toBe(true);
    expect(isReportable({ ...failed, statusCode: 429, planLimit: limit })).toBe(false);
    expect(isReportable({ message: '', planRefusal: refusal })).toBe(false);
  });
});
