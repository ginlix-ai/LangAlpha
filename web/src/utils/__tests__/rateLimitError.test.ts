import { describe, it, expect } from 'vitest';
import { buildRateLimitError } from '../rateLimitError';

// Deliberately not a host we operate. The portal URL is injected
// (`VITE_PLATFORM_URL`), so a neutral origin here is what proves it: a fixture
// naming our own deployment would still pass if the value were hardcoded.
const PORTAL = 'https://account.example.com';
const PORTAL_LINKS = [
  {
    url: `${PORTAL}/plans`,
    label: 'Manage plan',
    labelKey: 'chat.errorLinkManagePlan',
    external: true,
  },
  {
    url: `${PORTAL}/usage`,
    label: 'View usage',
    labelKey: 'chat.errorLinkViewUsage',
    external: true,
  },
];

describe('buildRateLimitError', () => {
  it('forwards the quota service message verbatim', () => {
    const result = buildRateLimitError(
      {
        type: 'credit_limit',
        used_credits: 80,
        credit_limit: 100,
        message: 'Daily credit limit reached (80/100 credits). Resets at midnight.',
      },
      PORTAL,
    );
    expect(result.message).toBe('Daily credit limit reached (80/100 credits). Resets at midnight.');
    expect(result.links).toEqual(PORTAL_LINKS);
  });

  it('forwards the message for a limit type it has never heard of', () => {
    // The point of the whole file: a new limit type on the quota service needs
    // no change here, and gets the same two CTAs.
    const result = buildRateLimitError(
      { type: 'some_future_limit', message: 'A limit we do not know about yet.' },
      PORTAL,
    );
    expect(result.message).toBe('A limit we do not know about yet.');
    expect(result.links).toEqual(PORTAL_LINKS);
  });

  it('does not reconstruct copy from the numeric fields', () => {
    // The counts still ride along for callers that want them, but a denial that
    // arrives without a message gets the generic line, never a sentence
    // assembled here out of used/limit.
    const result = buildRateLimitError(
      { type: 'credit_limit', used_credits: 80, credit_limit: 100 },
      PORTAL,
    );
    expect(result.message).toBe('Rate limit exceeded. Please try again later.');
  });

  it('marks the CTA external even when platformUrl is a same-origin path', () => {
    // The default. A path-shaped portal URL is same-origin and shaped exactly
    // like one of our routes, so without the flag the router swallows the click
    // and the CTA does nothing.
    const result = buildRateLimitError({ type: 'monthly_credit_limit' }, '/account');
    expect(result.links).toEqual([
      {
        url: '/account/plans',
        label: 'Manage plan',
        labelKey: 'chat.errorLinkManagePlan',
        external: true,
      },
      {
        url: '/account/usage',
        label: 'View usage',
        labelKey: 'chat.errorLinkViewUsage',
        external: true,
      },
    ]);
  });

  it('omits links when no portal is configured', () => {
    expect(buildRateLimitError({ type: 'credit_limit', message: 'Denied.' }).links).toBeUndefined();
    expect(buildRateLimitError({ type: 'credit_limit', message: 'Denied.' }, null).links).toBeUndefined();
  });

  it('keeps our own copy for burst_limit, and offers no portal link', () => {
    // This one is ours: raised by this service's concurrency gate, and nothing
    // on the account portal clears it.
    const result = buildRateLimitError({ type: 'burst_limit' }, PORTAL);
    expect(result.message).toBe('Too many concurrent requests. Please wait a moment.');
    expect(result.links).toBeUndefined();
  });

  it('ignores a quota service message for burst_limit', () => {
    const result = buildRateLimitError({ type: 'burst_limit', message: 'not ours to say' }, PORTAL);
    expect(result.message).toBe('Too many concurrent requests. Please wait a moment.');
  });

  it('offers no portal link when the service is down', () => {
    // service_unavailable is the quota service's misconfiguration guard, not a
    // cap. "Manage plan" here invites the user to buy their way out of an outage.
    const result = buildRateLimitError(
      { type: 'service_unavailable', message: 'Service temporarily unavailable. Please try again shortly.' },
      PORTAL,
    );
    expect(result.message).toBe('Service temporarily unavailable. Please try again shortly.');
    expect(result.links).toBeUndefined();
  });

  it('falls back to a generic message when there is no type or message', () => {
    const result = buildRateLimitError({});
    expect(result.message).toBe('Rate limit exceeded. Please try again later.');
    expect(result.links).toBeUndefined();
  });
});
