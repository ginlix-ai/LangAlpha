import { describe, it, expect } from 'vitest';
import { buildRateLimitError } from '../rateLimitError';

describe('buildRateLimitError', () => {
  it('returns message + link for credit_limit when platformUrl is set', () => {
    const result = buildRateLimitError(
      { type: 'credit_limit', used_credits: 80, credit_limit: 100 },
      'https://ginlix.ai/account',
    );
    expect(result.message).toBe('Daily credit limit reached (80/100 credits). Resets at midnight UTC.');
    expect(result.link).toEqual({
      url: 'https://ginlix.ai/account/plans',
      label: 'Upgrade plan',
    });
  });

  it('routes monthly_credit_limit to /plans with an upgrade CTA', () => {
    const result = buildRateLimitError(
      { type: 'monthly_credit_limit' },
      'https://ginlix.ai/account',
    );
    expect(result.link).toEqual({
      url: 'https://ginlix.ai/account/plans',
      label: 'Upgrade plan',
    });
  });

  it('routes permanent_credit_limit to /plans with a top-up CTA', () => {
    const result = buildRateLimitError(
      { type: 'permanent_credit_limit' },
      'https://ginlix.ai/account',
    );
    expect(result.link).toEqual({
      url: 'https://ginlix.ai/account/plans',
      label: 'Top up',
    });
  });

  it('returns message without link for credit_limit when platformUrl is not set', () => {
    const result = buildRateLimitError(
      { type: 'credit_limit', used_credits: 80, credit_limit: 100 },
    );
    expect(result.message).toBe('Daily credit limit reached (80/100 credits). Resets at midnight UTC.');
    expect(result.link).toBeUndefined();
  });

  it('returns message without link for credit_limit when platformUrl is null', () => {
    const result = buildRateLimitError(
      { type: 'credit_limit', used_credits: 50, credit_limit: 50 },
      null,
    );
    expect(result.message).toContain('Daily credit limit reached');
    expect(result.link).toBeUndefined();
  });

  it('returns workspace_limit message with no link', () => {
    const result = buildRateLimitError(
      { type: 'workspace_limit', current: 3, limit: 3 },
      'https://ginlix.ai/account',
    );
    expect(result.message).toBe('Active workspace limit reached (3/3). Stop or delete an existing workspace to free up a slot.');
    expect(result.link).toBeUndefined();
  });

  it('returns burst_limit message', () => {
    const result = buildRateLimitError(
      { type: 'burst_limit' },
      'https://ginlix.ai/account',
    );
    expect(result.message).toBe('Too many concurrent requests. Please wait a moment.');
    expect(result.link).toBeUndefined();
  });

  it('renders the outstanding balance and links to /plans for negative_balance', () => {
    const result = buildRateLimitError(
      { type: 'negative_balance', outstanding_debt: 42 },
      'https://ginlix.ai/account',
    );
    expect(result.message).toBe(
      'Outstanding balance of 42 credits from prior platform usage. Top up to clear the debt and continue.',
    );
    expect(result.link).toEqual({
      url: 'https://ginlix.ai/account/plans',
      label: 'Top up',
    });
  });

  it('falls back to backend message for negative_balance when outstanding_debt is absent (legacy cached path)', () => {
    const result = buildRateLimitError(
      { type: 'negative_balance', message: 'Outstanding credit balance. Please add credits to continue.' },
      'https://ginlix.ai/account',
    );
    expect(result.message).toBe('Outstanding credit balance. Please add credits to continue.');
    expect(result.link).toEqual({
      url: 'https://ginlix.ai/account/plans',
      label: 'Top up',
    });
  });

  it('uses the generic next-step copy when negative_balance has neither number nor message', () => {
    const result = buildRateLimitError({ type: 'negative_balance' });
    expect(result.message).toBe('Outstanding credit balance. Top up to clear the debt and continue.');
    expect(result.link).toBeUndefined();
  });

  it('falls back to info.message for unknown types', () => {
    const result = buildRateLimitError(
      { type: 'unknown', message: 'Custom rate limit message' },
    );
    expect(result.message).toBe('Custom rate limit message');
    expect(result.link).toBeUndefined();
  });

  it('falls back to generic message when no type or message', () => {
    const result = buildRateLimitError({});
    expect(result.message).toBe('Rate limit exceeded. Please try again later.');
    expect(result.link).toBeUndefined();
  });
});
