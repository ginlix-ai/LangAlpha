/**
 * The device sign-in (ChatGPT, Grok) as a pure step function: given what one poll came
 * back with, whether to poll again and when, or how the sign-in ended. The
 * component only runs the timer, so every branch here is testable alone.
 */
import type { DevicePollResult, OAuthErrorDetail } from '@/api/oauth';

/** How a sign-in can end without connecting, each worded on its own. */
export type DeviceFailure =
  | 'denied'
  | 'expired'
  | 'no_pending_flow'
  | 'redis_unavailable'
  | 'device_flow_unavailable'
  | 'upstream_unavailable'
  | 'device_poll_failed'
  | 'exchange_failed'
  | 'save_failed'
  | 'unknown';

const NAMED_FAILURES = new Set<DeviceFailure>([
  'redis_unavailable',
  'device_flow_unavailable',
  'upstream_unavailable',
  'device_poll_failed',
  'exchange_failed',
  'save_failed',
  'no_pending_flow',
]);

export function failureFromError(error: OAuthErrorDetail | null): DeviceFailure {
  const code = error?.code as DeviceFailure | undefined;
  return code && NAMED_FAILURES.has(code) ? code : 'unknown';
}

export type PollOutcome =
  | { kind: 'result'; result: DevicePollResult }
  /** `error` null is a failure with no coded detail: a dropped connection. */
  | { kind: 'error'; error: OAuthErrorDetail | null };

export interface PollContext {
  intervalMs: number;
  /** Consecutive polls that failed in a way worth retrying. */
  retries: number;
  now: number;
  /** When the code stops working (ms), else a bound the caller picks. */
  deadline: number;
}

export type PollStep =
  | { next: 'poll'; delayMs: number; retries: number }
  | { next: 'connected'; result: DevicePollResult }
  | { next: 'failed'; failure: DeviceFailure };

/** The longest wait between polls while the provider or network is unhappy. */
export const MAX_BACKOFF_MS = 60_000;

export function nextPollStep(outcome: PollOutcome, ctx: PollContext): PollStep {
  if (outcome.kind === 'result') {
    const { result } = outcome;
    switch (result.status) {
      case 'success':
        return { next: 'connected', result };
      case 'denied':
        return { next: 'failed', failure: 'denied' };
      case 'expired':
        return { next: 'failed', failure: result.code === 'no_pending_flow' ? 'no_pending_flow' : 'expired' };
      case 'pending':
      default:
        if (ctx.now >= ctx.deadline) return { next: 'failed', failure: 'expired' };
        return { next: 'poll', delayMs: ctx.intervalMs, retries: 0 };
    }
  }
  // A dropped connection says nothing about the sign-in, so it is retried
  // like a coded retryable one; a coded refusal that is not retryable ends it.
  const retryable = outcome.error === null || outcome.error.retryable;
  if (!retryable) return { next: 'failed', failure: failureFromError(outcome.error) };
  if (ctx.now >= ctx.deadline) return { next: 'failed', failure: 'expired' };
  const retries = ctx.retries + 1;
  const delayMs = Math.min(ctx.intervalMs * 2 ** retries, MAX_BACKOFF_MS);
  return { next: 'poll', delayMs, retries };
}
