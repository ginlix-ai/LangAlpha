// @vitest-environment node
import { describe, expect, it } from 'vitest';
import { MAX_BACKOFF_MS, nextPollStep, type PollContext } from '../devicePoll';

const ctx = (over: Partial<PollContext> = {}): PollContext => ({
  intervalMs: 5000,
  retries: 0,
  now: 1_000,
  deadline: 900_000,
  ...over,
});

describe('nextPollStep', () => {
  it('polls again at the server interval while pending', () => {
    expect(nextPollStep({ kind: 'result', result: { status: 'pending' } }, ctx({ retries: 3 })))
      .toEqual({ next: 'poll', delayMs: 5000, retries: 0 });
  });

  it('connects on success', () => {
    const result = { status: 'success' as const, email: 'a@b.c', plan_type: 'pro' };
    expect(nextPollStep({ kind: 'result', result }, ctx())).toEqual({ next: 'connected', result });
  });

  it('stops on a decline and on an expired code, telling them apart', () => {
    expect(nextPollStep({ kind: 'result', result: { status: 'denied' } }, ctx())).toEqual({ next: 'failed', failure: 'denied' });
    expect(nextPollStep({ kind: 'result', result: { status: 'expired', code: 'device_code_expired' } }, ctx()))
      .toEqual({ next: 'failed', failure: 'expired' });
    expect(nextPollStep({ kind: 'result', result: { status: 'expired', code: 'no_pending_flow' } }, ctx()))
      .toEqual({ next: 'failed', failure: 'no_pending_flow' });
  });

  it('reads a refusal by its code, and an unnamed one as unknown', () => {
    expect(nextPollStep({ kind: 'error', error: { code: 'device_poll_failed', message: '', retryable: false } }, ctx()))
      .toEqual({ next: 'failed', failure: 'device_poll_failed' });
    expect(nextPollStep({ kind: 'error', error: { code: 'something_new', message: '', retryable: false } }, ctx()))
      .toEqual({ next: 'failed', failure: 'unknown' });
  });

  it('gives up as expired once the code has run out, even while still pending', () => {
    expect(nextPollStep({ kind: 'result', result: { status: 'pending' } }, ctx({ now: 900_000 })))
      .toEqual({ next: 'failed', failure: 'expired' });
  });

  it('backs off on a retryable error and on a dropped connection, up to a cap', () => {
    const retryable = { code: 'upstream_unavailable', message: '', retryable: true };
    expect(nextPollStep({ kind: 'error', error: retryable }, ctx())).toEqual({ next: 'poll', delayMs: 10_000, retries: 1 });
    expect(nextPollStep({ kind: 'error', error: null }, ctx({ retries: 1 }))).toEqual({ next: 'poll', delayMs: 20_000, retries: 2 });
    expect(nextPollStep({ kind: 'error', error: retryable }, ctx({ retries: 6 }))).toEqual({ next: 'poll', delayMs: MAX_BACKOFF_MS, retries: 7 });
  });

  it('stops on a terminal error with its own reason', () => {
    const terminal = (code: string) => ({ kind: 'error' as const, error: { code, message: '', retryable: false } });
    expect(nextPollStep(terminal('exchange_failed'), ctx())).toEqual({ next: 'failed', failure: 'exchange_failed' });
    expect(nextPollStep(terminal('device_poll_failed'), ctx())).toEqual({ next: 'failed', failure: 'device_poll_failed' });
    expect(nextPollStep(terminal('something_new'), ctx())).toEqual({ next: 'failed', failure: 'unknown' });
  });

  it('does not retry past the deadline', () => {
    expect(nextPollStep({ kind: 'error', error: null }, ctx({ now: 900_001 }))).toEqual({ next: 'failed', failure: 'expired' });
  });
});
