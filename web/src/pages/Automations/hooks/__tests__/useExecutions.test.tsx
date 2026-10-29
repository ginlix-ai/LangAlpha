/**
 * A run's delivery record lands after the run ends, and a finish still being
 * posted is asked again, so the history keeps polling at the live pace while
 * a run's record may still change, and drops back once it can't.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { AutomationExecution, DeliveryAttempt } from '@/types/automation';
import * as api from '../../utils/api';
import { DELIVERY_SETTLE_MS, IDLE_POLL_MS, LIVE_POLL_MS } from '../../utils/polling';
import { useExecutions } from '../useExecutions';

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  listExecutions: vi.fn(),
}));

const T0 = Date.parse('2026-10-01T13:02:10Z');

const LANDED: DeliveryAttempt = { method: 'slack', success: true, address: 'slack:T1/C1', name: '#demo', via: 'fallback', error: null };
const UNCONFIRMED: DeliveryAttempt = { ...LANDED, success: false, via: null, error: "Delivery couldn't be confirmed." };

function ended(msAgo: number, delivery: DeliveryAttempt[] | null): AutomationExecution {
  return {
    automation_execution_id: 'exec-1',
    automation_id: 'auto-1',
    status: 'completed',
    conversation_thread_id: null,
    scheduled_at: '2026-10-01T13:00:00Z',
    started_at: '2026-10-01T13:00:01Z',
    completed_at: new Date(T0 - msAgo).toISOString(),
    error_message: null,
    skip_reason: null,
    failure_reason: null,
    delivery_result: delivery,
    created_at: '2026-10-01T13:00:00Z',
    excerpt: null,
    dismissed_at: null,
  };
}

const page = (run: AutomationExecution) => ({ data: { executions: [run], has_more: false } }) as never;
const calls = () => vi.mocked(api.listExecutions).mock.calls.length;

/** Advances the clock, then lets the fetch it set off reach the hook. The
 *  query client notifies on a zero-delay timer, which a fake clock moves a
 *  millisecond on when it is set during a tick. */
async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
    await vi.advanceTimersByTimeAsync(1);
  });
}

describe('useExecutions: polling while a delivery settles', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('keeps the live pace while a row reads unconfirmed, then idles once the record is final', async () => {
    vi.mocked(api.listExecutions)
      .mockResolvedValueOnce(page(ended(10_000, [UNCONFIRMED])))
      .mockResolvedValue(page(ended(10_000, [LANDED])));
    const { result } = renderHookWithProviders(() => useExecutions('auto-1', true));
    await tick(0);
    expect(calls()).toBe(1);
    expect(result.current.executions[0].delivery_result).toEqual([UNCONFIRMED]);

    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(2);
    expect(result.current.executions[0].delivery_result).toEqual([LANDED]);

    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(2);
    await tick(IDLE_POLL_MS - LIVE_POLL_MS);
    expect(calls()).toBe(3);
  });

  it('idles once the asks are over, though a row still reads unconfirmed', async () => {
    // Ended 7s short of the window: two more fast polls fit inside it.
    vi.mocked(api.listExecutions).mockResolvedValue(page(ended(DELIVERY_SETTLE_MS - 7_000, [UNCONFIRMED])));
    renderHookWithProviders(() => useExecutions('auto-1', true));
    await tick(0);
    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(2);
    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(3);

    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(3);
    await tick(IDLE_POLL_MS - LIVE_POLL_MS);
    expect(calls()).toBe(4);
  });

  it('waits at the live pace for a first record only when the automation delivers', async () => {
    vi.mocked(api.listExecutions).mockResolvedValue(page(ended(5_000, null)));
    const delivering = renderHookWithProviders(() => useExecutions('auto-1', true));
    await tick(0);
    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(2);
    delivering.unmount();

    vi.mocked(api.listExecutions).mockClear();
    renderHookWithProviders(() => useExecutions('auto-1', false));
    await tick(0);
    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(1);
  });
});
