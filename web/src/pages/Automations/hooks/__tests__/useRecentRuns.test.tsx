/**
 * The feed polls its first page, and stops once the reader loads more. A run
 * whose delivery record may still change keeps it polling either way, since
 * nothing announces the later record: at the live pace on the first page,
 * at the idle pace once every page refetches, and not at all after.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { AutomationRun, DeliveryAttempt } from '@/types/automation';
import * as api from '../../utils/api';
import { DELIVERY_SETTLE_MS, IDLE_POLL_MS, LIVE_POLL_MS } from '../../utils/polling';
import { useRecentRuns } from '../useRecentRuns';

vi.mock('../../utils/api', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../../utils/api')>()),
  listRecentRuns: vi.fn(),
}));

const T0 = Date.parse('2026-10-01T13:02:10Z');

const LANDED: DeliveryAttempt = { method: 'slack', success: true, address: 'slack:T1/C1', name: '#demo', via: 'agent', error: null };
const UNCONFIRMED: DeliveryAttempt = { ...LANDED, success: false, via: null, error: "Delivery couldn't be confirmed." };

function run(id: string, msAgo: number, delivery: DeliveryAttempt[] | null, automationId = 'auto-1'): AutomationRun {
  return {
    automation_execution_id: id,
    automation_id: automationId,
    status: 'completed',
    conversation_thread_id: null,
    scheduled_at: '2026-10-01T12:00:00Z',
    started_at: '2026-10-01T12:00:01Z',
    completed_at: new Date(T0 - msAgo).toISOString(),
    error_message: null,
    skip_reason: null,
    failure_reason: null,
    delivery_result: delivery,
    created_at: '2026-10-01T12:00:00Z',
    excerpt: null,
    dismissed_at: null,
    automation_name: 'Morning briefing',
    trigger_type: 'cron',
    workspace_id: null,
  };
}

const OLD = run('old', 60 * 60_000, [LANDED], 'auto-2');
const feedPage = (executions: AutomationRun[], hasMore = false) =>
  ({ data: { executions, has_more: hasMore } }) as never;
const calls = () => vi.mocked(api.listRecentRuns).mock.calls.length;
const deliversAll = () => true;

/** Advances the clock, then lets the fetch it set off reach the hook. The
 *  query client notifies on a zero-delay timer, which a fake clock moves a
 *  millisecond on when it is set during a tick. */
async function tick(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
    await vi.advanceTimersByTimeAsync(1);
  });
}

/** Answers the first page and the second by offset; `first` reads the
 *  newest run as of each call. */
function serve(first: () => AutomationRun) {
  vi.mocked(api.listRecentRuns).mockImplementation(async (params) =>
    (params as { offset?: number }).offset ? feedPage([OLD]) : feedPage([first()], true),
  );
}

describe('useRecentRuns: polling while a delivery settles', () => {
  beforeEach(() => {
    vi.useFakeTimers();
    vi.setSystemTime(T0);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.clearAllMocks();
  });

  it('polls the first page at the live pace until the record is final', async () => {
    let newest = run('new', 10_000, [UNCONFIRMED]);
    serve(() => newest);
    const { result } = renderHookWithProviders(() => useRecentRuns(deliversAll));
    await tick(0);
    expect(calls()).toBe(1);

    newest = run('new', 10_000, [LANDED]);
    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(2);
    expect(result.current.runs[0].delivery_result).toEqual([LANDED]);

    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(2);
  });

  it('waits for a first record only for an automation that delivers', async () => {
    serve(() => run('new', 5_000, null));
    renderHookWithProviders(() => useRecentRuns((id) => id !== 'auto-1'));
    await tick(0);
    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(1);
  });

  it('refetches every page at the idle pace while a record settles past the first page', async () => {
    let newest = run('new', 10_000, [UNCONFIRMED]);
    serve(() => newest);
    const { result } = renderHookWithProviders(() => useRecentRuns(deliversAll));
    await tick(0);
    await act(async () => {
      await result.current.fetchNextPage();
    });
    expect(calls()).toBe(2);

    await tick(LIVE_POLL_MS);
    expect(calls()).toBe(2);
    newest = run('new', 10_000, [LANDED]);
    await tick(IDLE_POLL_MS - LIVE_POLL_MS);
    // Both pages, once each.
    expect(calls()).toBe(4);
    expect(result.current.runs.map((r) => r.automation_execution_id)).toEqual(['new', 'old']);
    expect(result.current.runs[0].delivery_result).toEqual([LANDED]);

    // Final now: past the first page, nothing polls.
    await tick(IDLE_POLL_MS);
    expect(calls()).toBe(4);
  });

  it('stops past the first page once the asks are over, though the record still reads unconfirmed', async () => {
    serve(() => run('new', DELIVERY_SETTLE_MS - 20_000, [UNCONFIRMED]));
    const { result } = renderHookWithProviders(() => useRecentRuns(deliversAll));
    await tick(0);
    await act(async () => {
      await result.current.fetchNextPage();
    });
    expect(calls()).toBe(2);

    await tick(IDLE_POLL_MS);
    expect(calls()).toBe(4);
    await tick(IDLE_POLL_MS);
    expect(calls()).toBe(4);
  });
});
