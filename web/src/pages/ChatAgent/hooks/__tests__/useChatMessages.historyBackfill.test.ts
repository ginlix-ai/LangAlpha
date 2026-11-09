/**
 * A heavy thread's newest page stops short of a page's turns on its bytes, so
 * the depth a page used to give is filled in behind first paint: a desktop
 * fills back to a page of turns, a phone takes one older page and then keeps
 * the next one replayed but not shown, which the scroll trigger puts up at
 * once. Whatever overtakes the transcript drops a page held that way.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

const screen = vi.hoisted(() => ({ phone: false }));

vi.mock('@/hooks/useIsMobile', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useIsMobile')>()),
  getIsMobileSnapshot: () => screen.phone,
}));

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue('thread-1'),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../session/stream/mainEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).mainHandlersMockModule(await importOriginal()));

vi.mock('../../session/subagents/liveEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).subagentHandlersMockModule(await importOriginal()));

vi.mock('../../session/streamRefs', async (importOriginal) =>
  (await import('./chatHookHarness')).streamRefsMockModule(await importOriginal()));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { fetchThreadTurns, replayThreadHistory, getWorkflowStatus } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { settleMountEffect } from './chatHookHarness';
import type { PageBudget } from '../../session/history/pageBudget';

const mockFetchTurns = fetchThreadTurns as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockStatus = getWorkflowStatus as Mock;

type Emit = (e: Record<string, unknown>) => void;
type PageRequest = { page?: PageBudget; beforeTurn?: number };

// Every turn weighs this much on the wire.
const TURN_BYTES = 1_000_000;

/**
 * A thread of turns 0..80 served as the backend pages it under a budget: the
 * newest turns before `beforeTurn` until their bytes pass `maxBytes`, at least
 * `minTurns`, at most `limit`.
 */
async function serve(tid: string, onEvent: Emit, req: PageRequest = {}) {
  const end = req.beforeTurn === undefined ? 80 : req.beforeTurn - 1;
  let count = Math.min(req.page?.limit ?? end + 1, end + 1);
  if (req.page) count = Math.min(count, Math.max(req.page.minTurns, Math.floor(req.page.maxBytes / TURN_BYTES)));
  const from = end - count + 1;
  onEvent({ event: 'history_page', first_turn_index: from, has_more: from > 0 });
  for (let k = from; k <= end; k++) {
    onEvent({ event: 'user_message', turn_index: k, content: `question ${k}`, run_id: `run-${k}` });
    onEvent({ event: 'message_chunk', turn_index: k, role: 'assistant', content_type: 'text', content: `answer ${k}` });
  }
  onEvent({ event: 'replay_done', thread_id: tid });
}

function olderCalls(): PageRequest[] {
  return mockReplay.mock.calls
    .map((call) => call[2] as PageRequest)
    .filter((req) => req?.beforeTurn !== undefined);
}

async function mount(threadId?: () => string) {
  const hook = renderHookWithProviders(() => useChatMessages('ws-test', threadId?.()));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  return hook;
}

describe('useChatMessages – filling in a newest page cut short on its bytes', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    screen.phone = false;
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed', latest_turn_index: 80 });
    mockFetchTurns.mockResolvedValue({ turns: [] });
    mockReplay.mockImplementation(serve);
  });

  it('fills a desktop back to a page of turns, then owes nothing', async () => {
    const { result } = await mount();
    // 3 MB of 1 MB turns, raised to jsdom's floor of 5.
    expect(result.current.messages[0].id).toBe('history-user-76');
    expect(result.current.olderHistory.snapshot.aligned).toBe(true);

    for (let i = 0; i < 6; i++) {
      await act(async () => {
        await result.current.olderHistory.backfill();
      });
    }
    // 76..80, then 5 turns a page until 20 are loaded: 61..80.
    expect(olderCalls().map((req) => req.beforeTurn)).toEqual([76, 71, 66]);
    expect(olderCalls()[0].page).toEqual({ limit: 20, maxBytes: 3_000_000, minTurns: 5 });
    expect(result.current.messages[0].id).toBe('history-user-61');
    expect(result.current.olderHistory.snapshot.status).toBe('idle');
  });

  it('gives a phone one page, then holds the next one ready for the trigger', async () => {
    screen.phone = true;
    const { result } = await mount();
    // 1.5 MB on a phone: its floor of 3 turns.
    expect(result.current.messages[0].id).toBe('history-user-78');

    await act(async () => {
      await result.current.olderHistory.backfill();
    });
    expect(result.current.messages[0].id).toBe('history-user-75');
    await act(async () => {
      await result.current.olderHistory.backfill();
    });
    // Replayed, not shown.
    expect(olderCalls().map((req) => req.beforeTurn)).toEqual([78, 75]);
    expect(result.current.messages[0].id).toBe('history-user-75');
    expect(result.current.olderHistory.snapshot.status).toBe('idle');
    // Nothing more while one is held.
    await act(async () => {
      await result.current.olderHistory.backfill();
    });
    expect(olderCalls()).toHaveLength(2);

    expect(result.current.olderHistory.snapshot.hasAhead).toBe(true);

    // The trigger puts it up with no fetch of its own, which re-arms the
    // backfill to hold the next.
    const place = vi.fn((_n: number, commit: () => void) => commit());
    await act(async () => {
      await result.current.olderHistory.load(place);
    });
    expect(place).toHaveBeenCalledWith(6, expect.any(Function));
    expect(result.current.messages[0].id).toBe('history-user-72');
    expect(olderCalls()).toHaveLength(2);
    expect(result.current.olderHistory.snapshot.hasAhead).toBe(false);
    await act(async () => {
      await result.current.olderHistory.backfill();
    });
    expect(olderCalls().map((req) => req.beforeTurn)).toEqual([78, 75, 72]);
  });

  it('drops a page held ahead when the thread reloads', async () => {
    screen.phone = true;
    let latest = 80;
    mockStatus.mockImplementation(async () => ({ can_reconnect: false, status: 'completed', latest_turn_index: latest }));
    const { result } = await mount();
    await act(async () => {
      await result.current.olderHistory.backfill();
      await result.current.olderHistory.backfill();
    });
    expect(olderCalls()).toHaveLength(2);

    // A turn finished elsewhere; the reactivation check reloads the thread.
    latest = 81;
    const newestLoads = () => mockReplay.mock.calls.filter((call) => (call[2] as PageRequest).beforeTurn === undefined);
    const loads = newestLoads().length;
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(newestLoads()).toHaveLength(loads + 1));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    const before = olderCalls().length;

    // The page held before the reload is gone: the trigger asks afresh.
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(olderCalls().length).toBe(before + 1);
  });

  it('drops a page held ahead when the view switches threads', async () => {
    screen.phone = true;
    let threadId = 'thread-1';
    const { result, rerender } = await mount(() => threadId);
    await act(async () => {
      await result.current.olderHistory.backfill();
      await result.current.olderHistory.backfill();
    });
    threadId = 'thread-2';
    rerender();
    await settleMountEffect();
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    const before = olderCalls().length;
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(olderCalls().length).toBe(before + 1);
    expect(result.current.messages.every((m) => !m.id.endsWith('-72'))).toBe(true);
  });
});
