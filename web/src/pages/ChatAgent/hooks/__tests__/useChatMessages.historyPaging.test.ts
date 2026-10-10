/**
 * A paged thread opens on its newest turns, so the first bubble on screen is
 * not turn 0. Every turn-addressed action has to use the turn's own index
 * (stamped from the replay), never its position in the loaded transcript.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

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

// History handlers stay REAL: the replay has to build (and stamp) the bubbles.
vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import {
  sendChatMessageStream,
  fetchThreadTurns,
  replayThreadHistory,
  getWorkflowStatus,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { HISTORY_PAGE_TURNS } from '../../session/history/historyWindow';
import { scrollMemory } from '@/lib/scrollMemory';
import { settleMountEffect } from './chatHookHarness';

const mockSendStream = sendChatMessageStream as Mock;
const mockFetchTurns = fetchThreadTurns as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockStatus = getWorkflowStatus as Mock;

type Emit = (e: Record<string, unknown>) => void;

/** Replay events for turns [from, to], each a user message and a reply. */
function turnEvents(from: number, to: number): Record<string, unknown>[] {
  const out: Record<string, unknown>[] = [];
  for (let k = from; k <= to; k++) {
    out.push({ event: 'user_message', turn_index: k, content: `question ${k}`, run_id: `run-${k}` });
    out.push({
      event: 'message_chunk', turn_index: k, role: 'assistant', content_type: 'text', content: `answer ${k}`,
    });
  }
  return out;
}

/** A paged replay: the page header, then the turns, then replay_done. */
function pageOf(from: number, to: number, hasMore: boolean) {
  return [
    { event: 'history_page', first_turn_index: from, has_more: hasMore },
    ...turnEvents(from, to),
    { event: 'replay_done', thread_id: 'thread-1' },
  ];
}

type PageRequest = { limit?: number; beforeTurn?: number; signal?: AbortSignal };

/**
 * A thread whose newest turn is `latest()`, served the way the backend pages
 * it: the newest `limit` turns, or the `limit` turns before `beforeTurn`.
 */
function threadOf(latest: () => number) {
  return async (_tid: string, onEvent: Emit, req: PageRequest = {}) => {
    const end = req.beforeTurn === undefined ? latest() : req.beforeTurn - 1;
    const from = Math.max(0, end - (req.limit ?? end + 1) + 1);
    for (const item of pageOf(from, end, from > 0)) onEvent(item);
  };
}

/** A promise the test opens by hand. */
function gate() {
  let open!: () => void;
  const opened = new Promise<void>((resolve) => {
    open = resolve;
  });
  return { opened, open };
}

function ids(messages: Array<{ id: string }>): string[] {
  return messages.map((m) => m.id);
}

function replayWith(items: Record<string, unknown>[]) {
  return async (_tid: string, onEvent: Emit) => {
    for (const item of items) onEvent(item);
  };
}

/** /turns as the backend serves it: every turn of the thread, by turn_index. */
function allTurns(count: number) {
  return {
    turns: Array.from({ length: count }, (_, k) => ({
      turn_index: k,
      edit_checkpoint_id: k === 0 ? null : `edit-cp-${k}`,
      regenerate_checkpoint_id: `regen-cp-${k}`,
    })),
    retry_checkpoint_id: `regen-cp-${count - 1}`,
  };
}

async function mountOnPage(from: number, to: number, hasMore: boolean) {
  mockReplay.mockImplementation(replayWith(pageOf(from, to, hasMore)));
  const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  return hook;
}

function lastForkOpts(): Record<string, unknown> {
  const call = mockSendStream.mock.calls.at(-1)!;
  return call[3] as Record<string, unknown>;
}

describe('useChatMessages – absolute turn index on a paged transcript', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed', latest_turn_index: 80 });
    mockFetchTurns.mockResolvedValue(allTurns(81));
    mockSendStream.mockResolvedValue({ disconnected: false });
  });

  it('regenerating turn 80 forks turn 80 when the loaded history starts at turn 75', async () => {
    const { result } = await mountOnPage(75, 80, true);
    expect(result.current.messages.map((m) => m.id)).toContain('history-assistant-80');

    await act(async () => {
      await result.current.handleRegenerate('history-assistant-80');
    });

    expect(result.current.messageError).toBeNull();
    const opts = lastForkOpts();
    expect(opts.forkFromTurn).toBe(80);
    expect(opts.checkpointId).toBe('regen-cp-80');
  });

  it('editing turn 80 forks from turn 80 when the loaded history starts at turn 75', async () => {
    const { result } = await mountOnPage(75, 80, true);

    await act(async () => {
      await result.current.handleEditMessage('history-user-80', 'changed');
    });

    expect(result.current.messageError).toBeNull();
    const opts = lastForkOpts();
    expect(opts.forkFromTurn).toBe(80);
    expect(opts.checkpointId).toBe('edit-cp-80');
  });

  it('regenerating a turn from a page loaded above the transcript forks that turn', async () => {
    mockReplay.mockImplementation(threadOf(() => 80));
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountEffect();
    await waitFor(() => expect(result.current.olderHistory.status).toBe('idle'));
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(result.current.messages[0].id).toBe('history-user-41');

    await act(async () => {
      await result.current.handleRegenerate('history-assistant-50');
    });
    expect(lastForkOpts().forkFromTurn).toBe(50);
    expect(lastForkOpts().checkpointId).toBe('regen-cp-50');
  });

  it('matches /turns by turn_index, not by position, when the list has gaps', async () => {
    mockFetchTurns.mockResolvedValue({
      turns: [
        { turn_index: 3, edit_checkpoint_id: 'edit-cp-3', regenerate_checkpoint_id: 'regen-cp-3' },
        { turn_index: 77, edit_checkpoint_id: 'edit-cp-77', regenerate_checkpoint_id: 'regen-cp-77' },
      ],
    });
    const { result } = await mountOnPage(75, 80, true);

    await act(async () => {
      await result.current.handleRegenerate('history-assistant-77');
    });
    expect(lastForkOpts().checkpointId).toBe('regen-cp-77');
    expect(lastForkOpts().forkFromTurn).toBe(77);
  });

  it('edits a turn with no checkpoint from the next entry, keeping its own turn', async () => {
    mockFetchTurns.mockResolvedValue({
      turns: [
        { turn_index: 75, edit_checkpoint_id: 'edit-cp-75', regenerate_checkpoint_id: 'regen-cp-75' },
        { turn_index: 77, edit_checkpoint_id: 'edit-cp-77', regenerate_checkpoint_id: 'regen-cp-77' },
      ],
    });
    const { result } = await mountOnPage(75, 80, true);

    await act(async () => {
      await result.current.handleEditMessage('history-user-76', 'changed');
    });
    expect(result.current.messageError).toBeNull();
    expect(lastForkOpts().checkpointId).toBe('edit-cp-77');
    expect(lastForkOpts().forkFromTurn).toBe(76);
  });
});

describe('useChatMessages – paging older history', () => {
  let latest = 80;

  beforeEach(() => {
    vi.clearAllMocks();
    latest = 80;
    mockStatus.mockImplementation(async () => ({ can_reconnect: false, status: 'completed', latest_turn_index: latest }));
    mockFetchTurns.mockResolvedValue(allTurns(81));
    mockSendStream.mockResolvedValue({ disconnected: false });
    mockReplay.mockImplementation(threadOf(() => latest));
  });

  async function mount(threadId?: () => string) {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test', threadId?.()));
    await settleMountEffect();
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    return hook;
  }

  function olderCalls() {
    return mockReplay.mock.calls.filter((call) => (call[2] as PageRequest | undefined)?.beforeTurn !== undefined);
  }

  it('opens on the newest page and puts each older page above it, keeping every bubble', async () => {
    const { result } = await mount();
    expect(mockReplay.mock.calls[0][2]).toEqual({ limit: HISTORY_PAGE_TURNS });
    expect(result.current.messages[0].id).toBe('history-user-61');
    expect(result.current.olderHistory.status).toBe('idle');
    const newest = ids(result.current.messages);

    await act(async () => {
      await result.current.olderHistory.load();
    });
    const request = olderCalls()[0][2] as PageRequest;
    expect(request).toMatchObject({ beforeTurn: 61, limit: HISTORY_PAGE_TURNS });
    expect(request.signal).toBeInstanceOf(AbortSignal);
    const afterOne = ids(result.current.messages);
    expect(afterOne.slice(0, 2)).toEqual(['history-user-41', 'history-assistant-41']);
    expect(afterOne.slice(-newest.length)).toEqual(newest);
    expect(result.current.olderHistory.status).toBe('idle');

    await act(async () => {
      await result.current.olderHistory.load();
      await result.current.olderHistory.load();
      await result.current.olderHistory.load();
    });
    // 21..40, then 1..20, then 0 alone: the start of the thread ends paging.
    expect(olderCalls().map((call) => (call[2] as PageRequest).beforeTurn)).toEqual([61, 41, 21, 1]);
    expect(result.current.messages[0].id).toBe('history-user-0');
    expect(result.current.olderHistory.status).toBe('end');
    expect(ids(result.current.messages).slice(-afterOne.length)).toEqual(afterOne);
  });

  it('a later send still opens the next turn after paging', async () => {
    const { result } = await mount();
    await act(async () => {
      await result.current.olderHistory.load();
    });
    await act(async () => {
      await result.current.handleSendMessage('next question');
    });
    const sent = result.current.messages.filter((m) => !m.id.startsWith('history-'));
    expect(sent.map((m) => ('turnIndex' in m ? m.turnIndex : undefined))).toEqual([81, 81]);
  });

  it('a reload after paging asks for every loaded turn, so nothing on screen goes', async () => {
    const { result } = await mount();
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(result.current.messages[0].id).toBe('history-user-41');

    // Two turns finished elsewhere; the reactivation check reloads the thread.
    latest = 82;
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(result.current.messages.some((m) => m.id === 'history-user-82')).toBe(true));
    const reload = mockReplay.mock.calls.at(-1)![2] as PageRequest;
    expect(reload.beforeTurn).toBeUndefined();
    expect(reload.limit).toBeGreaterThanOrEqual(82 - 41 + 1);
    expect(result.current.messages[0].id).not.toBe('history-user-61');
    expect(result.current.messages.some((m) => m.id === 'history-user-41')).toBe(true);
    expect(result.current.olderHistory.status).toBe('idle');

    // The next reload reaches back to turn 41 again, not to where the last
    // one's margin started: the transcript keeps its start.
    const start = result.current.messages[0].id;
    latest = 83;
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(result.current.messages.some((m) => m.id === 'history-user-83')).toBe(true));
    expect((mockReplay.mock.calls.at(-1)![2] as PageRequest).limit).toBe(83 - 41 + 1 + 5);
    expect(result.current.messages[0].id).toBe(start);
  });

  it('a reload drops an older page still on its way', async () => {
    const { result } = await mount();
    const late = gate();
    let pageSignal: AbortSignal | undefined;
    const serve = threadOf(() => latest);
    mockReplay.mockImplementation(async (tid: string, onEvent: Emit, req: PageRequest = {}) => {
      if (req.beforeTurn !== undefined) {
        pageSignal = req.signal;
        await late.opened;
      }
      return serve(tid, onEvent, req);
    });

    let page!: Promise<void>;
    act(() => {
      page = result.current.olderHistory.load();
    });
    await waitFor(() => expect(result.current.olderHistory.status).toBe('loading'));

    latest = 81;
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(result.current.messages.some((m) => m.id === 'history-user-81')).toBe(true));
    expect(pageSignal?.aborted).toBe(true);

    late.open();
    await act(async () => {
      await page;
    });
    expect(result.current.messages.some((m) => m.id === 'history-user-41')).toBe(false);
    // The reload still covers the page that was on screen, 61 onward.
    expect(result.current.messages.some((m) => m.id === 'history-user-61')).toBe(true);
    expect(result.current.olderHistory.status).toBe('idle');
  });

  it('a thread switch drops an older page still on its way', async () => {
    let threadId = 'thread-1';
    const { result, rerender } = await mount(() => threadId);
    const late = gate();
    const serve = threadOf(() => latest);
    mockReplay.mockImplementation(async (tid: string, onEvent: Emit, req: PageRequest = {}) => {
      if (req.beforeTurn !== undefined) await late.opened;
      return serve(tid, onEvent, req);
    });

    let page!: Promise<void>;
    act(() => {
      page = result.current.olderHistory.load();
    });
    await waitFor(() => expect(result.current.olderHistory.status).toBe('loading'));

    threadId = 'thread-2';
    rerender();
    await settleMountEffect();
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    const opened = ids(result.current.messages);

    late.open();
    await act(async () => {
      await page;
    });
    expect(ids(result.current.messages)).toEqual(opened);
    expect(result.current.messages.some((m) => m.id === 'history-user-41')).toBe(false);
    expect(result.current.olderHistory.status).toBe('idle');
  });

  it('a return to a thread left mid-way reaches back to the oldest page it had loaded', async () => {
    let threadId = 'thread-1';
    const { result, rerender } = await mount(() => threadId);
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(result.current.messages[0].id).toBe('history-user-41');
    // Left mid-way: useChatScroll keeps a pixel offset for the thread.
    scrollMemory.set('thread:thread-1', 1200);
    try {
      threadId = 'thread-2';
      rerender();
      await settleMountEffect();
      await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
      threadId = 'thread-1';
      rerender();
      await settleMountEffect();
      // The offset is measured from turn 41 down, so the page starts there
      // again, with no margin above it.
      await waitFor(() => expect(result.current.messages[0]?.id).toBe('history-user-41'));
      expect((mockReplay.mock.calls.at(-1)![2] as PageRequest).limit).toBe(80 - 41 + 1);

      // Left at the bottom: the newest page is enough.
      scrollMemory.set('thread:thread-1', 'bottom');
      threadId = 'thread-2';
      rerender();
      await settleMountEffect();
      await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
      threadId = 'thread-1';
      rerender();
      await settleMountEffect();
      await waitFor(() => expect(result.current.messages[0]?.id).toBe('history-user-61'));
    } finally {
      scrollMemory.forget('thread:thread-1');
    }
  });

  it('asks for one older page at a time', async () => {
    const { result } = await mount();
    const late = gate();
    const serve = threadOf(() => latest);
    mockReplay.mockImplementation(async (tid: string, onEvent: Emit, req: PageRequest = {}) => {
      if (req.beforeTurn !== undefined) await late.opened;
      return serve(tid, onEvent, req);
    });

    let first!: Promise<void>;
    let second!: Promise<void>;
    act(() => {
      first = result.current.olderHistory.load();
      second = result.current.olderHistory.load();
    });
    late.open();
    await act(async () => {
      await Promise.all([first, second]);
    });
    expect(olderCalls()).toHaveLength(1);
    expect(result.current.messages.filter((m) => m.id === 'history-user-41')).toHaveLength(1);
  });

  it('keeps a live bubble where it is when an older page lands', async () => {
    const { result } = await mount();
    let finishStream!: () => void;
    mockSendStream.mockImplementation(() => new Promise((resolve) => {
      finishStream = () => resolve({ disconnected: false });
    }));
    act(() => {
      void result.current.handleSendMessage('live question');
    });
    await waitFor(() => expect(result.current.messages.some((m) => !m.id.startsWith('history-'))).toBe(true));
    const tail = ids(result.current.messages).slice(-2);

    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(ids(result.current.messages).slice(-2)).toEqual(tail);
    expect(result.current.messages[0].id).toBe('history-user-41');

    await act(async () => {
      finishStream();
    });
  });

  it('lands an older page that was on its way when a turn was sent', async () => {
    const { result } = await mount();
    const late = gate();
    const serve = threadOf(() => latest);
    mockReplay.mockImplementation(async (tid: string, onEvent: Emit, req: PageRequest = {}) => {
      if (req.beforeTurn !== undefined) await late.opened;
      return serve(tid, onEvent, req);
    });
    let page!: Promise<void>;
    act(() => {
      page = result.current.olderHistory.load();
    });
    await waitFor(() => expect(result.current.olderHistory.status).toBe('loading'));

    let finishStream!: () => void;
    mockSendStream.mockImplementation(() => new Promise((resolve) => {
      finishStream = () => resolve({ disconnected: false });
    }));
    act(() => {
      void result.current.handleSendMessage('live question');
    });
    await waitFor(() => expect(result.current.messages.some((m) => !m.id.startsWith('history-'))).toBe(true));
    const tail = ids(result.current.messages).slice(-2);

    late.open();
    await act(async () => {
      await page;
    });
    // The page the send found on its way is the one that lands: no second ask.
    expect(olderCalls()).toHaveLength(1);
    expect(result.current.messages[0].id).toBe('history-user-41');
    expect(ids(result.current.messages).slice(-2)).toEqual(tail);
    expect(result.current.olderHistory.status).toBe('idle');

    await act(async () => {
      finishStream();
    });
  });

  it('holds the top while a fork cuts the transcript, dropping the page on its way', async () => {
    const { result } = await mount();
    const late = gate();
    let pageSignal: AbortSignal | undefined;
    const serve = threadOf(() => latest);
    mockReplay.mockImplementation(async (tid: string, onEvent: Emit, req: PageRequest = {}) => {
      if (req.beforeTurn !== undefined) {
        pageSignal = req.signal;
        await late.opened;
      }
      return serve(tid, onEvent, req);
    });
    let page!: Promise<void>;
    act(() => {
      page = result.current.olderHistory.load();
    });
    await waitFor(() => expect(result.current.olderHistory.status).toBe('loading'));

    // The fork cuts first, then reads the checkpoint it forks from.
    const checkpoints = gate();
    mockFetchTurns.mockImplementation(async () => {
      await checkpoints.opened;
      return allTurns(81);
    });
    let fork!: Promise<void>;
    act(() => {
      fork = result.current.handleRegenerate('history-assistant-70');
    });
    await waitFor(() => expect(result.current.olderHistory.status).toBe('held'));
    expect(pageSignal?.aborted).toBe(true);
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(olderCalls()).toHaveLength(1);

    checkpoints.open();
    await act(async () => {
      await fork;
    });
    expect(lastForkOpts().forkFromTurn).toBe(70);
    expect(result.current.olderHistory.status).toBe('idle');

    late.open();
    await act(async () => {
      await page;
    });
    expect(result.current.messages.some((m) => m.id === 'history-user-41')).toBe(false);
  });

  it('lets paging go on after a fork whose checkpoint read failed', async () => {
    const { result } = await mount();
    mockFetchTurns.mockResolvedValue({ turns: [] });
    await act(async () => {
      await result.current.handleRegenerate('history-assistant-70');
    });
    expect(result.current.messageError).toBe('Unable to regenerate: checkpoint data unavailable');
    expect(result.current.olderHistory.status).toBe('idle');
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(result.current.messages[0].id).toBe('history-user-41');
  });

  it('a page that cannot start before the one it extends ends paging', async () => {
    const { result } = await mount();
    const serve = threadOf(() => latest);
    mockReplay.mockImplementation(async (tid: string, onEvent: Emit, req: PageRequest = {}) => {
      if (req.beforeTurn === undefined) return serve(tid, onEvent, req);
      // A misbehaving page: claims more but starts where the last one did.
      onEvent({ event: 'history_page', first_turn_index: req.beforeTurn, has_more: true });
      onEvent({ event: 'replay_done', thread_id: 'thread-1' });
    });
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(result.current.olderHistory.status).toBe('end');
  });

  it('an unpaged replay is the whole thread, with nothing more to load', async () => {
    mockReplay.mockImplementation(replayWith([...turnEvents(0, 3), { event: 'replay_done', thread_id: 'thread-1' }]));
    latest = 3;
    const { result } = await mount();
    expect(result.current.messages[0].id).toBe('history-user-0');
    expect(result.current.olderHistory.status).toBe('end');
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(olderCalls()).toHaveLength(0);
  });

  it('a failed older page offers a retry and keeps the transcript', async () => {
    const { result } = await mount();
    const before = ids(result.current.messages);
    const serve = threadOf(() => latest);
    mockReplay.mockImplementationOnce(async () => {
      throw new Error('network');
    });
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(result.current.olderHistory.status).toBe('failed');
    expect(ids(result.current.messages)).toEqual(before);

    mockReplay.mockImplementation(serve);
    await act(async () => {
      await result.current.olderHistory.load();
    });
    expect(result.current.olderHistory.status).toBe('idle');
    expect(result.current.messages[0].id).toBe('history-user-41');
  });
});
