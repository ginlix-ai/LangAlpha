/**
 * What the next send summarizes first, and the skip of it, stand until a send
 * starts a run of its own: a send refused or lost before that, or steered into
 * a run already going, still meets the summary next time. Every request that
 * starts a turn carries the skip: a send, an edit, a regenerate and a Retry.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus } from './chatHookHarness';

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { fetchThreadTurns, getWorkflowStatus, replayThreadHistory, sendChatMessageStream, sendRetryStream } from '../../utils/api';
import type { NextSummary } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { readRetryNotice, recoveryRequest, sendRequest } from '../../session/stream/autoRetry';

const mockFetchTurns = fetchThreadTurns as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSendStream = sendChatMessageStream as Mock;
const mockRetryStream = sendRetryStream as Mock;
const mockStatus = getWorkflowStatus as Mock;

type Emit = (e: Record<string, unknown>) => void;
type SendOptions = { onEvent: Emit; keepFullContext?: boolean };

const COLD = { reason: 'cold', at: 1_700_000_000_000, now: 1_700_000_000_000 };

function page() {
  const items: Record<string, unknown>[] = [{ event: 'history_page', first_turn_index: 0, has_more: false }];
  for (let k = 0; k <= 1; k++) {
    items.push({ event: 'user_message', turn_index: k, content: `question ${k}`, run_id: `run-${k}` });
    items.push({ event: 'message_chunk', turn_index: k, role: 'assistant', agent: 'main', content_type: 'text', content: `answer ${k}` });
  }
  return [...items, { event: 'replay_done', thread_id: 'th-1', latest_run_id: 'run-1' }];
}

async function mount() {
  mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
    for (const item of page()) onEvent(item);
  });
  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  return hook;
}

type Hook = Awaited<ReturnType<typeof mount>>;

async function send(result: Hook['result'], text: string) {
  await act(async () => {
    await result.current.handleSendMessage(text);
  });
  await waitFor(() => expect(result.current.isLoading).toBe(false));
}

const runStarts = async (_m: string, _ws: string, _tid: string, { onEvent }: SendOptions) => {
  onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-new' });
  return { disconnected: false, aborted: false };
};

describe('useChatMessages: the next summary outlasts a send that starts no run', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus.mockResolvedValue(threadStatus({ next_summary: COLD }));
  });

  it('keeps it through a refused send, a steer, and a lost POST, and drops it once a run starts', async () => {
    const { result } = await mount();
    expect(result.current.nextSummary).toEqual(COLD);
    // Read once: what stands below is the mount's reading, not a later one.
    mockStatus.mockClear();

    mockSendStream.mockRejectedValueOnce(Object.assign(new Error('busy'), { status: 409, errorInfo: { code: 'running', message: 'busy' } }));
    await send(result, 'refused');
    expect(result.current.nextSummary).toEqual(COLD);

    mockSendStream.mockImplementationOnce(async (_m: string, _ws: string, _tid: string, { onEvent }: SendOptions) => {
      onEvent({ event: 'steering_accepted', thread_id: 'th-1', content: 'steered', position: 1 });
      return { disconnected: false, aborted: false };
    });
    await send(result, 'steered');
    expect(result.current.nextSummary).toEqual(COLD);

    mockSendStream.mockRejectedValueOnce(new TypeError('network error'));
    await send(result, 'lost');
    expect(result.current.nextSummary).toEqual(COLD);

    expect(mockSendStream).toHaveBeenCalledTimes(3);
    expect(mockStatus).not.toHaveBeenCalled();

    mockSendStream.mockImplementationOnce(runStarts);
    await send(result, 'started');
    expect(result.current.nextSummary).toBeNull();
  });
});

describe('useChatMessages: a turn this view did not stream', () => {
  beforeEach(() => vi.clearAllMocks());

  it('takes the reading from the status a re-shown view reads', async () => {
    mockStatus.mockResolvedValue(threadStatus({ next_summary: null }));
    const { result } = await mount();
    expect(result.current.nextSummary).toBeNull();

    // A wake turn ended while the view was hidden.
    mockStatus.mockResolvedValue(threadStatus({ next_summary: COLD, latest_turn_index: 1 }));
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    expect(result.current.nextSummary).toEqual(COLD);
  });
});

describe('useChatMessages: every request that starts a turn carries the skip', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus.mockResolvedValue(threadStatus({ next_summary: COLD }));
    mockFetchTurns.mockResolvedValue({
      turns: [0, 1].map((k) => ({ turn_index: k, edit_checkpoint_id: `cp-in-${k}`, regenerate_checkpoint_id: `cp-${k}` })),
    });
    mockSendStream.mockImplementation(runStarts);
    mockRetryStream.mockImplementation(async (_ws: string, _tid: string, onEvent: Emit) => {
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-retry' });
      return { disconnected: false, aborted: false };
    });
  });

  const sentSkip = () => (mockSendStream.mock.lastCall?.[3] as SendOptions).keepFullContext;
  const bubble = (result: Hook['result'], role: 'user' | 'assistant', text: string) =>
    (result.current.messages as { id: string; role: string; content?: unknown }[])
      .find((m) => m.role === role && String(m.content).includes(text))!.id;

  it.each([true, false])('an edit sends keepFullContext=%s', async (skip) => {
    const { result } = await mount();
    await act(async () => {
      await result.current.handleEditMessage(bubble(result, 'user', 'question 1'), 'question 1 again', {}, skip);
    });
    expect(mockSendStream).toHaveBeenCalledOnce();
    expect(sentSkip()).toBe(skip);
  });

  it.each([true, false])('a regenerate sends keepFullContext=%s', async (skip) => {
    const { result } = await mount();
    await act(async () => {
      await result.current.handleRegenerate(bubble(result, 'assistant', 'answer 1'), {}, skip);
    });
    expect(mockSendStream).toHaveBeenCalledOnce();
    expect(sentSkip()).toBe(skip);
  });

  it.each([true, false])('a Retry sends keepFullContext=%s', async (skip) => {
    const { result } = await mount();
    await act(async () => {
      await result.current.handleRetry({}, skip);
    });
    expect(mockRetryStream).toHaveBeenCalledOnce();
    expect(mockRetryStream.mock.lastCall?.[11]).toBe(skip);
  });
});

describe('an automatic retry repeats the skip of the request that failed', () => {
  it.each([['retry', true], ['retry', false], ['resend', true], ['resend', false]] as const)('%s keeps keepFullContext=%s', (recovery, skip) => {
    const failed = sendRequest('question', null, { keepFullContext: skip });
    const next = recoveryRequest(failed, readRetryNotice({ event: 'retry', recovery }), 'run-failed');
    expect(next?.keepFullContext).toBe(skip);
  });
});

describe('a /compact or /offload that settles after the view moved on', () => {
  beforeEach(() => vi.clearAllMocks());

  it('leaves the reading and the transcript of the thread now on screen alone', async () => {
    const OTHER: NextSummary = { reason: 'threshold' };
    mockStatus.mockImplementation(async (tid: string) => threadStatus({ next_summary: tid === 'th-2' ? OTHER : COLD }));
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      for (const item of page()) onEvent(item);
    });
    let tid = 'th-1';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws', tid));
    await settleMountEffect();
    await waitFor(() => expect(result.current.nextSummary).toEqual(COLD));

    tid = 'th-2';
    rerender();
    await waitFor(() => expect(result.current.nextSummary).toEqual(OTHER));

    const notices = () => result.current.messages.filter((m) => m.role === 'notification').map((m) => m.content);
    const left = result.current.forThread('th-1');
    act(() => left.setNextSummary(null));
    act(() => left.setNextSummary(COLD as NextSummary));
    act(() => left.insertNotification('compacted on th-1'));
    expect(result.current.nextSummary).toEqual(OTHER);
    expect(notices()).toEqual([]);

    const shown = result.current.forThread('th-2');
    act(() => shown.setNextSummary(null));
    act(() => shown.insertNotification('compacted on th-2'));
    expect(result.current.nextSummary).toBeNull();
    expect(notices()).toEqual(['compacted on th-2']);
  });
});
