/**
 * The steers a run left waiting on the server (`pending_steers` on the status)
 * have no other record until a turn reads them, so each load draws them as
 * queued bubbles. A bubble this view already shows under a listed key becomes
 * the status's copy rather than a second one, and during a live run they go
 * below the reply it is writing, since the run reads them after it.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus } from './chatHookHarness';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { getWorkflowStatus, reconnectToWorkflowStream, replayThreadHistory, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { MessageRecord } from '../utils/types';

const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSendStream = sendChatMessageStream as Mock;

type Emit = (e: Record<string, unknown>) => void;

const chunk = (content: string, extra: Record<string, unknown> = {}) => ({
  event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content, ...extra,
});

const flush = () => new Promise((r) => setTimeout(r, 0));

/** Turns 0 and 1 as the replay has them. */
const twoTurns = async (_tid: string, onEvent: Emit) => {
  onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
  onEvent({ ...chunk('answer 0'), turn_index: 0 });
  onEvent({ event: 'user_message', turn_index: 1, content: 'question 1', run_id: 'run-1' });
  onEvent({ ...chunk('answer 1'), turn_index: 1 });
  onEvent({ event: 'replay_done', thread_id: 'th-1' });
};

async function mount(status: ReturnType<typeof threadStatus>) {
  mockReplay.mockImplementation(twoTurns);
  mockStatus.mockResolvedValue(status);
  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  return hook;
}

type Hook = Awaited<ReturnType<typeof mount>>;

const messages = (hook: Hook) => hook.result.current.liveMessages.get() as MessageRecord[];

/** Each message as role and words, in screen order. */
const transcript = (hook: Hook) => messages(hook).map((m) => `${m.role}: ${String(m.content ?? '')}`);

const steerBubbles = (hook: Hook) => messages(hook).filter((m) => m.role === 'user' && m.steering);

const waiting = [
  { content: 'also check MSFT', request_key: 'rk-1', timestamp: 1_700_000_000 },
  { content: 'and GOOG', request_key: 'rk-2', timestamp: 1_700_000_001 },
];

describe('useChatMessages — pending_steers on the status', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('draws the steers an ended run left as queued bubbles under its latest turn, oldest first', async () => {
    const hook = await mount(threadStatus({
      can_reconnect: false, status: 'error', latest_turn_index: 1, pending_steers: waiting,
    }));

    await waitFor(() => expect(steerBubbles(hook)).toHaveLength(2));
    expect(transcript(hook)).toEqual([
      'user: question 0', 'assistant: answer 0', 'user: question 1', 'assistant: answer 1',
      'user: also check MSFT', 'user: and GOOG',
    ]);
    const [first, second] = steerBubbles(hook);
    expect(first).toMatchObject({
      id: 'pending-steer-rk-1', isHistory: true, steering: true, requestKey: 'rk-1', turnIndex: 1,
    });
    expect(first.timestamp).toEqual(new Date(1_700_000_000 * 1000));
    expect(second).toMatchObject({ id: 'pending-steer-rk-2', requestKey: 'rk-2', turnIndex: 1 });
  });

  it('skips an entry with no words', async () => {
    const hook = await mount(threadStatus({
      can_reconnect: false, status: 'error', latest_turn_index: 1,
      pending_steers: [{ content: '', request_key: 'rk-0' }, waiting[0]],
    }));

    await waitFor(() => expect(steerBubbles(hook)).toHaveLength(1));
    expect(steerBubbles(hook)[0].requestKey).toBe('rk-1');
  });

  it('puts them below the reply a live run is writing, not above it', async () => {
    mockReconnect.mockImplementation((_tid: string, _rid: string, _cursor: unknown, onEvent: Emit, signal: AbortSignal) =>
      new Promise((resolve) => {
        onEvent({ ...chunk('writing the reply'), turn_index: 1, _eventId: 1 });
        onEvent({ event: 'caught_up' });
        signal.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }), { once: true });
      }));
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
      onEvent({ ...chunk('answer 0'), turn_index: 0 });
      onEvent({ event: 'user_message', turn_index: 1, content: 'question 1', run_id: 'run-1' });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    });
    mockStatus.mockResolvedValue(threadStatus({
      can_reconnect: true, status: 'running', run_id: 'run-1', latest_turn_index: 1, pending_steers: [waiting[0]],
    }));
    const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
    await settleMountEffect();
    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    await waitFor(() => expect(transcript(hook)).toContain('assistant: writing the reply'));

    const order = transcript(hook);
    expect(order.indexOf('user: also check MSFT')).toBeGreaterThan(order.indexOf('assistant: writing the reply'));
    expect(steerBubbles(hook)).toHaveLength(1);
    hook.unmount();
  });

  it('turns this view\'s own copy of a listed steer into the status\'s, and draws no second one', async () => {
    const hook = await mount(threadStatus({ can_reconnect: false, status: 'completed', latest_turn_index: 1 }));
    let steerKey = '';
    let finishTurn: (r: unknown) => void = () => {};
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string | null,
      opts: { onEvent: Emit; requestKey: string; steerOnly?: boolean }) => {
      if (opts.steerOnly) {
        steerKey = opts.requestKey;
        opts.onEvent({ event: 'steering_accepted', position: 1 });
        return { disconnected: false };
      }
      opts.onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-2', _eventId: 1 });
      return new Promise((resolve) => { finishTurn = resolve; });
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('question 2');
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(true));
    await act(async () => {
      await hook.result.current.handleSendMessage('also check MSFT');
    });
    expect(steerKey).not.toBe('');
    expect(steerBubbles(hook)).toHaveLength(1);
    expect(steerBubbles(hook)[0].isHistory).toBeFalsy();

    // The run errors before reading the steer; the server keeps it for the
    // next turn and the reload the dropped stream asks for lists it.
    mockStatus.mockResolvedValue(threadStatus({
      can_reconnect: false, status: 'error', latest_turn_index: 2,
      pending_steers: [{ content: 'also check MSFT', request_key: steerKey, timestamp: 1_700_000_000 }],
    }));
    const statusCalls = mockStatus.mock.calls.length;
    await act(async () => {
      finishTurn({ disconnected: true });
      await flush();
    });
    await waitFor(() => expect(mockStatus.mock.calls.length).toBeGreaterThan(statusCalls));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    await waitFor(() => expect(steerBubbles(hook).every((m) => m.isHistory)).toBe(true));

    const copies = messages(hook).filter((m) => m.role === 'user' && m.content === 'also check MSFT');
    expect(copies).toHaveLength(1);
    expect(copies[0]).toMatchObject({ requestKey: steerKey, steering: true, isHistory: true });
  });
});
