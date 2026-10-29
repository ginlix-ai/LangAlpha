/**
 * Tests that turn index calculations correctly exclude steering assistant messages.
 *
 * Steering messages (mid-turn follow-ups sent while the agent is running) create
 * extra assistant message bubbles in the frontend that don't correspond to backend
 * turns. The turn index must skip these when mapping to backend checkpoint data.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

// Mount with the thread already known (the production shape) so the history
// loader records its load key up front. Otherwise the sync mock stream commits
// thread_id AFTER the send resolves, the loader fires post-finalize, and —
// since finished turns are marked isHistory — it would clear the live bubbles
// against this fixture's empty replay.
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

vi.mock('../../session/history/historyHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).historyHandlersMockModule(await importOriginal()));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import {
  sendChatMessageStream,
  sendRetryStream,
  fetchThreadTurns,
  replayThreadHistory,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage, UserMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;
const mockRetryStream = sendRetryStream as Mock;
const mockFetchTurns = fetchThreadTurns as Mock;
const mockReplay = replayThreadHistory as Mock;

/**
 * Settle the mount history load before sending — its isHistory-clear must not
 * land mid-send and remove the finished turn's bubbles.
 */
async function settleMountLoad() {
  await waitFor(() => expect(mockReplay).toHaveBeenCalled());
  await act(async () => {});
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type StreamCallback = (e: Record<string, unknown>) => void;

/**
 * Mock the stream to emit thread_id, then N steering_delivered events, then
 * on the NEXT call emit just thread_id (no steering). This simulates two turns
 * where the first has steering continuations and the second doesn't.
 */
function mockTwoTurnsWithSteering(steeringCount: number) {
  let callCount = 0;
  mockSendStream.mockImplementation(
    async (
      _msg: string,
      _ws: string,
      _tid: string | null,
      { onEvent }: { onEvent: StreamCallback },
    ) => {
      callCount++;
      onEvent({ event: 'thread_id', thread_id: 'thread-1' });
      if (callCount === 1) {
        for (let i = 0; i < steeringCount; i++) {
          onEvent({
            event: 'steering_delivered',
            messages: [{ content: `follow-up ${i}`, timestamp: Date.now() / 1000 }],
          });
        }
      }
      return { disconnected: false };
    },
  );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useChatMessages – turn index with steering messages', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchTurns.mockResolvedValue({
      turns: [
        { turn_index: 0, edit_checkpoint_id: null, regenerate_checkpoint_id: 'cp-0' },
        { turn_index: 1, edit_checkpoint_id: 'cp-0', regenerate_checkpoint_id: 'cp-1' },
      ],
      retry_checkpoint_id: 'cp-1',
    });
  });

  it('steering_delivered creates assistant messages with isSteering flag', async () => {
    mockTwoTurnsWithSteering(3);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      const assistants = result.current.messages.filter(
        (m): m is AssistantMessage => m.role === 'assistant',
      );
      // 1 real assistant + 3 steering assistants
      expect(assistants.length).toBe(4);

      const steering = assistants.filter((m) => m.isSteering);
      expect(steering.length).toBe(3);

      // The first assistant (from the original turn) should NOT be steering
      expect(assistants[0].isSteering).toBeFalsy();
    });
  });

  it('turn index calculation excludes isSteering messages', () => {
    // Unit test of the filtering logic used in handleRegenerate/handleEditMessage/deriveTurnIndex
    const messages = [
      { id: 'u0', role: 'user' },
      { id: 'a0', role: 'assistant' },                           // turn 0
      { id: 'su1', role: 'user', steeringDelivered: true },
      { id: 'sa1', role: 'assistant', isSteering: true },        // steering (not a turn)
      { id: 'su2', role: 'user', steeringDelivered: true },
      { id: 'sa2', role: 'assistant', isSteering: true },        // steering (not a turn)
      { id: 'su3', role: 'user', steeringDelivered: true },
      { id: 'sa3', role: 'assistant', isSteering: true },        // steering (not a turn)
      { id: 'u1', role: 'user' },
      { id: 'a1', role: 'assistant' },                           // turn 1
      { id: 'u2', role: 'user' },
      { id: 'a2', role: 'assistant' },                           // turn 2
    ];

    // Regenerate turn index: count non-steering assistants up to and including target
    const regenTurnIndex = (msgId: string) => {
      const msgIndex = messages.findIndex(m => m.id === msgId);
      return messages.slice(0, msgIndex + 1).filter(m => m.role === 'assistant' && !m.isSteering).length - 1;
    };

    // Edit turn index: count non-steering assistants before target user message
    const editTurnIndex = (msgId: string) => {
      const msgIndex = messages.findIndex(m => m.id === msgId);
      return messages.slice(0, msgIndex).filter(m => m.role === 'assistant' && !m.isSteering).length;
    };

    // Regenerate: last assistant (a2) should be turn 2
    expect(regenTurnIndex('a2')).toBe(2);
    // Regenerate: middle real assistant (a1) should be turn 1
    expect(regenTurnIndex('a1')).toBe(1);
    // Regenerate: first assistant (a0) should be turn 0
    expect(regenTurnIndex('a0')).toBe(0);

    // Without the fix (counting all assistants), a2 would be turn 5 — WRONG
    const brokenRegenIndex = messages.slice(0, messages.findIndex(m => m.id === 'a2') + 1)
      .filter(m => m.role === 'assistant').length - 1;
    expect(brokenRegenIndex).toBe(5); // demonstrates the bug

    // Edit: editing u1 (after 3 steering pairs) should be turn 1
    expect(editTurnIndex('u1')).toBe(1);
    // Edit: editing u2 should be turn 2
    expect(editTurnIndex('u2')).toBe(2);
  });

  it('handleRegenerate uses correct turnIndex with steering messages present', async () => {
    mockTwoTurnsWithSteering(2);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();

    // Send two messages: first with steering, second without
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await act(async () => {
      await result.current.handleSendMessage('next question');
    });

    // Wait for all messages to settle
    let lastAssistantId: string;
    await waitFor(() => {
      const assistants = result.current.messages.filter(
        (m): m is AssistantMessage => m.role === 'assistant',
      );
      expect(assistants.length).toBe(4); // 2 real + 2 steering
      lastAssistantId = assistants[assistants.length - 1].id;
    });

    // Regenerate the last assistant message
    await act(async () => {
      await result.current.handleRegenerate(lastAssistantId!);
    });

    await waitFor(() => {
      expect(mockFetchTurns).toHaveBeenCalledWith('thread-1');
    });

    // Without the fix, turnIndex=3 would exceed backend's 2 turns → "checkpoint data unavailable"
    // With the fix, turnIndex=1 correctly maps to turns[1]
    expect(result.current.messageError).toBeNull();
  });

  it('refuses to edit a steering bubble; regenerating the continuation re-runs the whole turn', async () => {
    // A steering message has no turn identity (no /turns boundary). Editing it
    // must refuse — the fork would land on the NEXT turn and leave the original
    // steering text in the agent's context. Regenerating the post-steering
    // continuation is well-defined: the whole owning turn re-runs from its
    // input checkpoint (mid-run steering can't be replayed), and truncation
    // normalizes back to the turn's first bubble so the stale half and the
    // steering bubble leave the transcript with it.
    mockTwoTurnsWithSteering(1);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    let steeringUserId: string;
    let steeringAssistantId: string;
    await waitFor(() => {
      const su = result.current.messages.find(
        (m): m is UserMessage => m.role === 'user' && !!(m as UserMessage).steeringDelivered,
      );
      const sa = result.current.messages.find(
        (m): m is AssistantMessage => m.role === 'assistant' && !!(m as AssistantMessage).isSteering,
      );
      expect(su).toBeDefined();
      expect(sa).toBeDefined();
      steeringUserId = su!.id;
      steeringAssistantId = sa!.id;
    });

    const before = result.current.messages;
    const sendCalls = mockSendStream.mock.calls.length;

    await act(async () => {
      await result.current.handleEditMessage(steeringUserId!, 'changed my mind');
    });
    expect(result.current.messageError).toMatch(/steering/i);
    expect(result.current.messages).toBe(before); // no optimistic truncation
    expect(mockSendStream.mock.calls.length).toBe(sendCalls); // no fork sent

    await act(async () => {
      await result.current.handleRegenerate(steeringAssistantId!);
    });
    await waitFor(() => {
      expect(mockSendStream.mock.calls.length).toBe(sendCalls + 1);
    });
    const after = result.current.messages;
    // Steering user bubble gone with the re-run, single user message remains.
    expect(after.some((m) => m.role === 'user' && !!(m as UserMessage).steeringDelivered)).toBe(false);
    expect(after.filter((m) => m.role === 'user').length).toBe(1);
    // One fresh assistant bubble for the re-run — pre-steering half replaced too.
    expect(after.filter((m) => m.role === 'assistant').length).toBe(1);
    expect(result.current.messageError).toBeNull();
  });
});

describe('useChatMessages – /turns entries named by turn_index', () => {
  // The empty checkpoint the server names for the thread's first message.
  const ROOT = '00000000-0000-6000-8000-000000000000';
  // Turn 1's run died before its first checkpoint: it has rows and bubbles but
  // no /turns entry, so turns 2 and 3 sit one place below their numbers.
  const TURNS = {
    turns: [
      { turn_index: 0, edit_checkpoint_id: ROOT, regenerate_checkpoint_id: 'cp-in-0' },
      { turn_index: 2, edit_checkpoint_id: 'cp-end-0', regenerate_checkpoint_id: 'cp-in-2' },
      { turn_index: 3, edit_checkpoint_id: 'cp-end-2', regenerate_checkpoint_id: 'cp-in-3' },
    ],
    retry_checkpoint_id: 'cp-end-3',
  };

  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchTurns.mockResolvedValue(TURNS);
    mockSendStream.mockImplementation(
      async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: StreamCallback }) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        return { disconnected: false };
      },
    );
  });

  async function renderFourTurns() {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    for (const text of ['q0', 'q1', 'q2', 'q3']) {
      await act(async () => {
        await hook.result.current.handleSendMessage(text);
      });
      // Bubble ids carry Date.now(); sends in the same millisecond would share them.
      await new Promise((resolve) => setTimeout(resolve, 2));
    }
    return hook;
  }

  const lastFork = () => {
    const options = mockSendStream.mock.lastCall?.[3] as { checkpointId: string; forkFromTurn: number };
    return { checkpointId: options.checkpointId, forkFromTurn: options.forkFromTurn };
  };

  it.each([
    // [turn, checkpoint]: the failed turn forks where the next checkpointed one does.
    [0, ROOT],
    [1, 'cp-end-0'],
    [2, 'cp-end-0'],
    [3, 'cp-end-2'],
  ])('edits turn %i from %s', async (turn, checkpointId) => {
    const { result } = await renderFourTurns();
    const userId = result.current.messages.filter((m) => m.role === 'user')[turn].id;
    await act(async () => {
      await result.current.handleEditMessage(userId, 'changed');
    });
    expect(result.current.messageError).toBeNull();
    expect(lastFork()).toEqual({ checkpointId, forkFromTurn: turn });
  });

  it.each([
    [2, 'cp-in-2'],
    [3, 'cp-in-3'],
  ])('regenerates turn %i from %s', async (turn, checkpointId) => {
    const { result } = await renderFourTurns();
    const assistantId = result.current.messages.filter((m) => m.role === 'assistant')[turn].id;
    await act(async () => {
      await result.current.handleRegenerate(assistantId);
    });
    expect(result.current.messageError).toBeNull();
    expect(lastFork()).toEqual({ checkpointId, forkFromTurn: turn });
  });

  it('regenerates a turn with no checkpoint by sending its message again in place', async () => {
    const { result } = await renderFourTurns();
    const assistantId = result.current.messages.filter((m) => m.role === 'assistant')[1].id;
    await act(async () => {
      await result.current.handleRegenerate(assistantId);
    });
    expect(result.current.messageError).toBeNull();
    expect(lastFork()).toEqual({ checkpointId: 'cp-end-0', forkFromTurn: 1 });
    expect(mockSendStream.mock.lastCall?.[0]).toBe('q1');
    expect(result.current.messages.filter((m) => m.role === 'user').map((m) => m.content)).toEqual(['q0', 'q1']);
  });

  // Turn 1 never checkpointed; turns 2 and 3 did.
  const turnsThrough = (last: number) => ({
    turns: TURNS.turns.filter((entry) => entry.turn_index <= last),
    retry_checkpoint_id: null,
  });
  const refuseNextSend = () =>
    mockSendStream.mockRejectedValueOnce(Object.assign(new Error('Internal Server Error'), { status: 500 }));

  async function send(result: { current: ReturnType<typeof useChatMessages> }, text: string, attachments: Record<string, unknown>[] | null = null) {
    await act(async () => {
      await result.current.handleSendMessage(text, null, attachments);
    });
    await new Promise((resolve) => setTimeout(resolve, 2));
  }

  it('sends a message the server refused before admitting it again as new on edit', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    for (const text of ['q0', 'q1', 'q2']) await send(hook.result, text);
    refuseNextSend();
    await send(hook.result, 'q3');
    const { result } = hook;
    mockFetchTurns.mockResolvedValue(turnsThrough(2));
    const last = result.current.messages.filter((m) => m.role === 'user')[3];
    await act(async () => {
      await result.current.handleEditMessage(last.id, 'q3 again');
    });
    expect(result.current.messageError).toBeNull();
    expect(lastFork()).toEqual({ checkpointId: null, forkFromTurn: null });
    expect(mockSendStream.mock.lastCall?.[0]).toBe('q3 again');
    const users = result.current.messages.filter((m) => m.role === 'user') as UserMessage[];
    expect(users.map((m) => [m.content, m.turnIndex, !!m.unadmitted])).toEqual([
      ['q0', 0, false], ['q1', 1, false], ['q2', 2, false], ['q3 again', 3, false],
    ]);
  });

  it.each([
    ['lost on the way', () => mockSendStream.mockRejectedValueOnce(new TypeError('Failed to fetch'))],
    ['refused at capacity', () => mockSendStream.mockImplementationOnce(
      async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: StreamCallback }) => {
        onEvent({ event: 'error', thread_id: 'thread-1', error: 'at capacity', type: 'workflow_error', error_type: 'capacity_limit', code: 'writer_capacity' });
        return { disconnected: false };
      },
    )],
  ])('retries a send %s by sending it again in its place, under its first key', async (_case, fail) => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    for (const text of ['q0', 'q1']) await send(hook.result, text);
    fail();
    await act(async () => {
      await hook.result.current.handleSendMessage('q2', [{ type: 'image', name: 'chart.png' }]);
    });
    const { result } = hook;
    const failedKey = (mockSendStream.mock.lastCall?.[3] as { requestKey: string }).requestKey;
    expect((result.current.messages.at(-1) as AssistantMessage).unadmitted).toBe(true);

    await act(async () => {
      await result.current.handleRetry();
    });

    expect(mockRetryStream).not.toHaveBeenCalled();
    const resent = mockSendStream.mock.lastCall!;
    expect(resent[0]).toBe('q2');
    expect(resent[3]).toMatchObject({ requestKey: failedKey, additionalContext: [{ type: 'image', name: 'chart.png' }], forkFromTurn: null });
    const users = result.current.messages.filter((m) => m.role === 'user') as UserMessage[];
    expect(users.map((m) => [m.content, m.turnIndex, !!m.unadmitted])).toEqual([['q0', 0, false], ['q1', 1, false], ['q2', 2, false]]);
    expect(result.current.messages.filter((m) => m.role === 'assistant')).toHaveLength(3);
    expect(result.current.messages.some((m) => m.role !== 'notification' && m.unadmitted)).toBe(false);
  });

  it('says in the reply of a send that lost the run slot that another reply was running, and its Retry sends it again', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    await send(hook.result, 'q0');
    mockSendStream.mockImplementationOnce(
      async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: StreamCallback }) => {
        onEvent({
          event: 'error', thread_id: 'thread-1', type: 'workflow_error', error_type: 'admission_conflict', code: 'running',
          error: 'The turn is still running. Wait a moment, then retry — or use /reconnect to continue streaming, or /cancel to stop it.',
        });
        return { disconnected: false };
      },
    );
    await send(hook.result, 'q1');
    const { result } = hook;
    expect(result.current.messages.at(-1)).toMatchObject({
      error: true,
      unadmitted: true,
      content: '',
      structuredError: { message: 'chat.busyRunningSend', kind: 'notice' },
    });
    expect(result.current.messageError).toBeNull();

    await act(async () => {
      await result.current.handleRetry();
    });

    expect(mockRetryStream).not.toHaveBeenCalled();
    expect(mockSendStream.mock.lastCall?.[0]).toBe('q1');
  });

  it('adopts the run when a copy of the send it retries did arrive', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    await send(hook.result, 'q0');
    mockSendStream.mockRejectedValueOnce(new TypeError('Failed to fetch'));
    await send(hook.result, 'q1');
    const { result } = hook;
    mockSendStream.mockRejectedValueOnce(Object.assign(new Error('duplicate'), {
      status: 409,
      errorInfo: { code: 'duplicate_request', run_id: 'run-first-copy', thread_id: 'thread-1' },
    }));
    const replays = mockReplay.mock.calls.length;

    await act(async () => {
      await result.current.handleRetry();
    });

    expect(mockRetryStream).not.toHaveBeenCalled();
    expect(mockSendStream.mock.lastCall?.[0]).toBe('q1');
    expect(result.current.messageError).toBeNull();
    // The adopted run's reconnect finds it settled and reloads the thread,
    // whose replay replaces the bubbles this send drew.
    await waitFor(() => expect(mockReplay.mock.calls.length).toBeGreaterThan(replays));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(result.current.messages.some((m) => m.role !== 'notification' && m.unadmitted)).toBe(false);
  });

  it('retries at once, by its id, the failed run a copy of the send it retries started', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    await send(hook.result, 'q0');
    mockSendStream.mockRejectedValueOnce(new TypeError('Failed to fetch'));
    await act(async () => {
      await hook.result.current.handleSendMessage('q1', [{ type: 'image', name: 'chart.png' }]);
    });
    const { result } = hook;
    const sendKey = (mockSendStream.mock.lastCall?.[3] as { requestKey: string }).requestKey;
    mockSendStream.mockRejectedValueOnce(Object.assign(new Error('duplicate'), {
      status: 409,
      errorInfo: { code: 'duplicate_request', run_id: 'run-first-copy', run_status: 'error', thread_id: 'thread-1' },
    }));

    await act(async () => {
      await result.current.handleRetry();
    });

    await waitFor(() => expect(mockRetryStream).toHaveBeenCalledTimes(1));
    const retried = mockRetryStream.mock.lastCall!;
    // The run, its context, and a key of its own: the send's names the copy.
    expect(retried[9]).toBe('run-first-copy');
    expect(retried[10]).toEqual([{ type: 'image', name: 'chart.png' }]);
    expect(retried[8]).not.toBe(sendKey);
    expect(result.current.messageError).toBeNull();
    const users = result.current.messages.filter((m) => m.role === 'user') as UserMessage[];
    expect(users.map((m) => [m.content, m.turnIndex, !!m.unadmitted])).toEqual([['q0', 0, false], ['q1', 1, false]]);
    expect(result.current.messages.filter((m) => m.role === 'assistant')).toHaveLength(2);
  });

  it('adopts a stopped run a copy of the send it retries started, which only a regenerate reruns', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    mockSendStream.mockRejectedValueOnce(new TypeError('Failed to fetch'));
    await send(hook.result, 'q0');
    mockSendStream.mockRejectedValueOnce(Object.assign(new Error('duplicate'), {
      status: 409,
      errorInfo: { code: 'duplicate_request', run_id: 'run-first-copy', run_status: 'cancelled', thread_id: 'thread-1' },
    }));

    await act(async () => {
      await hook.result.current.handleRetry();
    });

    expect(mockRetryStream).not.toHaveBeenCalled();
    expect(hook.result.current.messageError).toBeNull();
    // The server holds the whole turn, so the reload replaces the bubbles the
    // send drew rather than keeping them below the turn it renders.
    await waitFor(() => expect(mockReplay.mock.calls.length).toBeGreaterThan(1));
    expect(hook.result.current.messages.filter((m) => !m.isHistory)).toEqual([]);
  });

  it("says a send lost on the way failed in its reply, never in the browser's words", async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    mockSendStream.mockRejectedValueOnce(new TypeError('Failed to fetch'));
    await send(hook.result, 'q0');
    refuseNextSend();
    await send(hook.result, 'q1');

    const { result } = hook;
    expect(result.current.messageError).toBeNull();
    const replies = result.current.messages.filter((m) => m.role === 'assistant') as AssistantMessage[];
    expect(replies.map((m) => [m.structuredError?.message, !!m.error])).toEqual([
      ['Failed to send message. Please try again.', true],
      ['Failed to send message. Please try again.', true],
    ]);
  });

  it.each([
    ['in its own words', { status: 409, errorInfo: { code: 'turn_conflict', message: 'Refresh the thread and resend.' } }, 'Refresh the thread and resend.'],
    ['as busy, in the view\'s words', { status: 409, errorInfo: { code: 'running', message: 'A turn is still running.' } },
      { message: 'chat.busyRunning', kind: 'notice' }],
    ['with the action it offers', { status: 402, errorInfo: { message: 'Add a key.', link: { url: '/settings', label: 'Settings' } } },
      { message: 'Add a key.', links: [{ url: '/settings', label: 'Settings' }] }],
    ['as too large, in the words the transport gave it', { status: 413, message: 'Files too large. Try smaller files or fewer attachments.' },
      'Files too large. Try smaller files or fewer attachments.'],
  ])('keeps the banner of a send the server refused %s', async (_case, refusal, banner) => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    mockSendStream.mockRejectedValueOnce(Object.assign(new Error('refused'), refusal));
    await send(hook.result, 'q0');

    expect(hook.result.current.messageError).toEqual(banner);
  });

  it('numbers a send after a refused one as the server does, so an edit of it replaces it', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    for (const text of ['q0', 'q1', 'q2']) await send(hook.result, text);
    refuseNextSend();
    await send(hook.result, 'q3');
    await send(hook.result, 'q4');
    const { result } = hook;
    const users = result.current.messages.filter((m) => m.role === 'user') as UserMessage[];
    expect(users.map((m) => [m.content, m.turnIndex, !!m.unadmitted])).toEqual([
      ['q0', 0, false], ['q1', 1, false], ['q2', 2, false], ['q3', 3, true], ['q4', 3, false],
    ]);
    mockFetchTurns.mockResolvedValue(turnsThrough(3));
    await act(async () => {
      await result.current.handleEditMessage(users[4].id, 'q4 changed');
    });
    expect(result.current.messageError).toBeNull();
    expect(lastFork()).toEqual({ checkpointId: 'cp-end-2', forkFromTurn: 3 });
  });

  it('keeps a refused message and its reply through a reload a later turn asks for', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    for (const text of ['q0', 'q1', 'q2']) await send(hook.result, text);
    refuseNextSend();
    await send(hook.result, 'q3');
    // Another tab added turn 3 meanwhile, so the server numbers q4 as turn 4,
    // which this view did not count to, and it reloads once q4 settles.
    mockSendStream.mockImplementationOnce(
      async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: StreamCallback }) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        onEvent({ event: 'metadata', thread_id: 'thread-1', run_id: 'run-q4', turn_index: 4 });
        return { disconnected: false };
      },
    );
    const replays = mockReplay.mock.calls.length;
    await send(hook.result, 'q4');
    const { result } = hook;
    await waitFor(() => expect(mockReplay.mock.calls.length).toBeGreaterThan(replays));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));

    // The replay rebuilds every turn the server holds, and q3 is in none.
    const kept = result.current.messages.filter((m) => m.role !== 'notification' && m.unadmitted);
    expect(kept.map((m) => [m.role, m.role === 'user' ? m.content : (m as AssistantMessage).error])).toEqual([
      ['user', 'q3'],
      ['assistant', true],
    ]);
  });

  it('edits a refused message with a stored one after it in the place of that one, replacing what follows', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    for (const text of ['q0', 'q1', 'q2']) await send(hook.result, text);
    refuseNextSend();
    await send(hook.result, 'q3');
    await send(hook.result, 'q4');
    const { result } = hook;
    mockFetchTurns.mockResolvedValue(turnsThrough(3));
    const refused = result.current.messages.filter((m) => m.role === 'user')[3];
    await act(async () => {
      await result.current.handleEditMessage(refused.id, 'q3 changed');
    });
    expect(result.current.messageError).toBeNull();
    expect(lastFork()).toEqual({ checkpointId: 'cp-end-2', forkFromTurn: 3 });
    expect(mockSendStream.mock.lastCall?.[0]).toBe('q3 changed');
    const users = result.current.messages.filter((m) => m.role === 'user') as UserMessage[];
    expect(users.map((m) => [m.content, m.turnIndex, !!m.unadmitted])).toEqual([
      ['q0', 0, false], ['q1', 1, false], ['q2', 2, false], ['q3 changed', 3, false],
    ]);
  });

  it("takes an edit of a message the server's thread does not hold as the chat having changed", async () => {
    const { result } = await renderFourTurns();
    mockFetchTurns.mockResolvedValue({ turns: [], retry_checkpoint_id: null });
    const sends = mockSendStream.mock.calls.length;
    const replays = mockReplay.mock.calls.length;
    const last = result.current.messages.filter((m) => m.role === 'user')[3];
    await act(async () => {
      await result.current.handleEditMessage(last.id, 'q3 again');
    });
    expect(mockSendStream.mock.calls.length).toBe(sends);
    expect(result.current.messageError).toEqual({ message: 'chat.forkStaleEdit', kind: 'notice' });
    // The thread reloads to show what the server holds, and the edit stays.
    await waitFor(() => expect(mockReplay.mock.calls.length).toBe(replays + 1));
    expect(result.current.editDrafts.get({ turnIndex: 3, original: 'q3' })).toBe('q3 again');
  });

  it('refuses to regenerate a turn with no checkpoint whose message carried more than its text', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    await send(hook.result, 'q0');
    await send(hook.result, 'q1', [{ name: 'chart.png', type: 'image/png', dataUrl: 'data:image/png;base64,AA==' }]);
    await send(hook.result, 'q2');
    const { result } = hook;
    const sends = mockSendStream.mock.calls.length;
    const assistantId = result.current.messages.filter((m) => m.role === 'assistant')[1].id;
    await act(async () => {
      await result.current.handleRegenerate(assistantId);
    });
    expect(mockSendStream.mock.calls.length).toBe(sends);
    expect(result.current.messageError).toBe('Failed to process request');
    expect(result.current.messages.filter((m) => m.role === 'assistant')).toHaveLength(3);
  });

  it('regenerates a turn with no checkpoint with what its Retry holds', async () => {
    const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
    await settleMountLoad();
    await send(hook.result, 'q0');
    // The server started q1's run, which failed before its first checkpoint,
    // and asks for the send again since it cannot rerun the image from what
    // it stored. It stopped asking on its own.
    const context = [{ type: 'image', data: 'AA==' }];
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: StreamCallback }) => {
      onEvent({ event: 'thread_id', thread_id: 'thread-1' });
      onEvent({ event: 'retry', thread_id: 'thread-1', auto_retry: false, retry_count: 1, max_retries: 3, recovery: 'resend', run_id: 'run-q1' });
      return { disconnected: false };
    });
    await act(async () => {
      await hook.result.current.handleSendMessage('q1', context, [{ name: 'chart.png', type: 'image/png' }]);
    });
    const { result } = hook;
    const failed = result.current.messages.filter((m) => m.role === 'assistant')[1];
    await act(async () => {
      await result.current.handleRegenerate(failed.id);
    });
    expect(result.current.messageError).toBeNull();
    expect(mockSendStream.mock.lastCall?.[0]).toBe('q1');
    const options = mockSendStream.mock.lastCall?.[3] as { additionalContext: unknown; forkFromTurn: unknown };
    expect(options.additionalContext).toEqual(context);
    expect(options.forkFromTurn).toBeNull();
    expect(result.current.messages.filter((m) => m.role === 'user').map((m) => m.content)).toEqual(['q0', 'q1']);
  });
});
