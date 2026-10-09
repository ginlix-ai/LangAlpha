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
  fetchThreadTurns,
  replayThreadHistory,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage, UserMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;
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
  // Turn 1's run died before its first checkpoint: it has rows and bubbles but
  // no /turns entry, so turns 2 and 3 sit one place below their numbers.
  const TURNS = {
    turns: [
      { turn_index: 0, edit_checkpoint_id: null, regenerate_checkpoint_id: 'cp-in-0' },
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

  it('refuses to regenerate a turn with no checkpoint', async () => {
    const { result } = await renderFourTurns();
    const sends = mockSendStream.mock.calls.length;
    const assistantId = result.current.messages.filter((m) => m.role === 'assistant')[1].id;
    await act(async () => {
      await result.current.handleRegenerate(assistantId);
    });
    expect(result.current.messageError).toBe('Unable to regenerate: checkpoint data unavailable');
    expect(mockSendStream.mock.calls.length).toBe(sends);
  });
});
