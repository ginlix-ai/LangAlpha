/**
 * Tests for the "surface model retry/fallback to the user" feature in
 * useChatMessages.
 *
 * Two SSE events drive it:
 *   - model_retry   → transient `modelStatus` pill (kind:'retrying'). Not
 *                     persisted; ignored for subagent-attributed events.
 *   - model_fallback → transient `modelStatus` pill (kind:'fallback'), a
 *                     persistent transcript notification segment (with
 *                     expandable error detail) on the current assistant
 *                     message, AND the `fallbackSuggestion` switch-to-working-
 *                     model pill state (which unlike modelStatus survives
 *                     stream end; cleared on error, new turns, and dismiss).
 *
 * The transient status clears on the first content event, on error, on stream
 * end, and on stop. The `error` event now also maps `model` /
 * `attempted_models` into the assistant's structuredError.
 *
 * Real stream/history handlers run (not mocked) so `isSubagentEvent` classifies
 * correctly and the notification actually lands on the message. The hung-stream
 * pattern (see compactionQueue.test.ts) keeps a turn "running" so we can observe
 * the transient status before stream-end cleanup clears it.
 *
 * Neutral placeholder model names only — no production data.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('@/components/ui/use-toast', () => ({
  toast: vi.fn(),
  useToast: () => ({ toast: vi.fn() }),
}));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', () => ({
  sendChatMessageStream: vi.fn(),
  sendHitlResponse: vi.fn(),
  cancelWorkflow: vi.fn().mockResolvedValue({ success: true }),
  replayThreadHistory: vi.fn().mockResolvedValue(undefined),
  getWorkflowStatus: vi.fn().mockResolvedValue({ can_reconnect: false, status: 'completed' }),
  reconnectToWorkflowStream: vi.fn(),
  streamSubagentTaskEvents: vi.fn(),
  fetchThreadTurns: vi.fn().mockResolvedValue({ turns: [], retry_checkpoint_id: null }),
  submitFeedback: vi.fn(),
  removeFeedback: vi.fn(),
  getThreadFeedback: vi.fn().mockResolvedValue([]),
  watchThread: vi.fn(),
}));

import { sendChatMessageStream, sendHitlResponse, replayThreadHistory } from '../../utils/api';
import { getStoredThreadId } from '../utils/threadStorage';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;
const mockSendHitl = sendHitlResponse as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockGetStoredThreadId = getStoredThreadId as Mock;

type OnEvent = (e: Record<string, unknown>) => void;
type HookResult = { current: ReturnType<typeof useChatMessages> };

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (v: T) => void;
}
function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

/**
 * Emits `events` synchronously, then hangs so the turn stays "running"
 * (isLoading true, no stream-end cleanup) and transient status is observable.
 * Returns the deferred so the test can release the stream at the end.
 */
function mockHangingStreamWith(events: Array<Record<string, unknown>>): Deferred<{ disconnected: boolean }> {
  const hang = deferred<{ disconnected: boolean }>();
  mockSendStream.mockImplementation(
    async (
      _msg: string,
      _ws: string,
      _tid: string | null,
      _hist: unknown[],
      _plan: boolean,
      onEvent: OnEvent,
    ) => {
      for (const e of events) onEvent(e);
      return hang.promise;
    },
  );
  return hang;
}

/**
 * Starts a send against a hung stream and waits until the turn is streaming.
 * Returns the pending send promise wrapped in an object — NOT bare — because an
 * async helper that `return`s a promise adopts it, which would deadlock the
 * caller against the (deliberately) hung stream.
 */
async function startHungSend(result: HookResult): Promise<{ send: Promise<unknown> }> {
  let send: Promise<unknown> = Promise.resolve();
  await act(async () => {
    send = result.current.handleSendMessage('hello', false);
    send.catch(() => undefined);
    await Promise.resolve();
  });
  await waitFor(() => expect(result.current.isLoading).toBe(true));
  return { send };
}

/** Release a hung stream and let the send settle. */
async function releaseHungSend(hang: Deferred<{ disconnected: boolean }>, send: Promise<unknown>) {
  hang.resolve({ disconnected: false });
  await act(async () => {
    await send.catch(() => undefined);
  });
}

function assistantOf(result: HookResult): AssistantMessage | undefined {
  return result.current.messages.find((m): m is AssistantMessage => m.role === 'assistant');
}

describe('useChatMessages — model retry/fallback resilience', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockResolvedValue(undefined);
    mockGetStoredThreadId.mockReturnValue(null);
  });

  it('model_retry sets the retrying modelStatus (attempt/maxRetries verbatim)', async () => {
    const hang = mockHangingStreamWith([
      { event: 'model_retry', agent: 'main', model: 'model-alpha', attempt: 1, max_retries: 3 },
    ]);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-mr'));
    const { send } = await startHungSend(result);

    await waitFor(() =>
      expect(result.current.modelStatus).toEqual({
        kind: 'retrying',
        model: 'model-alpha',
        attempt: 1,
        maxRetries: 3,
      }),
    );

    await releaseHungSend(hang, send);
  });

  it('model_fallback sets the fallback modelStatus AND appends a transcript notification', async () => {
    const hang = mockHangingStreamWith([
      {
        event: 'model_fallback',
        agent: 'main',
        from_model: 'model-alpha',
        to_model: 'model-beta',
        error: 'placeholder upstream error',
        status_code: 503,
        attempts_on_from: 4,
      },
    ]);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-mf'));
    const { send } = await startHungSend(result);

    await waitFor(() =>
      expect(result.current.modelStatus).toEqual({
        kind: 'fallback',
        fromModel: 'model-alpha',
        toModel: 'model-beta',
      }),
    );

    // Persistent notification content-segment on the current assistant message,
    // carrying the expandable error detail.
    const assistant = assistantOf(result)!;
    const notif = (assistant.contentSegments || []).find((s) => s.type === 'notification') as
      | import('@/types/chat').NotificationSegment
      | undefined;
    expect(notif).toBeDefined();
    expect(notif!.content).toBe('chat.modelFallbackNotification');
    expect(notif!.detail).toContain('placeholder upstream error');
    expect(notif!.detail).toContain('HTTP 503');
    expect(notif!.detailKind).toBe('error');

    // Switch-to-working-model suggestion (the pill above the input).
    expect(result.current.fallbackSuggestion).toEqual({
      fromModel: 'model-alpha',
      toModel: 'model-beta',
    });

    await releaseHungSend(hang, send);

    // Unlike the transient modelStatus pill, the suggestion SURVIVES stream
    // end — the user acts on it after reading the answer.
    expect(result.current.modelStatus).toBeNull();
    expect(result.current.fallbackSuggestion).toEqual({
      fromModel: 'model-alpha',
      toModel: 'model-beta',
    });
  });

  it('redelivered model_fallback (same _eventId) does not duplicate the divider', async () => {
    const fallbackEvent = {
      event: 'model_fallback',
      agent: 'main',
      _eventId: 41,
      from_model: 'model-alpha',
      to_model: 'model-beta',
      error: 'placeholder upstream error',
      status_code: 503,
      attempts_on_from: 4,
    };
    // A reconnect can re-send an already-applied event from the Redis buffer.
    const hang = mockHangingStreamWith([fallbackEvent, { ...fallbackEvent }]);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-mf-dup'));
    const { send } = await startHungSend(result);

    await waitFor(() => expect(result.current.modelStatus).not.toBeNull());
    const assistant = assistantOf(result)!;
    const notifs = (assistant.contentSegments || []).filter((s) => s.type === 'notification');
    expect(notifs).toHaveLength(1);

    await releaseHungSend(hang, send);
  });

  it('chained fallbacks keep the configured from-model and the latest to-model', async () => {
    const hang = mockHangingStreamWith([
      { event: 'model_fallback', agent: 'main', from_model: 'model-alpha', to_model: 'model-beta', from_is_primary: true },
      { event: 'model_fallback', agent: 'main', from_model: 'model-beta', to_model: 'model-gamma', from_is_primary: false },
    ]);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-chain'));
    const { send } = await startHungSend(result);

    await waitFor(() =>
      expect(result.current.fallbackSuggestion).toEqual({
        fromModel: 'model-alpha',
        toModel: 'model-gamma',
      }),
    );

    await releaseHungSend(hang, send);
  });

  it('clearFallbackSuggestion dismisses the suggestion', async () => {
    const hang = mockHangingStreamWith([
      { event: 'model_fallback', agent: 'main', from_model: 'model-alpha', to_model: 'model-beta' },
    ]);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-dismiss'));
    const { send } = await startHungSend(result);

    await waitFor(() => expect(result.current.fallbackSuggestion).not.toBeNull());
    await act(async () => {
      result.current.clearFallbackSuggestion();
    });
    expect(result.current.fallbackSuggestion).toBeNull();

    await releaseHungSend(hang, send);
  });

  it('ignores subagent-attributed (agent: "task:...") resilience events', async () => {
    const hang = mockHangingStreamWith([
      { event: 'model_retry', agent: 'task:1', model: 'model-alpha', attempt: 1, max_retries: 3 },
      { event: 'model_fallback', agent: 'task:1', from_model: 'model-alpha', to_model: 'model-beta' },
    ]);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-sub'));
    const { send } = await startHungSend(result);

    // Give any (erroneous) state update a chance to commit, then assert none did.
    await act(async () => {
      await Promise.resolve();
    });
    expect(result.current.modelStatus).toBeNull();
    const assistant = assistantOf(result);
    expect((assistant?.contentSegments || []).some((s) => s.type === 'notification')).toBe(false);

    await releaseHungSend(hang, send);
  });

  it('clears modelStatus on the first message_chunk content event', async () => {
    const hang = mockHangingStreamWith([
      { event: 'model_retry', agent: 'main', model: 'model-alpha', attempt: 0, max_retries: 2 },
      { event: 'message_chunk', agent: 'main', content_type: 'text', content: 'hi there' },
    ]);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-clear-chunk'));
    const { send } = await startHungSend(result);

    await waitFor(() => expect(result.current.modelStatus).toBeNull());
    // The content actually rendered (real handlers ran).
    await waitFor(() => expect(assistantOf(result)?.content).toContain('hi there'));

    await releaseHungSend(hang, send);
  });

  it('HITL resume clears a lingering fallbackSuggestion (new-run boundary)', async () => {
    // A fallback fires, then the turn interrupts for approval; the stream
    // ends there (matching the real interrupt flow).
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        _hist: unknown[],
        _plan: boolean,
        onEvent: OnEvent,
      ) => {
        onEvent({
          event: 'model_fallback',
          agent: 'main',
          from_model: 'model-alpha',
          to_model: 'model-beta',
        });
        onEvent({
          event: 'interrupt',
          interrupt_id: 'plan-1',
          action_requests: [{ name: 'SubmitPlan', description: 'Step 1.' }],
        });
        return { disconnected: false };
      },
    );
    const { result } = renderHookWithProviders(() => useChatMessages('ws-hitl'));
    await act(async () => {
      await result.current.handleSendMessage('make a plan', false);
    });
    await waitFor(() => expect(result.current.fallbackSuggestion).not.toBeNull());
    await waitFor(() => expect(result.current.pendingInterrupt).not.toBeNull());

    // Approving opens a fresh backend run whose model calls start from the
    // primary again — the pre-interrupt suggestion must not survive to sit
    // under the resumed turn's answer (a re-fired model_fallback re-sets it).
    mockSendHitl.mockResolvedValue({ disconnected: false, aborted: false });
    await act(async () => {
      result.current.handleApproveInterrupt();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalled());
    expect(result.current.fallbackSuggestion).toBeNull();
  });

  it('thread switch clears a mid-retry modelStatus (no stale pill on thread B)', async () => {
    const hang = mockHangingStreamWith([
      { event: 'model_retry', agent: 'main', model: 'model-alpha', attempt: 2, max_retries: 3 },
    ]);
    let threadId = 'thread-a';
    const { result, rerender } = renderHookWithProviders(() =>
      useChatMessages('ws-switch', threadId),
    );
    const { send } = await startHungSend(result);
    await waitFor(() => expect(result.current.modelStatus).not.toBeNull());

    threadId = 'thread-b';
    rerender();

    await waitFor(() => expect(result.current.modelStatus).toBeNull());
    await releaseHungSend(hang, send);
  });

  it('clears modelStatus on error and maps model + attempted_models into structuredError', async () => {
    const hang = mockHangingStreamWith([
      { event: 'model_retry', agent: 'main', model: 'model-alpha', attempt: 1, max_retries: 3 },
      { event: 'model_fallback', agent: 'main', from_model: 'model-alpha', to_model: 'model-beta' },
      {
        event: 'error',
        agent: 'main',
        error: 'Error code: 500 - upstream unavailable',
        error_kind: 'upstream',
        status_code: 500,
        model: 'model-alpha',
        attempted_models: [
          { model: 'model-alpha', error: '500 upstream unavailable', status_code: 500, attempts: 3 },
          { model: 'model-beta', error: 'timed out', status_code: null, attempts: 1 },
        ],
      },
    ]);
    const { result } = renderHookWithProviders(() => useChatMessages('ws-err'));
    const { send } = await startHungSend(result);

    // The error branch cleared the pill AND the switch suggestion (the
    // fallback model didn't save the turn either). Stream is still hung, so
    // cleanup hasn't run.
    await waitFor(() => expect(result.current.modelStatus).toBeNull());
    expect(result.current.fallbackSuggestion).toBeNull();

    await waitFor(() => {
      const assistant = assistantOf(result) as
        | (AssistantMessage & { structuredError?: import('@/utils/rateLimitError').StructuredError })
        | undefined;
      expect(assistant?.structuredError?.kind).toBe('upstream');
      expect(assistant?.structuredError?.model).toBe('model-alpha');
      expect(assistant?.structuredError?.attemptedModels).toEqual([
        { model: 'model-alpha', error: '500 upstream unavailable', statusCode: 500, attempts: 3 },
        { model: 'model-beta', error: 'timed out', statusCode: null, attempts: 1 },
      ]);
    });

    await releaseHungSend(hang, send);
  });

  // ---------------------------------------------------------------------------
  // History replay: the persisted model_fallback re-creates the notification on
  // the correct turn's assistant message (no transient pill on reload).
  // ---------------------------------------------------------------------------
  it('replays a persisted model_fallback as a transcript notification on reload', async () => {
    mockGetStoredThreadId.mockReturnValue('thread-mf-history');
    mockReplay.mockImplementation(async (_threadId: string, onEvent: OnEvent) => {
      onEvent({
        event: 'user_message',
        role: 'user',
        content: 'historical question',
        turn_index: 0,
        timestamp: '2024-01-01T00:00:00Z',
      });
      onEvent({
        event: 'model_fallback',
        agent: 'main',
        from_model: 'model-alpha',
        to_model: 'model-beta',
        error: 'placeholder persisted error',
        status_code: 429,
        attempts_on_from: 4,
        turn_index: 0,
      });
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-mf-history'));

    await waitFor(() => {
      expect(mockReplay).toHaveBeenCalled();
      const assistant = result.current.messages.find(
        (m): m is AssistantMessage => m.role === 'assistant' && m.id === 'history-assistant-0',
      );
      expect(assistant).toBeDefined();
      const notif = (assistant!.contentSegments || []).find((s) => s.type === 'notification') as
        | import('@/types/chat').NotificationSegment
        | undefined;
      expect(notif).toBeDefined();
      expect(notif!.content).toBe('chat.modelFallbackNotification');
      // The persisted event replays with the same expandable detail.
      expect(notif!.detail).toContain('placeholder persisted error');
      expect(notif!.detail).toContain('HTTP 429');
      expect(notif!.detailKind).toBe('error');
    });

    // No transient pill on a pure reload — but the switch suggestion IS
    // restored (the config problem persists across reloads).
    expect(result.current.modelStatus).toBeNull();
    expect(result.current.fallbackSuggestion).toEqual({
      fromModel: 'model-alpha',
      toModel: 'model-beta',
    });
  });

  it('replay: a later turn without fallback clears the suggestion (user_message boundary)', async () => {
    mockGetStoredThreadId.mockReturnValue('thread-mf-clean-after');
    mockReplay.mockImplementation(async (_threadId: string, onEvent: OnEvent) => {
      onEvent({ event: 'user_message', role: 'user', content: 'first question', turn_index: 0, timestamp: '2024-01-01T00:00:00Z' });
      onEvent({ event: 'model_fallback', agent: 'main', from_model: 'model-alpha', to_model: 'model-beta', turn_index: 0 });
      onEvent({ event: 'user_message', role: 'user', content: 'second question', turn_index: 1, timestamp: '2024-01-01T00:01:00Z' });
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-mf-clean-after'));

    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await waitFor(() =>
      expect(result.current.messages.some((m) => m.role === 'user')).toBe(true),
    );
    expect(result.current.fallbackSuggestion).toBeNull();
  });

  it('replay: an errored turn replays its error event and clears the suggestion', async () => {
    mockGetStoredThreadId.mockReturnValue('thread-mf-errored');
    mockReplay.mockImplementation(async (_threadId: string, onEvent: OnEvent) => {
      onEvent({ event: 'user_message', role: 'user', content: 'doomed question', turn_index: 0, timestamp: '2024-01-01T00:00:00Z' });
      onEvent({ event: 'model_fallback', agent: 'main', from_model: 'model-alpha', to_model: 'model-beta', turn_index: 0 });
      onEvent({ event: 'error', agent: 'main', error: 'placeholder total failure', error_kind: 'upstream', turn_index: 0 });
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-mf-errored'));

    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await waitFor(() =>
      expect(result.current.messages.some((m) => m.role === 'user')).toBe(true),
    );
    expect(result.current.fallbackSuggestion).toBeNull();
  });
});
