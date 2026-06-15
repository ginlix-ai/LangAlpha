/**
 * Tests for the hard-stop flow (stopWorkflow) in useChatMessages.
 *
 * stopWorkflow() turns the chat "stop" control into an immediate,
 * state-preserving hard cancel:
 *  (a) aborts the main stream reader (the signal threaded into
 *      sendChatMessageStream) so the stop feels instant;
 *  (b) finalizes the open assistant message through the real handler pipeline —
 *      the open reasoning block closes (reasoningComplete) and the message gets
 *      a `stopped` flag — and clears isLoading;
 *  (c) POSTs /cancel with ONE retry, then an error toast on failure;
 *  (d) is idempotent on a double-click (no duplicate synthetic events).
 *
 * An aborted stream is swallowed: no error banner, no double cleanup.
 *
 * We use the REAL streamEventHandlers so the reasoning-close + stopped flag are
 * observable on result.current.messages, but mock the api + toast modules.
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
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

const toastMock = vi.fn();
vi.mock('@/components/ui/use-toast', () => ({
  toast: (...args: unknown[]) => toastMock(...args),
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

import { sendChatMessageStream, cancelWorkflow, getWorkflowStatus, reconnectToWorkflowStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;
const mockCancel = cancelWorkflow as Mock;
const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;

type OnEvent = (e: Record<string, unknown>) => void;

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (v: T) => void;
}
function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => { resolve = r; });
  return { promise, resolve };
}

/** Find the open reasoning process across the assistant message. */
function findReasoning(msg: AssistantMessage | undefined) {
  if (!msg) return null;
  const procs = (msg.reasoningProcesses as unknown as Record<string, Record<string, unknown>>) || {};
  const keys = Object.keys(procs);
  return keys.length ? procs[keys[keys.length - 1]] : null;
}

describe('useChatMessages — stopWorkflow (hard stop)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCancel.mockResolvedValue({ success: true });
  });

  /**
   * Drive a send that opens a reasoning block, then leaves the stream hanging
   * so isLoading stays true and the reasoning block stays open.
   * Returns the captured signal + assistant message id + the hang deferred.
   */
  async function startHangingSendWithReasoning(
    result: { current: ReturnType<typeof useChatMessages> },
  ) {
    const hang = deferred<{ disconnected: boolean; aborted: boolean }>();
    let capturedSignal: AbortSignal | undefined;

    mockSendStream.mockImplementation(
      async (...args: unknown[]) => {
        const onEvent = args[5] as OnEvent;
        capturedSignal = args[17] as AbortSignal;
        // metadata first (latches run_id), then open a reasoning block.
        onEvent({ event: 'metadata', thread_id: 'th-stop', run_id: 'run-1' });
        onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'reasoning_signal', content: 'start' });
        onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'reasoning', content: 'thinking hard...' });
        return hang.promise;
      },
    );

    let send: Promise<unknown> = Promise.resolve();
    await act(async () => {
      send = result.current.handleSendMessage('analyze AAPL', false);
      await Promise.resolve();
      await Promise.resolve();
    });

    await waitFor(() => expect(result.current.isLoading).toBe(true));

    return { hang, getSignal: () => capturedSignal, send };
  }

  it('aborts the controller, clears isLoading, and closes the open reasoning block synchronously', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-stop', 'th-stop'));
    const { hang, getSignal, send } = await startHangingSendWithReasoning(result);

    // Sanity: the reasoning block is open before stop.
    const before = result.current.messages.find((m) => m.role === 'assistant') as AssistantMessage;
    expect(findReasoning(before)?.reasoningComplete).toBeFalsy();
    expect(getSignal()?.aborted).toBe(false);

    await act(async () => {
      await result.current.stopWorkflow();
    });

    // (a) the reader's signal was aborted.
    expect(getSignal()?.aborted).toBe(true);
    // (b) loading cleared.
    expect(result.current.isLoading).toBe(false);
    // (b) open reasoning block closed + message stamped stopped.
    const after = result.current.messages.find((m) => m.role === 'assistant') as AssistantMessage;
    expect(findReasoning(after)?.reasoningComplete).toBe(true);
    expect(findReasoning(after)?.isReasoning).toBe(false);
    expect((after as { stopped?: boolean }).stopped).toBe(true);
    expect(after.isStreaming).toBe(false);
    // (c) cancel POSTed once (success → no retry, no toast).
    expect(mockCancel).toHaveBeenCalledTimes(1);
    expect(mockCancel).toHaveBeenCalledWith('th-stop');
    expect(toastMock).not.toHaveBeenCalled();
    // No error banner from the aborted stream.
    expect(result.current.messageError).toBeNull();

    // Resolve the hanging send as aborted; the finally must not re-toggle state.
    hang.resolve({ disconnected: false, aborted: true });
    await act(async () => { await send.catch(() => undefined); });
    expect(result.current.isLoading).toBe(false);
    expect(result.current.messageError).toBeNull();
  });

  it('clears the in-flight tool-call "generating" row on stop (no lingering shimmer)', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-stop', 'th-stop'));

    const hang = deferred<{ disconnected: boolean; aborted: boolean }>();
    mockSendStream.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as OnEvent;
      onEvent({ event: 'metadata', thread_id: 'th-stop', run_id: 'run-1' });
      // An in-flight tool call: args still streaming, no tool_calls completion
      // and no result — this is what renders the "generating (~N chars)…" shimmer.
      onEvent({ event: 'tool_call_chunks', tool_call_chunks: [{ name: 'web_search', args: '{"query":"AAPL ' }] });
      return hang.promise;
    });

    let send: Promise<unknown> = Promise.resolve();
    await act(async () => {
      send = result.current.handleSendMessage('analyze AAPL', false);
      await Promise.resolve();
      await Promise.resolve();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));

    // Before stop: the preparing tool-call chunks are present (shimmer on).
    const before = result.current.messages.find((m) => m.role === 'assistant') as AssistantMessage;
    expect(Object.keys((before.pendingToolCallChunks as Record<string, unknown>) || {}).length).toBeGreaterThan(0);

    await act(async () => {
      await result.current.stopWorkflow();
    });

    // After stop: chunks cleared → preparingToolCall is null → no shimmer.
    const after = result.current.messages.find((m) => m.role === 'assistant') as AssistantMessage;
    expect(Object.keys((after.pendingToolCallChunks as Record<string, unknown>) || {})).toHaveLength(0);
    expect((after as { stopped?: boolean }).stopped).toBe(true);
    expect(after.isStreaming).toBe(false);

    hang.resolve({ disconnected: false, aborted: true });
    await act(async () => { await send.catch(() => undefined); });
  });

  it('folds an in-progress tool-call process on stop (no row left spinning)', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-stop', 'th-stop'));

    const hang = deferred<{ disconnected: boolean; aborted: boolean }>();
    mockSendStream.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[5] as OnEvent;
      onEvent({ event: 'metadata', thread_id: 'th-stop', run_id: 'run-1' });
      // A started tool call with no result yet → toolCallProcesses entry with
      // isInProgress:true. Always-live tools (TaskOutput/WebFetch) render their
      // spinner off this flag regardless of isStreaming, so it must be folded.
      onEvent({
        event: 'tool_calls',
        tool_calls: [{ id: 'tc-1', name: 'TaskOutput', args: '{}' }],
        _eventId: 1,
      });
      return hang.promise;
    });

    let send: Promise<unknown> = Promise.resolve();
    await act(async () => {
      send = result.current.handleSendMessage('check on the subagent', false);
      await Promise.resolve();
      await Promise.resolve();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));

    // Before stop: the process is in progress.
    const before = result.current.messages.find((m) => m.role === 'assistant') as AssistantMessage;
    const beforeProc = before.toolCallProcesses['tc-1'];
    expect(beforeProc?.isInProgress).toBe(true);

    await act(async () => {
      await result.current.stopWorkflow();
    });

    // After stop: folded to complete so it stops spinning.
    const after = result.current.messages.find((m) => m.role === 'assistant') as AssistantMessage;
    const afterProc = after.toolCallProcesses['tc-1'];
    expect(afterProc?.isInProgress).toBe(false);
    expect(afterProc?.isComplete).toBe(true);

    hang.resolve({ disconnected: false, aborted: true });
    await act(async () => { await send.catch(() => undefined); });
  });

  it('double-click stop is idempotent (one cancel, no duplicate synthetic events)', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-stop', 'th-stop'));
    const { hang, send } = await startHangingSendWithReasoning(result);

    await act(async () => {
      await result.current.stopWorkflow();
      await result.current.stopWorkflow(); // second click no-ops
    });

    // Only one reasoning process exists (no duplicate synthetic close appended).
    const after = result.current.messages.find((m) => m.role === 'assistant') as AssistantMessage;
    const procCount = Object.keys((after.reasoningProcesses as Record<string, unknown>) || {}).length;
    expect(procCount).toBe(1);
    // cancel fired exactly once despite two stop calls.
    expect(mockCancel).toHaveBeenCalledTimes(1);

    hang.resolve({ disconnected: false, aborted: true });
    await act(async () => { await send.catch(() => undefined); });
  });

  it('cancel POST failing twice triggers one retry then an error toast', async () => {
    mockCancel
      .mockRejectedValueOnce(new Error('net'))
      .mockRejectedValueOnce(new Error('net again'));

    const { result } = renderHookWithProviders(() => useChatMessages('ws-stop', 'th-stop'));
    const { hang, send } = await startHangingSendWithReasoning(result);

    await act(async () => {
      await result.current.stopWorkflow();
    });

    // Two attempts (original + one retry), then a destructive toast.
    expect(mockCancel).toHaveBeenCalledTimes(2);
    expect(toastMock).toHaveBeenCalledTimes(1);
    expect(toastMock).toHaveBeenCalledWith(
      expect.objectContaining({ variant: 'destructive', description: 'chat.stopFailed' }),
    );
    // Stop still cleared loading even though cancel failed.
    expect(result.current.isLoading).toBe(false);

    hang.resolve({ disconnected: false, aborted: true });
    await act(async () => { await send.catch(() => undefined); });
  });

  it('an aborted stream is swallowed — no error banner, no double cleanup', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-stop', 'th-stop'));
    const { hang, send } = await startHangingSendWithReasoning(result);

    await act(async () => {
      await result.current.stopWorkflow();
    });

    // Simulate the real api returning the aborted marker for the hung send.
    hang.resolve({ disconnected: false, aborted: true });
    await act(async () => { await send.catch(() => undefined); });

    expect(result.current.messageError).toBeNull();
    expect(result.current.isLoading).toBe(false);
    // cancel still fired exactly once (the aborted-send finally did not re-run it).
    expect(mockCancel).toHaveBeenCalledTimes(1);
  });

  it('stop during a reconnect aborts the reconnect reader and skips its cleanup', async () => {
    // Mount-effect reconnect: status says reconnectable and the reconnect
    // stream hangs, so we can press stop while the reader is mid-reconnect.
    mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', active_tasks: [] });

    const hang = deferred<{ disconnected: boolean; aborted: boolean }>();
    let reconnectSignal: AbortSignal | undefined;
    mockReconnect.mockImplementation(async (...args: unknown[]) => {
      // reconnectToWorkflowStream(threadId, runId, lastEventId, onEvent, signal)
      reconnectSignal = args[4] as AbortSignal;
      const onEvent = args[3] as OnEvent;
      onEvent({ event: 'metadata', thread_id: 'th-stop', run_id: 'run-1' });
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'reasoning_signal', content: 'start' });
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'reasoning', content: 'resuming...' });
      return hang.promise;
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-stop', 'th-stop'));

    // Mount effect kicks off the reconnect.
    await waitFor(() => expect(result.current.isReconnecting).toBe(true));
    // The reconnect reader was handed a live, un-aborted signal (the fix:
    // without it, stopWorkflow's abort would be a no-op during reconnect).
    expect(reconnectSignal).toBeDefined();
    expect(reconnectSignal?.aborted).toBe(false);

    await act(async () => {
      await result.current.stopWorkflow();
    });

    // Stop aborted the reconnect reader and cleared loading.
    expect(reconnectSignal?.aborted).toBe(true);
    expect(result.current.isLoading).toBe(false);
    expect(mockCancel).toHaveBeenCalledTimes(1);

    // Resolve the hung reader as aborted; the reconnect finally must NOT re-run
    // cleanupAfterStreamEnd (wasStoppedRef guard) — no error banner, no re-toggle.
    hang.resolve({ disconnected: false, aborted: true });
    await act(async () => { await Promise.resolve(); await Promise.resolve(); });
    expect(result.current.messageError).toBeNull();
    expect(result.current.isLoading).toBe(false);
    expect(result.current.isReconnecting).toBe(false);
  });

  it('a reconnect after a prior stop runs to completion (resets the stale stop flag)', async () => {
    // Regression: stopWorkflow sets wasStoppedRef=true and it is only reset on a
    // new send/resume/steer — NOT on a reconnect. Switching to a live thread B
    // after stopping thread A drives reconnectToStream with a stale true flag,
    // which (before the fix) bailed the legitimate reconnect and left isLoading
    // stuck. reconnectToStream must reset the flag on entry.
    let ws = 'ws-a';
    let tid = 'th-a';
    // Thread A is not reconnectable; thread B is live.
    mockStatus.mockImplementation(async (t: string) =>
      t === 'th-b'
        ? { can_reconnect: true, status: 'running', active_tasks: [] }
        : { can_reconnect: false, status: 'completed' },
    );
    // Thread B's reconnect streams content and completes normally (not aborted).
    mockReconnect.mockImplementation(async (...args: unknown[]) => {
      const onEvent = args[3] as OnEvent;
      onEvent({ event: 'metadata', thread_id: 'th-b', run_id: 'run-b' });
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'resumed content' });
      return { disconnected: false, aborted: false };
    });

    const { result, rerender } = renderHookWithProviders(() => useChatMessages(ws, tid));

    // Stop on thread A with no active turn → sets wasStoppedRef=true (else branch).
    await act(async () => {
      await result.current.stopWorkflow();
    });
    expect(mockCancel).toHaveBeenCalledWith('th-a');

    // Switch to live thread B → mount effect fires reconnectToStream.
    ws = 'ws-b';
    tid = 'th-b';
    await act(async () => {
      rerender();
      await Promise.resolve();
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    // The reconnect must complete (NOT bail on the stale flag): loading clears
    // and the resumed bubble is not left streaming forever.
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    const bubble = result.current.messages.find(
      (m) => m.role === 'assistant' && (m as AssistantMessage).isStreaming,
    );
    expect(bubble).toBeUndefined();
    expect(result.current.messageError).toBeNull();
  });
});
