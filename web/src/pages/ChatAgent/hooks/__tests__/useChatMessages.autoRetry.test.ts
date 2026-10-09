/**
 * A turn the server ends with `retry` instead of `error`.
 *
 * The server sends it when a run fails before reaching the agent on an error
 * it classes as transient (a computer still starting can hold the turn past
 * its 60s lock), finalizes the run as a retryable failure, then re-raises,
 * which drops the connection. The client resends on its own a bounded number
 * of times, and leaves a failed reply with Retry on it when it stops. The frame
 * names the recovery: POST /retry for a run the server started, the same send
 * again when it started none. The POST streams run through the real transport,
 * so the frames are the server's bytes, parsed as the browser parses them.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect } from './chatHookHarness';
import type { AssistantMessage, ChatMessage } from '@/types/chat';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('@/lib/authToken', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/lib/authToken')>()),
  getAuthHeaders: async () => ({}),
}));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async (importOriginal) => {
  const real = await importOriginal<typeof import('../../utils/api')>();
  return (await import('./chatHookHarness')).apiMockModule({
    sendChatMessageStream: real.sendChatMessageStream,
    sendRetryStream: real.sendRetryStream,
  });
});

import { getWorkflowStatus, replayThreadHistory } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;

const WS = 'ws-x';
const TID = 'th-x';

// error_handling.py yields these as f-strings over json.dumps, so the default
// ", " and ": " separators are part of the wire, and `retry` has no `id:` line.
const STARTING = `id: 0\nevent: workspace_status\ndata: {"status": "starting", "workspace_id": "${WS}"}\n\n`;
const retryFrame = (retryCount: number, recovery: 'retry' | 'resend' = 'retry') =>
  `event: retry\ndata: {"message": "Temporary error occurred, you can retry or resume the turn", "thread_id": "${TID}", "auto_retry": true, "error_type": "timeout_error", "error_class": "RuntimeError", "retry_count": ${retryCount}, "max_retries": 3, "recovery": "${recovery}"}\n\n`;
const failureFrame = (recovery: 'retry' | 'resend') =>
  `event: error\ndata: {"thread_id": "${TID}", "error": "Workspace ${WS} not found", "type": "workflow_error", "error_type": "workspace_not_found", "error_class": "ValueError", "recovery": "${recovery}"}\n\n`;
const metadataFrame = (runId: string) =>
  `id: 1\nevent: metadata\ndata: {"thread_id": "${TID}", "run_id": "${runId}"}\n\n`;
const ANSWER = `id: 2\nevent: message_chunk\ndata: {"agent": "main", "role": "assistant", "content": "Recovered answer", "content_type": "text"}\n\n`;

/** A 200 SSE response. `drop` ends the body the way the server's re-raise
 * does: the read rejects with a TypeError instead of finishing. */
function sse(runId: string, frames: string[], { drop }: { drop: boolean }): Response {
  const encoder = new TextEncoder();
  const chunks = frames.map((f) => encoder.encode(f));
  let next = 0;
  const reader = {
    read: async () => {
      if (next < chunks.length) return { done: false, value: chunks[next++] };
      if (drop) throw new TypeError('network error');
      return { done: true, value: undefined };
    },
  };
  const location = `/api/v1/threads/${TID}/messages/stream?run_id=${runId}`;
  return {
    ok: true,
    status: 200,
    headers: { get: (k: string) => (k.toLowerCase() === 'content-location' ? location : null) },
    body: { getReader: () => reader },
  } as unknown as Response;
}

interface Post { path: 'messages' | 'retry'; body: Record<string, unknown> }
let posts: Post[] = [];
let onSend: (n: number) => Response;
let onRetry: (n: number) => Response;
const retryPosts = () => posts.filter((p) => p.path === 'retry');
const sendPosts = () => posts.filter((p) => p.path === 'messages');

const QUESTION = 'what moved NVDA today?';
const CONTEXT = [{ type: 'image', data: 'data:image/png;base64,iVBORw0KGgo=', description: 'chart.png' }];
const users = (messages: readonly ChatMessage[]) => messages.filter((m) => m.role === 'user');

const assistants = (messages: readonly ChatMessage[]): AssistantMessage[] =>
  messages.filter((m): m is AssistantMessage => m.role === 'assistant');

/** An assistant bubble that settled with nothing on it: what MessageList hides,
 * and what this flow used to leave behind. */
const settledEmpty = (messages: readonly ChatMessage[]): boolean =>
  assistants(messages).some((m) =>
    !m.isStreaming && !m.content && !m.error && !m.stopped && (m.contentSegments?.length ?? 0) === 0,
  );

/** Let the stream's promise chain run between timer steps. */
async function advance(ms: number) {
  await act(async () => {
    await vi.advanceTimersByTimeAsync(ms);
  });
  for (let i = 0; i < 5; i++) {
    await act(async () => {
      await vi.advanceTimersByTimeAsync(0);
    });
  }
}

async function mountAndSend(
  additionalContext: Record<string, unknown>[] | null = null,
  options: { subagentsAllowed?: boolean } = {},
) {
  const rendered = renderHookWithProviders(() => useChatMessages(WS, TID));
  await waitFor(() => expect(mockReplay).toHaveBeenCalled());
  await settleMountEffect();
  // Every transcript the hook publishes, so a bubble that settles empty for a
  // single write is caught too.
  const published: ChatMessage[][] = [];
  rendered.result.current.liveMessages.subscribe(() => {
    published.push([...rendered.result.current.liveMessages.get()]);
  });
  const statusCallsAtMount = mockStatus.mock.calls.length;
  vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
  await act(async () => {
    await rendered.result.current.handleSendMessage(QUESTION, additionalContext, null, options);
  });
  return { ...rendered, published, statusCallsAtMount };
}

describe('useChatMessages: a turn the server ends with `retry`', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockReset();
    mockReplay.mockResolvedValue(undefined);
    mockStatus.mockReset();
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
    posts = [];
    vi.stubGlobal('fetch', vi.fn(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = String(input);
      const body = JSON.parse(String(init?.body ?? '{}')) as Record<string, unknown>;
      if (url.endsWith(`/threads/${TID}/retry`)) {
        posts.push({ path: 'retry', body });
        return onRetry(retryPosts().length);
      }
      if (url.endsWith(`/threads/${TID}/messages`)) {
        posts.push({ path: 'messages', body });
        return onSend(sendPosts().length);
      }
      // Anything else (a build check after the turn) gets a quiet miss.
      return { ok: false, status: 404, headers: { get: () => null }, text: async () => '', json: async () => ({}) } as unknown as Response;
    }));
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.unstubAllGlobals();
  });

  it('resends through /retry and keeps the reply streaming until the new attempt answers', async () => {
    onSend = () => sse('run-1', [STARTING, retryFrame(1)], { drop: true });
    onRetry = () => sse('run-2', [metadataFrame('run-2'), ANSWER], { drop: false });

    const { result, published, statusCallsAtMount } = await mountAndSend();

    // Waiting out the backoff: still loading, the reply still streaming, and
    // no reconnect to a run the server already ended.
    expect(result.current.isLoading).toBe(true);
    expect(assistants(result.current.messages)).toHaveLength(1);
    expect(assistants(result.current.messages)[0]).toMatchObject({ isStreaming: true });
    expect(assistants(result.current.messages)[0].error).toBeFalsy();
    expect(mockStatus.mock.calls.length).toBe(statusCallsAtMount);

    await advance(999);
    expect(retryPosts()).toHaveLength(0);

    await advance(1);
    expect(retryPosts()).toHaveLength(1);
    // Pinned to the run that failed, so the server refuses rather than retry
    // a different turn's attempt.
    expect(retryPosts()[0].body).toMatchObject({ workspace_id: WS, run_id: 'run-1' });

    const replies = assistants(result.current.messages);
    expect(replies).toHaveLength(1);
    expect(replies[0].isStreaming).toBe(false);
    expect(replies[0].error).toBeFalsy();
    expect(JSON.stringify(replies[0])).toContain('Recovered answer');
    expect(result.current.isLoading).toBe(false);
    expect(published.some(settledEmpty)).toBe(false);
  });

  it('stops at the client bound and leaves the reply failed, and the manual Retry still works', async () => {
    // Every attempt fails the same way, and the server reports attempt 1 each
    // time (it does when the failure lands outside the run it numbers), so
    // only the client's own count can end the chain.
    onSend = () => sse('run-1', [STARTING, retryFrame(1)], { drop: true });
    onRetry = (n) => sse(`run-${n + 1}`, [STARTING, retryFrame(1)], { drop: true });

    const { result, published } = await mountAndSend();

    await advance(1000);
    expect(retryPosts()).toHaveLength(1);
    expect(retryPosts()[0].body.run_id).toBe('run-1');
    await advance(2000);
    expect(retryPosts()).toHaveLength(2);
    expect(retryPosts()[1].body.run_id).toBe('run-2');
    await advance(4000);
    expect(retryPosts()).toHaveLength(3);
    await advance(60_000);
    expect(retryPosts()).toHaveLength(3);

    // Failed rather than settled empty: `error` is what puts Retry on the bubble.
    const replies = assistants(result.current.messages);
    expect(replies).toHaveLength(1);
    expect(replies[0]).toMatchObject({ isStreaming: false, error: true, content: 'chat.autoRetryExhausted' });
    expect(result.current.isLoading).toBe(false);
    expect(published.some(settledEmpty)).toBe(false);

    // The manual Retry takes the failed bubble's place, as it does after an `error`.
    onRetry = () => sse('run-9', [metadataFrame('run-9'), ANSWER], { drop: false });
    await act(async () => {
      await result.current.handleRetry();
    });
    expect(retryPosts()).toHaveLength(4);
    // The run the last automatic attempt left, which is the latest.
    expect(retryPosts()[3].body.run_id).toBe('run-4');
    const after = assistants(result.current.messages);
    expect(after).toHaveLength(1);
    expect(after[0].error).toBeFalsy();
    expect(JSON.stringify(after[0])).toContain('Recovered answer');
  });

  it('a stop during the wait cancels the resend', async () => {
    onSend = () => sse('run-1', [STARTING, retryFrame(1)], { drop: true });
    onRetry = () => sse('run-2', [metadataFrame('run-2'), ANSWER], { drop: false });

    const { result } = await mountAndSend();
    await act(async () => {
      await result.current.stopWorkflow();
    });
    await advance(10_000);

    expect(retryPosts()).toHaveLength(0);
    expect(assistants(result.current.messages)[0]).toMatchObject({ isStreaming: false, stopped: true });
    expect(result.current.isLoading).toBe(false);
  });

  it('a `retry` after the run started reconnects instead of resending', async () => {
    // Past `metadata` the run belongs to the background executor; a `retry`
    // there means only this reader failed, and the run may still be going.
    onSend = () => sse('run-1', [metadataFrame('run-1'), retryFrame(1)], { drop: true });
    onRetry = () => sse('run-2', [metadataFrame('run-2'), ANSWER], { drop: false });

    const { statusCallsAtMount } = await mountAndSend();
    await advance(10_000);

    expect(mockStatus.mock.calls.length).toBeGreaterThan(statusCallsAtMount);
    expect(retryPosts()).toHaveLength(0);
  });

  it('resends the same message as a send when the server started no run', async () => {
    onSend = (n) => n === 1
      ? sse('run-1', [STARTING, retryFrame(1, 'resend')], { drop: true })
      : sse('run-2', [metadataFrame('run-2'), ANSWER], { drop: false });

    const { result, published, statusCallsAtMount } = await mountAndSend(CONTEXT);
    await advance(1000);

    expect(retryPosts()).toHaveLength(0);
    const [first, again] = sendPosts();
    expect(again.body).toMatchObject({
      messages: [{ role: 'user', content: QUESTION }],
      additional_context: CONTEXT,
    });
    // A new request: the first never became a run to deduplicate against.
    expect(again.body.request_key).not.toBe(first.body.request_key);
    expect(users(result.current.messages)).toHaveLength(1);
    const replies = assistants(result.current.messages);
    expect(replies).toHaveLength(1);
    expect(JSON.stringify(replies[0])).toContain('Recovered answer');
    expect(mockStatus.mock.calls.length).toBe(statusCallsAtMount);
    expect(published.some(settledEmpty)).toBe(false);
  });

  it('resends the subagents pick the send carried', async () => {
    onSend = (n) => n === 1
      ? sse('run-1', [STARTING, retryFrame(1, 'resend')], { drop: true })
      : sse('run-2', [metadataFrame('run-2'), ANSWER], { drop: false });

    await mountAndSend(null, { subagentsAllowed: false });
    await advance(1000);

    const [first, again] = sendPosts();
    expect(first.body.subagents_allowed).toBe(false);
    expect(again.body.subagents_allowed).toBe(false);
  });

  it('sends the context the send carried with /retry', async () => {
    onSend = () => sse('run-1', [STARTING, retryFrame(1)], { drop: true });
    onRetry = () => sse('run-2', [metadataFrame('run-2'), ANSWER], { drop: false });

    await mountAndSend(CONTEXT);
    await advance(1000);

    expect(retryPosts()[0].body).toMatchObject({ run_id: 'run-1', additional_context: CONTEXT });
  });

  it('a failure `error` with no run left keeps the send for the manual Retry', async () => {
    onSend = (n) => n === 1
      ? sse('run-1', [STARTING, failureFrame('resend')], { drop: true })
      : sse('run-2', [metadataFrame('run-2'), ANSWER], { drop: false });

    const { result, statusCallsAtMount } = await mountAndSend(CONTEXT);
    await advance(60_000);

    // Failed in place, with no reconnect to a run that was never started, and
    // the question still on screen: no reload may take what was never stored.
    expect(sendPosts()).toHaveLength(1);
    expect(mockStatus.mock.calls.length).toBe(statusCallsAtMount);
    expect(assistants(result.current.messages)[0]).toMatchObject({ isStreaming: false, error: true });
    expect(users(result.current.messages)[0].isHistory).toBeFalsy();
    expect(result.current.isLoading).toBe(false);

    await act(async () => {
      await result.current.handleRetry();
    });

    expect(retryPosts()).toHaveLength(0);
    expect(sendPosts()[1].body).toMatchObject({
      messages: [{ role: 'user', content: QUESTION }],
      additional_context: CONTEXT,
    });
    expect(users(result.current.messages)).toHaveLength(1);
    const after = assistants(result.current.messages);
    expect(after).toHaveLength(1);
    expect(after[0].error).toBeFalsy();
    expect(JSON.stringify(after[0])).toContain('Recovered answer');
  });

  it('names the end of the server\'s own retries in the reader\'s language', async () => {
    const outOfRetries = `event: error\ndata: {"thread_id": "${TID}", "message": "Turn failed after 3 retry attempts", "retry_count": 4, "max_retries": 3, "recovery": "retry"}\n\n`;
    onSend = () => sse('run-1', [STARTING, outOfRetries], { drop: true });

    const { result } = await mountAndSend();
    await advance(60_000);

    expect(retryPosts()).toHaveLength(0);
    expect(assistants(result.current.messages)[0]).toMatchObject({
      isStreaming: false,
      error: true,
      content: 'chat.autoRetryExhausted',
    });
  });

  it('a failure `error` on a started run retries that run from the manual Retry', async () => {
    onSend = () => sse('run-1', [STARTING, failureFrame('retry')], { drop: true });
    onRetry = () => sse('run-2', [metadataFrame('run-2'), ANSWER], { drop: false });

    const { result } = await mountAndSend(CONTEXT);
    await advance(60_000);
    expect(retryPosts()).toHaveLength(0);

    await act(async () => {
      await result.current.handleRetry();
    });

    expect(retryPosts()[0].body).toMatchObject({ run_id: 'run-1', additional_context: CONTEXT });
    expect(JSON.stringify(assistants(result.current.messages))).toContain('Recovered answer');
  });
});
