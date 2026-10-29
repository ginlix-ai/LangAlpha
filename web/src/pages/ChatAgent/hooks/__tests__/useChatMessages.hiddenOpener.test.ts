/**
 * A send that opens its turn hidden: the onboarding kickoff, whose only words
 * are written for the model and ride in `additionalContext`. It draws no user
 * bubble, so the reply is the first thing the chat shows, and the thread it
 * creates takes the title the caller names rather than one generated from an
 * empty first query. When the server refuses it, the failed reply stays as the
 * place its Retry lives, since the user has no message on screen to resend,
 * and so it does when the run fails over the stream. Its reply offers no
 * Regenerate: the replay a regenerate runs holds none of the kickoff.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule({
  createThreadWithTitle: vi.fn(),
}));

import { createThreadWithTitle, fetchThreadTurns, sendChatMessageStream, sendRetryStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { isOrphanAssistantMessage } from '../../components/messageList/contentProjection';
import type { AssistantMessage, ChatMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;
const mockSendRetry = sendRetryStream as Mock;
const mockFetchTurns = fetchThreadTurns as Mock;
const mockCreateThread = createThreadWithTitle as Mock;

const KICKOFF_CONTEXT = [
  { type: 'skills', name: 'onboarding', instruction: 'Greet the user and propose three first tasks.' },
  { type: 'directive', content: 'Their brokerage is connected.' },
];
const TITLE = 'Getting started';
const HIDDEN = { hiddenOpener: true, threadTitle: TITLE } as const;

const users = (messages: readonly ChatMessage[]) => messages.filter((m) => m.role === 'user');
const replies = (messages: readonly ChatMessage[]) =>
  messages.filter((m): m is AssistantMessage => m.role === 'assistant');

function renderNewChat(queryClient?: QueryClient) {
  return renderHookWithProviders(() => useChatMessages('ws-test', '__default__'), { queryClient });
}

/** A stream the test settles, so the view renders between the send and its
 *  end as it does live: in one act, the new thread's history load would run
 *  only after the send ended, and clear the banner it set. */
function heldStream() {
  let settle!: { resolve: (v: unknown) => void; reject: (e: unknown) => void };
  mockSendStream.mockImplementationOnce(() => new Promise((resolve, reject) => {
    settle = { resolve, reject };
  }));
  return {
    end: () => settle.resolve({ disconnected: false, aborted: false }),
    fail: (err: unknown) => settle.reject(err),
  };
}

/** Starts a send and returns once it reaches the stream `heldStream` holds,
 *  with the send's promise boxed (an async function would adopt it). */
async function startSend(send: () => Promise<unknown>): Promise<{ sent: Promise<unknown> }> {
  let sent!: Promise<unknown>;
  await act(async () => {
    sent = send();
  });
  await waitFor(() => expect(mockSendStream).toHaveBeenCalled());
  return { sent };
}

/** A held stream whose frames the test writes, as the server would. */
function framedStream() {
  let held!: { onEvent: (e: Record<string, unknown>) => void; resolve: (v: unknown) => void };
  mockSendStream.mockImplementationOnce((_m: string, _w: string, _t: string, opts: { onEvent: (e: Record<string, unknown>) => void }) =>
    new Promise((resolve) => {
      held = { onEvent: opts.onEvent, resolve };
    }));
  return {
    emit: (...events: Record<string, unknown>[]) => events.forEach((e) => held.onEvent(e)),
    end: () => held.resolve({ disconnected: false, aborted: false }),
  };
}

const admitted = { event: 'metadata', run_id: 'run-1', thread_id: 'thread-new', turn_index: 0 };

function quotaDenied(): Error {
  return Object.assign(new Error('HTTP error! status: 429'), {
    status: 429,
    rateLimitInfo: { type: 'credit_limit', message: 'Out of credits.' },
  });
}

describe('useChatMessages: a hidden opener', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCreateThread.mockResolvedValue({
      thread_id: 'thread-new', workspace_id: 'ws-test', thread_index: 0, title: TITLE, msg_type: 'ptc',
    });
    // Reset, not just cleared: a queued once-implementation a failed test
    // left behind would answer the next test's first send.
    mockSendStream.mockReset().mockResolvedValue({ disconnected: false, aborted: false });
    mockSendRetry.mockReset().mockResolvedValue({ disconnected: false, aborted: false });
  });

  it('draws no user bubble, only the reply, and sends an empty message with its context whole', async () => {
    const { result } = renderNewChat();
    const stream = heldStream();

    const { sent } = await startSend(() => result.current.handleSendMessage('', KICKOFF_CONTEXT, null, HIDDEN));

    expect(users(result.current.messages)).toEqual([]);
    expect(replies(result.current.messages)).toHaveLength(1);
    expect(replies(result.current.messages)[0]).toMatchObject({ isStreaming: true, hiddenOpener: true, turnIndex: 0 });
    expect(result.current.isLoading).toBe(true);

    await act(async () => {
      stream.end();
      await sent;
    });

    const [message, , threadId, opts] = mockSendStream.mock.calls[0];
    expect(message).toBe('');
    expect(threadId).toBe('thread-new');
    expect((opts as { additionalContext: unknown }).additionalContext).toEqual(KICKOFF_CONTEXT);

    expect(users(result.current.messages)).toEqual([]);
    expect(replies(result.current.messages)).toHaveLength(1);
    expect(result.current.isLoading).toBe(false);
  });

  it('creates the thread under the title it names, and paints the row with it', async () => {
    // Keeps the list entry with no observer reading it, as the sidebar would.
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false, gcTime: Infinity } } });
    const { result } = renderNewChat(queryClient);
    queryClient.setQueryData(queryKeys.threads.byWorkspace('ws-test'), { threads: [], total: 0 });

    await act(async () => {
      await result.current.handleSendMessage('', KICKOFF_CONTEXT, null, HIDDEN);
    });

    expect(mockCreateThread).toHaveBeenCalledTimes(1);
    expect(mockCreateThread.mock.calls[0][0]).toMatchObject({ workspaceId: 'ws-test', firstQuery: '', title: TITLE });
    const list = queryClient.getQueryData<{ threads: Array<{ thread_id: string; title: string; first_query_content: string }> }>(
      queryKeys.threads.byWorkspace('ws-test'),
    );
    expect(list?.threads[0]).toMatchObject({ thread_id: 'thread-new', title: TITLE, first_query_content: '' });
  });

  it('a send with no words and no title skips the create the server would refuse', async () => {
    const { result } = renderNewChat();
    const image = [{ type: 'image', data: 'data:image/png;base64,AAAA', description: 'chart.png' }];

    await act(async () => {
      await result.current.handleSendMessage('', image, [{ name: 'chart.png', type: 'image/png' }]);
    });

    expect(mockCreateThread).not.toHaveBeenCalled();
    // The legacy door, where the stream names the thread.
    expect(mockSendStream.mock.calls[0][2]).toBe('__default__');
    // A plain send keeps its bubble, attachments and all.
    expect(users(result.current.messages)).toHaveLength(1);
  });

  it('refused, keeps its reply failed beside the banner, and its Retry sends the kickoff again', async () => {
    const { result } = renderNewChat();
    const stream = heldStream();

    const { sent } = await startSend(() => result.current.handleSendMessage('', KICKOFF_CONTEXT, null, HIDDEN));
    await act(async () => {
      stream.fail(quotaDenied());
      await sent;
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.messageError).not.toBeNull();
    expect(users(result.current.messages)).toEqual([]);
    const [failed] = replies(result.current.messages);
    expect(failed).toMatchObject({ error: true, isStreaming: false, unadmitted: true });

    await act(async () => {
      await result.current.handleRetry();
    });

    expect(mockSendStream).toHaveBeenCalledTimes(2);
    const [message, , threadId, opts] = mockSendStream.mock.calls[1];
    expect(message).toBe('');
    expect(threadId).toBe('thread-new');
    expect((opts as { additionalContext: unknown }).additionalContext).toEqual(KICKOFF_CONTEXT);
    // Under the key the refused copy went with, in case it arrived after all.
    expect((opts as { requestKey: string }).requestKey).toBe(
      (mockSendStream.mock.calls[0][3] as { requestKey: string }).requestKey,
    );
    expect(users(result.current.messages)).toEqual([]);
    const after = replies(result.current.messages);
    expect(after).toHaveLength(1);
    expect(after[0]).toMatchObject({ hiddenOpener: true, isStreaming: false });
    expect(after[0].error).toBeFalsy();
    expect(after[0].unadmitted).toBeFalsy();
  });

  it('a resend the server refuses again holds its Retry again', async () => {
    const { result } = renderNewChat();
    const stream = heldStream();

    const { sent } = await startSend(() => result.current.handleSendMessage('', KICKOFF_CONTEXT, null, HIDDEN));
    await act(async () => {
      stream.fail(quotaDenied());
      await sent;
    });
    mockSendStream.mockRejectedValueOnce(quotaDenied());
    await act(async () => {
      await result.current.handleRetry();
    });

    expect(replies(result.current.messages)).toHaveLength(1);
    expect(replies(result.current.messages)[0]).toMatchObject({ error: true, unadmitted: true, hiddenOpener: true });

    await act(async () => {
      await result.current.handleRetry();
    });

    // Still the send, not a Retry of a run the server never started.
    expect(mockSendStream).toHaveBeenCalledTimes(3);
    expect(mockSendStream.mock.calls[2][0]).toBe('');
    expect((mockSendStream.mock.calls[2][3] as { additionalContext: unknown }).additionalContext).toEqual(KICKOFF_CONTEXT);
  });

  it('failed in transport, settles its reply failed and clears loading', async () => {
    const { result } = renderNewChat();
    const stream = heldStream();

    const { sent } = await startSend(() => result.current.handleSendMessage('', KICKOFF_CONTEXT, null, HIDDEN));
    await act(async () => {
      stream.fail(new TypeError('Failed to fetch'));
      await sent;
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(users(result.current.messages)).toEqual([]);
    expect(replies(result.current.messages)).toEqual([
      expect.objectContaining({ error: true, isStreaming: false, hiddenOpener: true }),
    ]);
  });

  it('admitted, then failed over the stream with nothing written, keeps its reply failed and its Retry brings the kickoff again', async () => {
    const { result } = renderNewChat();
    const stream = framedStream();

    const { sent } = await startSend(() => result.current.handleSendMessage('', KICKOFF_CONTEXT, null, HIDDEN));
    // Its worker died: recovery closed the row, and the outcome is all the
    // stream hears.
    await act(async () => {
      stream.emit(admitted, { event: 'run_end', run_id: 'run-1', thread_id: 'thread-new', outcome: 'error' });
      stream.end();
      await sent;
    });

    expect(result.current.isLoading).toBe(false);
    expect(result.current.messageError).toEqual({ message: '', kind: 'internal' });
    expect(users(result.current.messages)).toEqual([]);
    const [failed] = replies(result.current.messages);
    expect(failed).toMatchObject({ error: true, isStreaming: false, hiddenOpener: true });
    expect(isOrphanAssistantMessage(failed as unknown as Record<string, unknown>)).toBe(false);

    await act(async () => {
      await result.current.handleRetry();
    });

    // The run is retried, with the context a rerun of its input needs.
    expect(mockSendStream).toHaveBeenCalledTimes(1);
    expect(mockSendRetry).toHaveBeenCalledTimes(1);
    const retry = mockSendRetry.mock.calls[0];
    expect(retry[1]).toBe('thread-new');
    expect(retry[10]).toEqual(KICKOFF_CONTEXT);
    const after = replies(result.current.messages);
    expect(after).toHaveLength(1);
    expect(after[0]).toMatchObject({ hiddenOpener: true, isStreaming: false });
  });

  it('a Retry that fails the same way holds the kickoff for the next one', async () => {
    const { result } = renderNewChat();
    const stream = framedStream();

    const { sent } = await startSend(() => result.current.handleSendMessage('', KICKOFF_CONTEXT, null, HIDDEN));
    await act(async () => {
      stream.emit(admitted, { event: 'run_end', run_id: 'run-1', thread_id: 'thread-new', outcome: 'error' });
      stream.end();
      await sent;
    });
    mockSendRetry.mockImplementationOnce(async (_w: string, _t: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'metadata', run_id: 'run-2', thread_id: 'thread-new', turn_index: 0 });
      onEvent({ event: 'run_end', run_id: 'run-2', thread_id: 'thread-new', outcome: 'error' });
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      await result.current.handleRetry();
    });

    expect(replies(result.current.messages)).toEqual([
      expect.objectContaining({ error: true, hiddenOpener: true }),
    ]);

    await act(async () => {
      await result.current.handleRetry();
    });

    expect(mockSendRetry).toHaveBeenCalledTimes(2);
    expect(mockSendRetry.mock.calls[1][10]).toEqual(KICKOFF_CONTEXT);
  });

  it('offers its reply no Regenerate: the checkpoint a regenerate replays holds none of the kickoff', async () => {
    const { result } = renderNewChat();
    const stream = heldStream();
    mockFetchTurns.mockResolvedValue({
      turns: [{ turn_index: 0, edit_checkpoint_id: 'cp-root', regenerate_checkpoint_id: 'cp-0' }],
    });

    const { sent } = await startSend(() => result.current.handleSendMessage('', KICKOFF_CONTEXT, null, HIDDEN));
    await act(async () => {
      stream.end();
      await sent;
    });
    const before = result.current.messages;
    const [reply] = replies(before);

    await act(async () => {
      await result.current.handleRegenerate(reply.id);
    });

    expect(mockFetchTurns).not.toHaveBeenCalled();
    expect(mockSendStream).toHaveBeenCalledTimes(1);
    expect(result.current.messages).toBe(before);
    expect(result.current.isLoading).toBe(false);
  });
});
