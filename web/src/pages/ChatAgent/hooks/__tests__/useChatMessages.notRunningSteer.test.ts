/**
 * A steer the server refuses as `not_running`.
 *
 * A message sent while the view shows a turn running goes out as a steer
 * (`steer_only`). The view's loading state lags the server's, so the turn
 * may have ended by the time the POST lands, and the server refuses it as
 * `not_running` rather than opening a turn on a steer's connection. The hook
 * sends it again as a plain send under the same request key once the view
 * lets the ended turn go.
 */

import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks, declared before any imports that depend on them
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

// The real handleTextContent mutates the assistant message via the setter
// it receives. We capture the calls so we can assert chunks were routed
// to the *new* (demoted) assistant message id, not the old one.
const textContentCalls: Array<{ assistantMessageId: string; text: string }> = [];

vi.mock('../../session/stream/mainEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).mainHandlersMockModule(await importOriginal(), {
    handleTextContent: vi.fn(({ event, assistantMessageId }) => {
      textContentCalls.push({
        assistantMessageId,
        text: (event?.content as string) || '',
      });
      return true;
    }),
  }));

vi.mock('../../session/subagents/liveEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).subagentHandlersMockModule(await importOriginal()));

vi.mock('../../session/streamRefs', async (importOriginal) =>
  (await import('./chatHookHarness')).streamRefsMockModule(await importOriginal()));

vi.mock('../../session/history/historyHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).historyHandlersMockModule(await importOriginal()));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule({
  // No pre-create unless a test gives one: the send falls back to the
  // '__default__' door, where the stream names the thread.
  createThreadWithTitle: vi.fn().mockRejectedValue(new Error('no pre-create')),
}));

import { sendChatMessageStream, cancelWorkflow, createThreadWithTitle } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { UserMessage, AssistantMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;
const mockCancel = cancelWorkflow as Mock;
const mockCreateThread = createThreadWithTitle as Mock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type OnEvent = (e: Record<string, unknown>) => void;

interface Deferred<T> {
  promise: Promise<T>;
  resolve: (v: T) => void;
}

function notRunning(): Error {
  return Object.assign(new Error('The turn has ended.'), {
    status: 409,
    errorInfo: { code: 'not_running', message: 'The turn has ended.' },
  });
}

function refusal(code: string): Error {
  return Object.assign(new Error('Refused.'), { status: 409, errorInfo: { code, message: 'Refused.' } });
}

function deferred<T>(): Deferred<T> {
  let resolve!: (v: T) => void;
  const promise = new Promise<T>((r) => {
    resolve = r;
  });
  return { promise, resolve };
}

describe('useChatMessages: a steer the server refuses as not_running', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    textContentCalls.length = 0;
  });

  it('goes out again as a send under its key once the ended turn lets go, and opens its own turn', async () => {
    const firstHang = deferred<{ disconnected: boolean }>();
    let resendOnEvent: OnEvent | null = null;
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: OnEvent },
      ) => {
        const call = mockSendStream.mock.calls.length;
        if (call === 1) {
          onEvent({ event: 'thread_id', thread_id: 'thread-nr-1' });
          return firstHang.promise;
        }
        if (call === 2) throw notRunning();
        resendOnEvent = onEvent;
        return new Promise(() => {});
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-nr'));
    let firstSend: Promise<unknown> = Promise.resolve();
    await act(async () => {
      firstSend = result.current.handleSendMessage('first turn');
      await Promise.resolve();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));

    await act(async () => {
      await result.current.handleSendMessage('follow-up steering');
    });

    // Sent as a steer only, and refused: it waits on screen while the view
    // still shows the old turn running.
    expect(mockSendStream).toHaveBeenCalledTimes(2);
    const steerOpts = mockSendStream.mock.calls[1][3] as { requestKey: string; steerOnly?: boolean };
    expect(steerOpts.steerOnly).toBe(true);
    const waiting = result.current.messages.filter(
      (m): m is UserMessage => m.role === 'user' && m.content === 'follow-up steering',
    );
    expect(waiting).toHaveLength(1);
    expect((waiting[0] as UserMessage & { steering?: boolean }).steering).toBe(true);

    const assistantsBefore = result.current.messages.filter((m) => m.role === 'assistant');
    firstHang.resolve({ disconnected: false });
    await act(async () => {
      await firstSend.catch(() => undefined);
    });

    await waitFor(() => expect(mockSendStream).toHaveBeenCalledTimes(3));
    expect(mockSendStream.mock.calls[2][0]).toBe('follow-up steering');
    const resendOpts = mockSendStream.mock.calls[2][3] as { requestKey: string; steerOnly?: boolean };
    expect(resendOpts.requestKey).toBe(steerOpts.requestKey);
    expect(resendOpts.steerOnly).toBeFalsy();

    await act(async () => {
      resendOnEvent!({ event: 'metadata', thread_id: 'thread-nr-1', run_id: 'run-resent-1' });
      resendOnEvent!({
        event: 'message_chunk',
        role: 'assistant',
        content: 'resent reply',
        content_type: 'text',
        agent: 'main',
        id: 'msg-resent-1',
      });
      await Promise.resolve();
    });

    // One copy of the message, now a plain send, with a turn of its own. It
    // keeps the waiting bubble's id, so the list keeps that bubble mounted.
    const copies = result.current.messages.filter(
      (m): m is UserMessage => m.role === 'user' && m.content === 'follow-up steering',
    );
    expect(copies).toHaveLength(1);
    expect(copies[0].id).toBe(waiting[0].id);
    expect((copies[0] as UserMessage & { steering?: boolean }).steering).toBeFalsy();
    const assistantsAfter = result.current.messages.filter(
      (m): m is AssistantMessage => m.role === 'assistant',
    );
    expect(assistantsAfter.length).toBe(assistantsBefore.length + 1);
    const resentAssistant = assistantsAfter[assistantsAfter.length - 1];
    expect(textContentCalls.at(-1)?.assistantMessageId).toBe(resentAssistant.id);
  });

  it('a resend the server failed to start outlives a reload, and its Retry sends it again under its key', async () => {
    const firstHang = deferred<{ disconnected: boolean }>();
    const steerAnswer = deferred<void>();
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: OnEvent },
      ) => {
        const call = mockSendStream.mock.calls.length;
        if (call === 1) {
          onEvent({ event: 'thread_id', thread_id: 'thread-nr-2' });
          return firstHang.promise;
        }
        if (call === 2) {
          // The running turn ends while the steer waits for its answer.
          await steerAnswer.promise;
          throw notRunning();
        }
        if (call === 3) {
          // The START the resend opens fails before the run exists.
          onEvent({
            event: 'error',
            thread_id: 'thread-nr-2',
            error: 'start failed',
            error_type: 'workflow_error',
            recovery: 'resend',
          });
          return { disconnected: false };
        }
        onEvent({ event: 'metadata', thread_id: 'thread-nr-2', run_id: 'run-resent' });
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-nr'));
    let firstSend: Promise<unknown> = Promise.resolve();
    await act(async () => {
      firstSend = result.current.handleSendMessage('first turn');
      await Promise.resolve();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));
    let steeringSend: Promise<unknown> = Promise.resolve();
    await act(async () => {
      steeringSend = result.current.handleSendMessage('follow-up steering');
      await Promise.resolve();
    });
    firstHang.resolve({ disconnected: false });
    await act(async () => {
      await firstSend.catch(() => undefined);
    });
    steerAnswer.resolve();
    await act(async () => {
      await steeringSend;
    });
    await waitFor(() => expect(mockSendStream).toHaveBeenCalledTimes(3));
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    // Nothing on the server holds it, so a reload, which keeps only what is
    // not history, must keep both bubbles.
    const followUp = result.current.messages.find(
      (m): m is UserMessage => m.role === 'user' && m.content === 'follow-up steering',
    );
    expect(followUp).toMatchObject({ unadmitted: true });
    expect(followUp?.isHistory).toBeFalsy();
    const failed = result.current.messages.filter((m): m is AssistantMessage => m.role === 'assistant').at(-1);
    expect(failed).toMatchObject({ error: true, unadmitted: true });
    expect(failed?.isHistory).toBeFalsy();

    await act(async () => {
      await result.current.handleRetry();
    });

    const steerKey = (mockSendStream.mock.calls[1][3] as { requestKey: string }).requestKey;
    expect(mockSendStream).toHaveBeenCalledTimes(4);
    expect(mockSendStream.mock.calls[3][0]).toBe('follow-up steering');
    expect((mockSendStream.mock.calls[3][3] as { requestKey: string }).requestKey).toBe(steerKey);
  });
});

// ---------------------------------------------------------------------------
// The queue of refused steers
// ---------------------------------------------------------------------------

interface SendOpts {
  onEvent: OnEvent;
  requestKey: string;
  steerOnly?: boolean;
  signal?: AbortSignal | null;
}

/** One POST: a plain send whose stream stays open until ended, or a steer whose answer the test gives. */
interface Post {
  message: string;
  opts: SendOpts;
  /** Plain send: ends its stream. */
  end: () => void;
  /** Steer: refuses it with `code`. */
  refuse: (code: string) => void;
}

/** Every POST in order. A steer hangs for its answer; a send opens a turn of its own on `threadId`. */
function capturePosts(threadId = 'thread-q'): Post[] {
  const posts: Post[] = [];
  let runs = 0;
  mockSendStream.mockImplementation(
    (message: string, _ws: string, _tid: string | null, opts: SendOpts) =>
      new Promise((resolve, reject) => {
        const post: Post = {
          message,
          opts,
          end: () => resolve({ disconnected: false }),
          refuse: (code) => reject(refusal(code)),
        };
        posts.push(post);
        if (opts.steerOnly) return;
        runs += 1;
        opts.onEvent({ event: 'thread_id', thread_id: threadId });
        opts.onEvent({ event: 'metadata', thread_id: threadId, run_id: `run-${runs}` });
        opts.signal?.addEventListener('abort', () => resolve({ disconnected: false, aborted: true }), { once: true });
      }),
  );
  return posts;
}

type Hook = ReturnType<typeof renderHookWithProviders<ReturnType<typeof useChatMessages>>>;

/** Opens a first turn that stays running until its stream ends. */
async function startTurn() {
  const posts = capturePosts();
  const hook = renderHookWithProviders(() => useChatMessages('ws-q'));
  await act(async () => {
    void hook.result.current.handleSendMessage('first turn');
    await Promise.resolve();
  });
  await waitFor(() => expect(hook.result.current.isLoading).toBe(true));
  return { hook, posts };
}

/** Sends `message` while the view shows the turn running: a steer whose POST waits for its answer. */
async function steer(hook: Hook, message: string): Promise<{ sent: Promise<unknown> }> {
  let sent: Promise<unknown> = Promise.resolve();
  await act(async () => {
    sent = hook.result.current.handleSendMessage(message);
    await Promise.resolve();
  });
  // Boxed: an async function would otherwise wait for the POST's answer.
  return { sent };
}

async function settle(step: () => void | Promise<unknown>) {
  await act(async () => {
    await step();
    await new Promise((r) => setTimeout(r, 0));
  });
}

const userBubbles = (hook: Hook, text: string) =>
  hook.result.current.messages.filter((m): m is UserMessage => m.role === 'user' && m.content === text);

describe('useChatMessages: refused steers go out one at a time, oldest first', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCancel.mockResolvedValue({ cancelled: true, message: 'ok' });
  });

  it('sends the second only once the turn the first opened has ended', async () => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    const two = await steer(hook, 'steer two');
    expect(posts.map((p) => [p.message, !!p.opts.steerOnly])).toEqual([
      ['first turn', false], ['steer one', true], ['steer two', true],
    ]);
    await settle(async () => {
      posts[1].refuse('not_running');
      await one.sent;
      posts[2].refuse('not_running');
      await two.sent;
    });
    // Both wait on screen while the view still shows the old turn running.
    expect(posts).toHaveLength(3);
    expect(userBubbles(hook, 'steer one')).toHaveLength(1);
    expect(userBubbles(hook, 'steer two')).toHaveLength(1);

    await settle(() => posts[0].end());
    await waitFor(() => expect(posts).toHaveLength(4));
    expect(posts[3].message).toBe('steer one');
    expect(posts[3].opts.steerOnly).toBeFalsy();
    expect(posts[3].opts.requestKey).toBe(posts[1].opts.requestKey);
    // The second waits for the turn the first opened.
    await settle(() => {});
    expect(posts).toHaveLength(4);
    expect(userBubbles(hook, 'steer two')).toHaveLength(1);

    await settle(() => posts[3].end());
    await waitFor(() => expect(posts).toHaveLength(5));
    expect(posts[4].message).toBe('steer two');
    expect(posts[4].opts.steerOnly).toBeFalsy();
    expect(posts[4].opts.requestKey).toBe(posts[2].opts.requestKey);
    expect(userBubbles(hook, 'steer one')).toHaveLength(1);
    expect(userBubbles(hook, 'steer two')).toHaveLength(1);
  });

  it('resends them in the order they were sent, whatever order their refusals land in', async () => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    const two = await steer(hook, 'steer two');
    await settle(async () => {
      posts[2].refuse('not_running');
      await two.sent;
    });
    // Queued behind the refused steer, and sent after both steers.
    await settle(() => hook.result.current.handleSendMessage('typed later'));
    await settle(async () => {
      posts[1].refuse('not_running');
      await one.sent;
    });
    expect(posts).toHaveLength(3);

    await settle(() => posts[0].end());
    await waitFor(() => expect(posts).toHaveLength(4));
    expect(posts[3].message).toBe('steer one');
    expect(posts[3].opts.requestKey).toBe(posts[1].opts.requestKey);
    await settle(() => posts[3].end());
    await waitFor(() => expect(posts).toHaveLength(5));
    expect(posts[4].message).toBe('steer two');
    expect(posts[4].opts.requestKey).toBe(posts[2].opts.requestKey);
    await settle(() => posts[4].end());
    await waitFor(() => expect(posts).toHaveLength(6));
    expect(posts[5].message).toBe('typed later');
  });

  it('holds a refused steer while one sent before it has no answer, even once the view lets the turn go', async () => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    const two = await steer(hook, 'steer two');
    await settle(async () => {
      posts[2].refuse('not_running');
      await two.sent;
    });
    await settle(() => posts[0].end());
    await settle(() => {});
    expect(posts.map((p) => p.message)).toEqual(['first turn', 'steer one', 'steer two']);
    expect(hook.result.current.isLoading).toBe(false);

    await settle(async () => {
      posts[1].refuse('not_running');
      await one.sent;
    });
    await waitFor(() => expect(posts).toHaveLength(4));
    expect(posts[3].message).toBe('steer one');
    expect(posts[3].opts.requestKey).toBe(posts[1].opts.requestKey);
    await settle(() => posts[3].end());
    await waitFor(() => expect(posts).toHaveLength(5));
    expect(posts[4].message).toBe('steer two');
    expect(posts[4].opts.requestKey).toBe(posts[2].opts.requestKey);
  });

  it('releases the hold when the earlier steer fails', async () => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    const two = await steer(hook, 'steer two');
    await settle(async () => {
      posts[2].refuse('not_running');
      await two.sent;
    });
    await settle(() => posts[0].end());
    await settle(() => {});
    expect(posts.map((p) => p.message)).toEqual(['first turn', 'steer one', 'steer two']);
    expect(hook.result.current.isLoading).toBe(false);

    // A timed-out POST lands here too: the steer API bounds it.
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    await settle(async () => {
      posts[1].refuse('server_error');
      await one.sent;
    });
    logged.mockRestore();
    await waitFor(() => expect(posts).toHaveLength(4));
    expect(posts[3].message).toBe('steer two');
  });

  it('queues a message sent meanwhile behind them instead of steering the turn the first opens', async () => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    await settle(async () => {
      posts[1].refuse('not_running');
      await one.sent;
    });

    // The view still shows the old turn running, but nothing goes out.
    await settle(() => hook.result.current.handleSendMessage('typed later'));
    expect(posts).toHaveLength(2);
    expect(userBubbles(hook, 'typed later')).toMatchObject([{ steering: true }]);
    const waitingId = userBubbles(hook, 'typed later')[0].id;

    await settle(() => posts[0].end());
    await waitFor(() => expect(posts).toHaveLength(3));
    expect(posts[2].message).toBe('steer one');

    // Still behind the refused steer while the turn it opened runs: queued, not steered.
    await settle(() => {});
    expect(posts).toHaveLength(3);

    await settle(() => posts[2].end());
    await waitFor(() => expect(posts).toHaveLength(4));
    expect(posts[3].message).toBe('typed later');
    expect(posts[3].opts.steerOnly).toBeFalsy();
    expect(posts[3].opts.requestKey).not.toBe(posts[1].opts.requestKey);
    expect(userBubbles(hook, 'typed later')).toMatchObject([{ id: waitingId }]);
  });

  it('steers as usual once the queue has drained', async () => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    await settle(async () => {
      posts[1].refuse('not_running');
      await one.sent;
    });
    await settle(() => posts[0].end());
    await waitFor(() => expect(posts).toHaveLength(3));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(true));

    await steer(hook, 'into the new turn');
    expect(posts).toHaveLength(4);
    expect(posts[3]).toMatchObject({ message: 'into the new turn', opts: { steerOnly: true } });
  });
});

describe('useChatMessages: a refused steer after the user stopped', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCancel.mockResolvedValue({ cancelled: true, message: 'ok' });
  });

  it.each(['not_running', 'stopping'])('goes back to the composer when the %s refusal lands after the stop', async (code) => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    await settle(() => hook.result.current.stopWorkflow());
    expect(hook.result.current.isLoading).toBe(false);

    await settle(async () => {
      posts[1].refuse(code);
      await one.sent;
    });
    expect(hook.result.current.returnedSteering).toBe('steer one');
    expect(userBubbles(hook, 'steer one')).toEqual([]);
    await settle(() => {});
    expect(posts).toHaveLength(2);
  });

  it('gives back the steers still waiting to go out', async () => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    const two = await steer(hook, 'steer two');
    await settle(async () => {
      posts[1].refuse('not_running');
      await one.sent;
      posts[2].refuse('not_running');
      await two.sent;
    });
    await settle(() => hook.result.current.stopWorkflow());

    expect(hook.result.current.returnedSteering).toBe('steer one\nsteer two');
    expect(userBubbles(hook, 'steer one')).toEqual([]);
    expect(userBubbles(hook, 'steer two')).toEqual([]);
    await settle(() => {});
    expect(posts).toHaveLength(3);
  });

  it('resends a not_running refusal that lands with no stop', async () => {
    const { hook, posts } = await startTurn();
    const one = await steer(hook, 'steer one');
    await settle(async () => {
      posts[1].refuse('not_running');
      await one.sent;
    });
    expect(hook.result.current.returnedSteering).toBeNull();
    await settle(() => posts[0].end());
    await waitFor(() => expect(posts).toHaveLength(3));
  });
});

describe('useChatMessages: refused steers belong to their view, not to a thread id', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockCancel.mockResolvedValue({ cancelled: true, message: 'ok' });
  });

  it('sends a steer refused while a new chat creates its thread, on the created thread once idle', async () => {
    // The first send of a new chat shows the turn running before the thread
    // exists, so a follow-up goes out as a steer under '__default__'. The
    // thread the create names is the same chat, so the steer goes out there.
    const created = deferred<Record<string, unknown>>();
    mockCreateThread.mockReturnValueOnce(created.promise);
    const posts = capturePosts('thread-pc');
    const hook = renderHookWithProviders(() => useChatMessages('ws-pc'));
    await act(async () => {
      void hook.result.current.handleSendMessage('first turn');
      await Promise.resolve();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(true));

    const one = await steer(hook, 'sent during the create');
    expect(posts.map((p) => [p.message, !!p.opts.steerOnly])).toEqual([['sent during the create', true]]);
    await settle(async () => {
      posts[0].refuse('not_running');
      await one.sent;
    });

    await settle(() =>
      created.resolve({ thread_id: 'thread-pc', workspace_id: 'ws-pc', thread_index: 0, msg_type: 'ptc', title: null }),
    );
    await waitFor(() => expect(posts).toHaveLength(2));
    expect(posts[1].message).toBe('first turn');
    expect(mockSendStream.mock.calls[1][2]).toBe('thread-pc');
    expect(userBubbles(hook, 'sent during the create')).toHaveLength(1);

    await settle(() => posts[1].end());
    await waitFor(() => expect(posts).toHaveLength(3));
    expect(posts[2].message).toBe('sent during the create');
    expect(posts[2].opts.steerOnly).toBeFalsy();
    expect(posts[2].opts.requestKey).toBe(posts[0].opts.requestKey);
    expect(mockSendStream.mock.calls[2][2]).toBe('thread-pc');
    expect(userBubbles(hook, 'sent during the create')).toHaveLength(1);
  });

  it('ends with an in-place thread switch: nothing goes out, and the keys are released', async () => {
    // One refusal lands before the switch and one after. Both bubbles go with
    // the thread, so neither steer may go out on the next one, and the same
    // text sent again on the first thread is a new send, not a retransmit.
    // The earlier steer's open POST holds the queue past the turn's end.
    const posts = capturePosts('thread-A');
    let tid = 'thread-A';
    const hook = renderHookWithProviders(() => useChatMessages('ws-sw', tid));
    await act(async () => {
      void hook.result.current.handleSendMessage('first turn');
      await Promise.resolve();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(true));
    const early = await steer(hook, 'refused after the switch');
    const late = await steer(hook, 'refused before the switch');
    await settle(async () => {
      posts[2].refuse('not_running');
      await late.sent;
    });
    await settle(() => posts[0].end());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    expect(posts).toHaveLength(3);

    await act(async () => {
      tid = 'thread-B';
      hook.rerender();
      await Promise.resolve();
    });
    await settle(async () => {
      posts[1].refuse('not_running');
      await early.sent;
    });
    expect(posts).toHaveLength(3);
    expect(userBubbles(hook, 'refused before the switch')).toHaveLength(0);
    expect(userBubbles(hook, 'refused after the switch')).toHaveLength(0);

    await act(async () => {
      tid = 'thread-A';
      hook.rerender();
      await Promise.resolve();
    });
    await steer(hook, 'refused before the switch');
    await steer(hook, 'refused after the switch');
    expect(posts.slice(3).map((p) => p.message)).toEqual(['refused before the switch', 'refused after the switch']);
    expect(posts[3].opts.requestKey).not.toBe(posts[2].opts.requestKey);
    expect(posts[4].opts.requestKey).not.toBe(posts[1].opts.requestKey);
  });
});
