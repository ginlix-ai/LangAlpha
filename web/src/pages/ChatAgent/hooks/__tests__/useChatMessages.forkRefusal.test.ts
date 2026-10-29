/**
 * An edit or regenerate the server refuses at admission changed nothing on the
 * thread: refused as `stale_fork` it forked a branch the thread no longer has,
 * refused as busy the turns it cut are all still there. The route refuses with
 * a 409, and the stream with an SSE error frame once it is open. Either way
 * the transcript the fork cut must not stay cut: the view reloads the thread,
 * says why, and hands an edit's text back to send again. An answer to a pause
 * a fork elsewhere replaced is refused as `stale_fork` and reloads the same
 * way, since that pause can never be answered.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { captureWatchCalls, settleMountEffect, threadStatus } from './chatHookHarness';

// As in react-i18next, each render's `t` is bound to the language shown then.
const locale = vi.hoisted(() => {
  const bound = new Map<string, (k: string) => string>();
  const state = { lang: 'en', t: (k: string) => k };
  return {
    state,
    use: (lang: string) => {
      if (!bound.has(lang)) bound.set(lang, lang === 'en' ? (k) => k : (k) => `${lang}:${k}`);
      state.lang = lang;
      state.t = bound.get(lang)!;
    },
  };
});

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: locale.state.t }) }));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { fetchThreadTurns, getReportBackStatus, getWorkflowStatus, reconnectToWorkflowStream, replayThreadHistory, sendChatMessageStream, sendHitlResponse, sendRetryStream, watchThread } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage } from '@/types/chat';

beforeEach(() => locale.use('en'));

const mockFetchTurns = fetchThreadTurns as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSendStream = sendChatMessageStream as Mock;
const mockRetryStream = sendRetryStream as Mock;
const mockSendHitl = sendHitlResponse as Mock;
const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;

type Emit = (e: Record<string, unknown>) => void;

// Not the server's wording: the view shows whatever the refusal says.
const REFUSAL = 'the branch moved';
// A busy refusal shows in the view's words instead, keyed by its code.
const BUSY = 'a turn is still running';
const BUSY_NOTICE = { message: 'chat.busyRunning', kind: 'notice' };

/** Turns 0 to `to`; `latestRunId` is the newest run the server names, which
 *  a server too old to name one leaves out. */
function pageOf(to: number, latestRunId?: string | null) {
  const items: Record<string, unknown>[] = [{ event: 'history_page', first_turn_index: 0, has_more: false }];
  for (let k = 0; k <= to; k++) {
    items.push({ event: 'user_message', turn_index: k, content: `question ${k}`, run_id: `run-${k}` });
    items.push({ event: 'message_chunk', turn_index: k, role: 'assistant', agent: 'main', content_type: 'text', content: `answer ${k}` });
  }
  const done = latestRunId === undefined ? {} : { latest_run_id: latestRunId };
  return [...items, { event: 'replay_done', thread_id: 'th-1', ...done }];
}

const refusalFrame = (code: string, message: string) => ({
  event: 'error',
  thread_id: 'th-1',
  error: message,
  type: 'workflow_error',
  error_type: 'admission_conflict',
  code,
});

const refusals = {
  route: (code = 'stale_fork', message = REFUSAL) =>
    mockSendStream.mockRejectedValue(
      Object.assign(new Error(message), { status: 409, errorInfo: { code, message } }),
    ),
  frame: (code = 'stale_fork', message = REFUSAL) =>
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string, { onEvent }: { onEvent: Emit }) => {
      onEvent(refusalFrame(code, message));
      return { disconnected: false };
    }),
};

const texts = (messages: unknown[], role: 'user' | 'assistant') =>
  (messages as { role: string; content?: unknown }[])
    .filter((m) => m.role === role)
    .map((m) => String(m.content ?? ''));

/** Mounts on `first`, turns 0-2 by default; each replay after it serves the
 *  next page. */
async function mountThreadOn(first: ReturnType<typeof pageOf>, ...reloads: ReturnType<typeof pageOf>[]) {
  const pages = [first, ...reloads];
  mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
    for (const item of pages.shift() ?? []) onEvent(item);
  });
  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  return hook;
}

const mountThread = (...reloads: ReturnType<typeof pageOf>[]) => mountThreadOn(pageOf(2), ...reloads);

type Hook = Awaited<ReturnType<typeof mountThread>>;

/** The text of the edit box open over question 1, if one is. */
const openEdit = (result: Hook['result']) => result.current.editDrafts.get({ turnIndex: 1, original: 'question 1' });

async function fork(result: Hook['result'], action: 'edit' | 'regenerate') {
  const messages = result.current.messages as { id: string; role: string; content?: unknown }[];
  await act(async () => {
    if (action === 'edit') {
      const target = messages.find((m) => m.role === 'user' && m.content === 'question 1')!;
      await result.current.handleEditMessage(target.id, 'question 1 rewritten');
    } else {
      const target = messages.find((m) => m.role === 'assistant' && String(m.content).includes('answer 1'))!;
      await result.current.handleRegenerate(target.id);
    }
  });
}

describe('useChatMessages – a fork refused at admission', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchTurns.mockResolvedValue({
      turns: [0, 1, 2].map((k) => ({ turn_index: k, edit_checkpoint_id: `cp-in-${k}`, regenerate_checkpoint_id: `cp-${k}` })),
    });
  });

  it.each([
    ['edit', 'route'],
    ['edit', 'frame'],
    ['regenerate', 'route'],
    ['regenerate', 'frame'],
  ] as const)('reloads the thread after a refused %s (%s)', async (action, refusal) => {
    // The view holds turns 0-2; another tab has since added turn 3.
    const { result } = await mountThread(pageOf(3));
    refusals[refusal]();

    await fork(result, action);

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2', 'question 3']);
    expect(texts(result.current.messages, 'assistant')).toEqual(['answer 0', 'answer 1', 'answer 2', 'answer 3']);
    // The view's own words, not the server's: the user did not see the chat change.
    expect(result.current.messageError).toEqual({ message: action === 'edit' ? 'chat.forkStaleEdit' : 'chat.forkStaleRegenerate', kind: 'notice' });
    expect(result.current.isLoading).toBe(false);
    // An edit's text opens again in the box of the message it was typed over.
    expect(openEdit(result)).toBe(action === 'edit' ? 'question 1 rewritten' : undefined);
    expect(result.current.returnedEdit).toBeNull();
  });

  it('hands a refused edit to the composer when its message is gone after the reload', async () => {
    // Another tab replaced turn 1 meanwhile.
    const moved = pageOf(2).map((item) =>
      item.event === 'user_message' && item.turn_index === 1 ? { ...item, content: 'question 1 from elsewhere' } : item);
    const { result } = await mountThread(moved);
    refusals.route();

    await fork(result, 'edit');

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.returnedEdit).toBe('question 1 rewritten'));
    expect(openEdit(result)).toBeUndefined();
    expect(result.current.messageError).toEqual({ message: 'chat.forkStaleEdit', kind: 'notice' });
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1 from elsewhere', 'question 2']);
  });

  it('puts back the edit boxes open on the messages a refused fork cut', async () => {
    const { result } = await mountThread(pageOf(2));
    act(() => result.current.editDrafts.put({ turnIndex: 2, original: 'question 2', text: 'question 2 half typed' }));
    refusals.frame('running', BUSY);

    await fork(result, 'regenerate');

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2']);
    expect(result.current.editDrafts.get({ turnIndex: 2, original: 'question 2' })).toBe('question 2 half typed');
    expect(result.current.returnedEdit).toBeNull();
  });

  it('reads the turns afresh for each fork', async () => {
    // The first edit finds no turn for its message: the chat changed, and the
    // reload shows the turns the server holds now.
    mockFetchTurns.mockResolvedValueOnce({ turns: [{ turn_index: 0, edit_checkpoint_id: 'cp-in-0', regenerate_checkpoint_id: 'cp-0' }] });
    const { result } = await mountThread(pageOf(2));
    await fork(result, 'edit');
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    mockSendStream.mockResolvedValue({ disconnected: false, aborted: false });

    await fork(result, 'edit');

    expect(mockFetchTurns).toHaveBeenCalledTimes(2);
    expect(mockSendStream.mock.lastCall?.[3]).toMatchObject({ checkpointId: 'cp-in-1', forkFromTurn: 1 });
  });

  it('gives an edit back when its checkpoint read fails', async () => {
    const { result } = await mountThread();
    mockFetchTurns.mockRejectedValue(new Error('network down'));

    await fork(result, 'edit');

    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2']);
    expect(openEdit(result)).toBe('question 1 rewritten');
    expect(mockSendStream).not.toHaveBeenCalled();
  });

  it('puts back the transcript from before the fork when its automatic resend is refused', async () => {
    const { result } = await mountThread(pageOf(2));
    // The server started no run and asks for the request again; the resend
    // then finds the thread busy.
    mockSendStream
      .mockImplementationOnce(async (_msg: string, _ws: string, _tid: string, { onEvent }: { onEvent: Emit }) => {
        onEvent({ event: 'retry', thread_id: 'th-1', auto_retry: true, retry_count: 1, max_retries: 3, recovery: 'resend' });
        return { disconnected: false, aborted: false };
      })
      .mockRejectedValueOnce(Object.assign(new Error(BUSY), { status: 409, errorInfo: { code: 'running', message: BUSY } }));

    await fork(result, 'edit');

    await waitFor(() => expect(mockSendStream).toHaveBeenCalledTimes(2), { timeout: 3000 });
    expect(mockSendStream.mock.calls[1][0]).toBe('question 1 rewritten');
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    // The cut transcript held the edit's own bubbles, which a reload keeps.
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2']);
    expect(texts(result.current.messages, 'assistant')).toEqual(['answer 0', 'answer 1', 'answer 2']);
    expect(result.current.messageError).toEqual(BUSY_NOTICE);
    expect(openEdit(result)).toBe('question 1 rewritten');
  });

  it("puts back the transcript from before the fork when its reply's Retry is refused", async () => {
    const { result } = await mountThread(pageOf(2));
    // The server started no run and stopped asking for the request again, so
    // the reply's Retry holds the fork.
    mockSendStream
      .mockImplementationOnce(async (_msg: string, _ws: string, _tid: string, { onEvent }: { onEvent: Emit }) => {
        onEvent({ event: 'retry', thread_id: 'th-1', auto_retry: false, retry_count: 1, max_retries: 3, recovery: 'resend' });
        return { disconnected: false, aborted: false };
      })
      .mockRejectedValueOnce(Object.assign(new Error(BUSY), { status: 409, errorInfo: { code: 'running', message: BUSY } }));

    await fork(result, 'edit');
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1 rewritten']);
    await act(async () => {
      await result.current.handleRetry();
    });

    expect(mockSendStream.mock.calls[1][0]).toBe('question 1 rewritten');
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2']);
    expect(result.current.messageError).toEqual(BUSY_NOTICE);
    expect(openEdit(result)).toBe('question 1 rewritten');
  });

  it.each([
    ['edit', 'frame'],
    ['edit', 'route'],
    ['regenerate', 'frame'],
  ] as const)('puts back the turns a refused %s cut when the thread is busy (%s)', async (action, refusal) => {
    // Nothing moved: the reload serves the same turns the view already held.
    const { result } = await mountThread(pageOf(2));
    refusals[refusal]('running', BUSY);

    await fork(result, action);

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2']);
    expect(texts(result.current.messages, 'assistant')).toEqual(['answer 0', 'answer 1', 'answer 2']);
    expect(result.current.messageError).toEqual(BUSY_NOTICE);
    expect(result.current.isLoading).toBe(false);
    expect(openEdit(result)).toBe(action === 'edit' ? 'question 1 rewritten' : undefined);
  });

  it.each([
    ['stopping', 'chat.busyStopping'],
    ['compacting', 'chat.busyCompacting'],
  ] as const)('says in its own words that the thread was %s once the reload finds the turn over', async (code, key) => {
    const { result } = await mountThread(pageOf(2));
    refusals.frame(code, 'the server says to wait');

    await fork(result, 'regenerate');

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(result.current.messageError).toEqual({ message: key, kind: 'notice' });
  });

  it('says the thread is busy in the language shown when the refusal comes', async () => {
    const { result, rerender } = await mountThread(pageOf(2));
    locale.use('zh');
    rerender();
    refusals.route('running', BUSY);

    await fork(result, 'edit');

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(result.current.messageError).toEqual({ message: 'zh:chat.busyRunning', kind: 'notice' });
  });

  it.each([
    ['running', BUSY, null],
    ['stale_fork', REFUSAL, { message: 'chat.forkStaleEdit', kind: 'notice' }],
  ] as const)('a refusal as %s whose reload follows the running turn', async (code, message, shown) => {
    const { result } = await mountThread(pageOf(2));
    // The turn the server named is still running when the reload reads it.
    mockStatus.mockResolvedValueOnce(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-live' }));
    refusals.frame(code, message);

    await fork(result, 'edit');

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    // A busy refusal is the turn the view now shows, and stale once it ends;
    // the chat changing stays true.
    expect(result.current.messageError).toEqual(shown);
    expect(openEdit(result)).toBe('question 1 rewritten');
  });

  it('a retry refused as busy says so in its reply, in the view\'s words, and does not reload', async () => {
    // A retry cut no turns, so there is nothing to put back.
    const { result } = await mountThread();
    mockRetryStream.mockImplementation(async (_ws: string, _tid: string, onEvent: Emit) => {
      onEvent(refusalFrame('running', BUSY));
      return { disconnected: false };
    });

    await act(async () => {
      await result.current.handleRetry();
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(mockRetryStream).toHaveBeenCalledTimes(1);
    expect(mockReplay).toHaveBeenCalledTimes(1);
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2']);
    expect(texts(result.current.messages, 'assistant')).toEqual(['answer 0', 'answer 1', 'answer 2', '']);
    expect(result.current.messages.at(-1)).toMatchObject({ error: true, structuredError: { message: 'chat.busyRunningSend', kind: 'notice' } });
    expect(result.current.messageError).toBeNull();
  });
});

describe('useChatMessages – a fork names the newest run the view saw', () => {
  type SendOptions = { expectedLatestRunId?: string | null; onEvent: Emit; onRunIdResolved?: (id: string) => void };
  const sentWith = () => mockSendStream.mock.lastCall?.[3] as SendOptions;

  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchTurns.mockResolvedValue({
      turns: [0, 1, 2, 3].map((k) => ({ turn_index: k, edit_checkpoint_id: `cp-in-${k}`, regenerate_checkpoint_id: `cp-${k}` })),
    });
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string, { onRunIdResolved, onEvent }: SendOptions) => {
      const runId = `run-sent-${mockSendStream.mock.calls.length}`;
      onRunIdResolved?.(runId);
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: runId });
      return { disconnected: false, aborted: false };
    });
  });

  it('names the run the replay ended on, then each run it started since', async () => {
    const { result } = await mountThreadOn(pageOf(2, 'run-2'));

    await fork(result, 'regenerate');
    expect(sentWith().expectedLatestRunId).toBe('run-2');
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    await fork(result, 'edit');
    expect(sentWith().expectedLatestRunId).toBe('run-sent-1');
  });

  it('names a run only once the server started it, never one it refused', async () => {
    const { result } = await mountThreadOn(pageOf(2, 'run-2'));
    // The header names the run before the server starts it; this one was
    // refused at capacity, so the thread's newest is still run-2.
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string, { onRunIdResolved, onEvent }: SendOptions) => {
      onRunIdResolved?.('run-refused');
      onEvent({ event: 'error', thread_id: 'th-1', error: 'at capacity', type: 'workflow_error', error_type: 'capacity_limit', code: 'writer_capacity' });
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      await result.current.handleSendMessage('question 3');
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    await fork(result, 'edit');
    expect(sentWith().expectedLatestRunId).toBe('run-2');
  });

  it('names a run whose failure frame names it', async () => {
    const { result } = await mountThreadOn(pageOf(2, 'run-2'));
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string, { onRunIdResolved, onEvent }: SendOptions) => {
      onRunIdResolved?.('run-failed');
      onEvent({ event: 'error', thread_id: 'th-1', error: 'boom', type: 'workflow_error', error_type: 'unknown', recovery: 'retry', run_id: 'run-failed' });
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      await result.current.handleSendMessage('question 3');
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    await fork(result, 'edit');
    expect(sentWith().expectedLatestRunId).toBe('run-failed');
  });

  it('names none when the server named none, and the fork goes unchecked', async () => {
    const { result } = await mountThread();

    await fork(result, 'edit');

    expect(sentWith().expectedLatestRunId).toBeNull();
  });

  it.each(['route', 'frame'] as const)(
    'retries the newest run it saw, and reloads when another came since (%s)',
    async (refusal) => {
      const stale = { code: 'stale_retry', message: 'The requested run is no longer the latest attempt.' };
      const { result } = await mountThreadOn(pageOf(2, 'run-2'), pageOf(3, 'run-3'));
      if (refusal === 'route') {
        mockRetryStream.mockRejectedValue(Object.assign(new Error(stale.message), { status: 409, errorInfo: stale }));
      } else {
        mockRetryStream.mockImplementation(async (_ws: string, _tid: string, onEvent: Emit) => {
          onEvent(refusalFrame(stale.code, stale.message));
          return { disconnected: false };
        });
      }

      await act(async () => {
        await result.current.handleRetry();
      });

      // The run is the Retry's tenth argument.
      expect(mockRetryStream.mock.calls[0][9]).toBe('run-2');
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
      expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2', 'question 3']);
      expect(texts(result.current.messages, 'assistant')).toEqual(['answer 0', 'answer 1', 'answer 2', 'answer 3']);
      expect(result.current.messageError).toEqual({ message: 'chat.forkStaleRegenerate', kind: 'notice' });
      expect(result.current.isLoading).toBe(false);
    },
  );

  it('keeps a run a report-back attach saw while a reload ran over the older run the reload names', async () => {
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, latest_turn_index: 2 }));
    (getReportBackStatus as Mock).mockImplementation((...args: unknown[]) => mockStatus(...args));
    const watchCalls = captureWatchCalls(watchThread as Mock);
    const { result } = await mountThreadOn(pageOf(2, 'run-2'));
    await waitFor(() => expect(watchCalls).toHaveLength(1));

    // Another tab added turn 3. The reload's replay read run-3 as the newest
    // and its last frame is still on the way when the report-back run starts.
    let finishReplay!: () => void;
    const replayEnds = new Promise<void>((resolve) => { finishReplay = resolve; });
    mockReplay.mockImplementationOnce(async (_tid: string, onEvent: Emit) => {
      const page = pageOf(3, 'run-3');
      for (const item of page.slice(0, -1)) onEvent(item);
      await replayEnds;
      onEvent(page[page.length - 1]);
    });
    mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, latest_turn_index: 3 }));
    await act(async () => {
      await result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));

    mockReconnect.mockImplementation(async (_tid: string, runId: string, _cursor: unknown, onEvent: Emit) => {
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: runId });
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'report' });
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-run-4' });
    });
    expect(mockReconnect.mock.calls.map((call) => call[1])).toContain('rb-run-4');
    await act(async () => finishReplay());
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    await waitFor(() => expect(result.current.isLoading).toBe(false));

    await fork(result, 'edit');
    expect(sentWith().expectedLatestRunId).toBe('rb-run-4');
  });
});

const INTERRUPT_ID = 'int-1';

describe('useChatMessages – an answer to a pause a fork replaced', () => {
  const pauseWith = (interruptId: string) =>
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string, { onEvent }: { onEvent: Emit }) => {
      onEvent({
        event: 'interrupt',
        interrupt_id: interruptId,
        action_requests: [{ name: 'mcp__broker__order_place', args: { code: 'US.AAPL', qty: '1' } }],
      });
      return { disconnected: false };
    });

  beforeEach(() => {
    vi.clearAllMocks();
    mockFetchTurns.mockResolvedValue({ turns: [] });
    pauseWith(INTERRUPT_ID);
  });

  const approvalCard = (messages: readonly unknown[], interruptId = INTERRUPT_ID) =>
    (messages as AssistantMessage[])
      .filter((m) => m.role === 'assistant')
      .map((m) => m.toolApprovals?.[interruptId])
      .find(Boolean);

  async function approve(result: Hook['result'], interruptId: string) {
    await act(async () => {
      result.current.handleApproveToolCall(interruptId, interruptId, { index: 0, count: 1 });
      await new Promise((r) => setTimeout(r, 0));
    });
  }

  const refusedAnswers = {
    route: () =>
      mockSendHitl.mockRejectedValue(
        Object.assign(new Error(REFUSAL), { status: 409, errorInfo: { code: 'stale_fork', message: REFUSAL } }),
      ),
    // The run id arrives with the headers, before START refuses the answer.
    frame: () =>
      mockSendHitl.mockImplementation(
        async (_answer: unknown, _ws: string, _tid: string, { onEvent, onRunIdResolved }: { onEvent: Emit; onRunIdResolved: (id: string) => void }) => {
          onRunIdResolved('run-never-started');
          onEvent(refusalFrame('stale_fork', REFUSAL));
          return { disconnected: false, aborted: false };
        },
      ),
  };

  it.each(['route', 'frame'] as const)('reloads the thread instead of putting the card back (%s)', async (refusal) => {
    // Another tab replaced the paused turn meanwhile.
    const replaced = pageOf(3).map((item) =>
      item.event === 'user_message' && item.turn_index === 3 ? { ...item, content: 'question 3 from elsewhere' } : item);
    const { result } = await mountThread(replaced);
    await act(async () => {
      await result.current.handleSendMessage('question 3');
    });
    await waitFor(() => expect(approvalCard(result.current.messages)?.status).toBe('pending'));
    refusedAnswers[refusal]();

    await approve(result, INTERRUPT_ID);

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(mockSendHitl).toHaveBeenCalledTimes(1);
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2', 'question 3 from elsewhere']);
    expect(texts(result.current.messages, 'assistant')).toEqual(['answer 0', 'answer 1', 'answer 2', 'answer 3']);
    expect(approvalCard(result.current.messages)).toBeUndefined();
    expect(result.current.messageError).toEqual({ message: 'chat.forkStaleRegenerate', kind: 'notice' });
    expect(result.current.isLoading).toBe(false);

    // Nothing is owed to the replaced pause, so the next pause's answer goes out.
    pauseWith('int-2');
    await act(async () => {
      await result.current.handleSendMessage('question 4');
    });
    await waitFor(() => expect(approvalCard(result.current.messages, 'int-2')?.status).toBe('pending'));
    mockSendHitl.mockReset();
    mockSendHitl.mockResolvedValue({ disconnected: false, aborted: false });
    await approve(result, 'int-2');
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalledTimes(1));
    expect(Object.keys(mockSendHitl.mock.calls[0][0])).toEqual(['int-2']);
  });
});

describe('useChatMessages – a send from a view that missed a turn', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('reloads to show the turn it missed once its stream ends', async () => {
    // The view holds turns 0-2; another tab has since added turn 3.
    const { result } = await mountThreadOn(pageOf(2), pageOf(4));
    let endStream = () => {};
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-4', turn_index: 4 });
      onEvent({ event: 'message_chunk', thread_id: 'th-1', agent: 'main', role: 'assistant', content_type: 'text', content: 'answer 4' });
      await new Promise<void>((resolve) => { endStream = resolve; });
      return { disconnected: false, aborted: false };
    });

    let sent: Promise<void> = Promise.resolve();
    act(() => {
      sent = result.current.handleSendMessage('question 4');
    });

    // Numbered 3 by the view, stamped 4 by the stream; the reload waits for it.
    await waitFor(() =>
      expect(result.current.messages.slice(-2).map((m) => (m as { turnIndex?: number }).turnIndex)).toEqual([4, 4]));
    expect(mockReplay).toHaveBeenCalledTimes(1);
    await act(async () => {
      endStream();
      await sent;
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
    expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2', 'question 3', 'question 4']);
  });

  it('does not reload for a send the view numbered right', async () => {
    const { result } = await mountThreadOn(pageOf(2));
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-3', turn_index: 3 });
      return { disconnected: false, aborted: false };
    });

    await act(async () => {
      await result.current.handleSendMessage('question 3');
    });

    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(mockReplay).toHaveBeenCalledTimes(1);
    expect(result.current.messages.slice(-2).map((m) => (m as { turnIndex?: number }).turnIndex)).toEqual([3, 3]);
  });
});

describe('useChatMessages – a Retry the server refuses', () => {
  type SendOptions = { onEvent: Emit; onRunIdResolved?: (id: string) => void };

  beforeEach(() => {
    vi.clearAllMocks();
  });

  /** Sends question 3, whose reply the view marks failed on a failure frame
   *  that names run-3. */
  async function failReplyTo3(result: Hook['result']) {
    mockSendStream.mockImplementationOnce(async (_msg: string, _ws: string, _tid: string, { onRunIdResolved, onEvent }: SendOptions) => {
      onRunIdResolved?.('run-3');
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-3', turn_index: 3 });
      onEvent({ event: 'error', thread_id: 'th-1', error: 'boom', type: 'workflow_error', error_type: 'unknown', recovery: 'retry', run_id: 'run-3' });
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      await result.current.handleSendMessage('question 3');
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect((result.current.messages.at(-1) as AssistantMessage).error).toBeTruthy();
  }

  const refuseRetry = (via: 'route' | 'frame', detail: { code: string; message: string } & Record<string, unknown>) => {
    if (via === 'route') {
      mockRetryStream.mockRejectedValue(Object.assign(new Error(detail.message), { status: 409, errorInfo: detail }));
    } else {
      mockRetryStream.mockImplementation(async (_ws: string, _tid: string, onEvent: Emit) => {
        onEvent(refusalFrame(detail.code, detail.message));
        return { disconnected: false };
      });
    }
  };

  const failed = (messages: readonly unknown[]) =>
    (messages as AssistantMessage[]).filter((m) => m.role === 'assistant' && m.error);

  it.each(['route', 'frame'] as const)(
    'shows the reply the server holds when the run it retries did not fail, with no notice (%s)',
    async (via) => {
      // The server finished run-3 after all.
      const { result } = await mountThreadOn(pageOf(2, 'run-2'), pageOf(3, 'run-3'));
      await failReplyTo3(result);
      refuseRetry(via, {
        code: 'not_retryable',
        message: 'Latest run is completed; only failed runs can be retried.',
        latest_run_id: 'run-3',
        latest_status: 'completed',
      });

      await act(async () => {
        await result.current.handleRetry();
      });

      expect(mockRetryStream.mock.calls[0][9]).toBe('run-3');
      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
      expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2', 'question 3']);
      expect(texts(result.current.messages, 'assistant')).toEqual(['answer 0', 'answer 1', 'answer 2', 'answer 3']);
      expect(failed(result.current.messages)).toEqual([]);
      expect(result.current.messageError).toBeNull();
      expect(result.current.isLoading).toBe(false);
    },
  );

  it.each(['route', 'frame'] as const)(
    'gives the question back to the composer when its input has to be sent again (%s)',
    async (via) => {
      // The server holds turn 3's question and no reply to it.
      const unanswered = pageOf(3, 'run-3').filter((item) => !(item.event === 'message_chunk' && item.turn_index === 3));
      const { result } = await mountThreadOn(pageOf(2, 'run-2'), unanswered);
      await failReplyTo3(result);
      refuseRetry(via, { code: 'resend_required', message: "The failed turn's input was more than its stored message; send it again." });

      await act(async () => {
        await result.current.handleRetry();
      });

      await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
      await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));
      expect(result.current.returnedEdit).toBe('question 3');
      expect(result.current.messageError).toEqual({ message: 'chat.resendRequired', kind: 'notice' });
      expect(texts(result.current.messages, 'user')).toEqual(['question 0', 'question 1', 'question 2', 'question 3']);
      expect(result.current.isLoading).toBe(false);
    },
  );

  it('says to send the question again in the language shown when the refusal comes', async () => {
    const unanswered = pageOf(3, 'run-3').filter((item) => !(item.event === 'message_chunk' && item.turn_index === 3));
    const { result, rerender } = await mountThreadOn(pageOf(2, 'run-2'), unanswered);
    await failReplyTo3(result);
    locale.use('zh');
    rerender();
    refuseRetry('route', { code: 'resend_required', message: "The failed turn's input was more than its stored message; send it again." });

    await act(async () => {
      await result.current.handleRetry();
    });

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    expect(result.current.messageError).toEqual({ message: 'zh:chat.resendRequired', kind: 'notice' });
  });

  it('says to send the last message again when the reply answered a pause rather than the question', async () => {
    const { result } = await mountThreadOn(pageOf(2, 'run-2'), pageOf(2, 'run-2'));
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'interrupt', interrupt_id: INTERRUPT_ID, action_requests: [{ name: 'mcp__broker__order_place', args: { code: 'US.AAPL', qty: '1' } }] });
      return { disconnected: false };
    });
    await act(async () => {
      await result.current.handleSendMessage('question 3');
    });
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    // The answer's run fails where the view can only offer Retry.
    mockSendHitl.mockImplementation(async (_answer: unknown, _ws: string, _tid: string, { onEvent, onRunIdResolved }: SendOptions) => {
      onRunIdResolved?.('run-answer');
      onEvent({ event: 'error', thread_id: 'th-1', error: 'boom', type: 'workflow_error', error_type: 'unknown', recovery: 'retry', run_id: 'run-answer' });
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      result.current.handleApproveToolCall(INTERRUPT_ID, INTERRUPT_ID, { index: 0, count: 1 });
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(failed(result.current.messages)).toHaveLength(1));
    const resend = "The failed turn's input was more than its stored message; send it again.";
    refuseRetry('route', { code: 'resend_required', message: resend });

    await act(async () => {
      await result.current.handleRetry();
    });

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    expect(result.current.returnedEdit).toBeNull();
    expect(result.current.messageError).toEqual({ message: 'chat.resendLastMessage', kind: 'notice' });
  });
});
