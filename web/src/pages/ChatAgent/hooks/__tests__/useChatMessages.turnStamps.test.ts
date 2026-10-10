/**
 * Every bubble a live action creates has to land on its backend turn, and a
 * paged thread gives no count to fall back on: its first bubble is turn 40,
 * not turn 0. Each action is pinned by the turn the projection gives its
 * bubbles, the reading edit, regenerate and feedback address the turn with.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus } from './chatHookHarness';

vi.mock('react-i18next', () => {
  const t = (k: string) => k;
  return { useTranslation: () => ({ t }) };
});

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import {
  fetchThreadTurns,
  getWorkflowStatus,
  reconnectToWorkflowStream,
  replayThreadHistory,
  sendChatMessageStream,
  sendHitlResponse,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { projectTurns } from '../../components/messageList/turnProjection';
import type { MessageRecord } from '../utils/types';

const mockFetchTurns = fetchThreadTurns as Mock;
const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSendStream = sendChatMessageStream as Mock;
const mockSendHitl = sendHitlResponse as Mock;

type Emit = (e: Record<string, unknown>) => void;

const chunk = (content: string) => ({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content });

const flush = () => new Promise((r) => setTimeout(r, 0));

/** The newest page of a thread: turns `from`..`to`, then whatever is still running. */
function pageOf(from: number, to: number, running: Record<string, unknown>[] = []) {
  const items: Record<string, unknown>[] = [{ event: 'history_page', first_turn_index: from, has_more: from > 0 }];
  for (let k = from; k <= to; k++) {
    items.push({ event: 'user_message', turn_index: k, content: `question ${k}`, run_id: `run-${k}` });
    items.push({ event: 'message_chunk', turn_index: k, role: 'assistant', agent: 'main', content_type: 'text', content: `answer ${k}` });
  }
  return [...items, ...running, { event: 'replay_done', thread_id: 'th-1' }];
}

/** One POST the test drives: its frames, then its end. */
interface OpenStream {
  emit: Emit;
  end: (result?: Record<string, unknown>) => void;
}

function captureStreams(mock: Mock): OpenStream[] {
  const streams: OpenStream[] = [];
  mock.mockImplementation(
    (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) =>
      new Promise((resolve) => {
        streams.push({ emit: onEvent, end: (result = { disconnected: false }) => resolve(result) });
      }),
  );
  return streams;
}

/** The turn the transcript projects for the bubble of `role` showing `text`. */
function turnOfText(messages: unknown[], role: 'user' | 'assistant', text: string): number {
  const entry = projectTurns(messages as MessageRecord[]).find(
    (p) => p.message.role === role && String(p.message.content ?? '').includes(text),
  );
  if (!entry) throw new Error(`no ${role} bubble shows ${text}`);
  return entry.turnIndex;
}

async function mountOnPage(replay: Record<string, unknown>[] = pageOf(40, 42)) {
  mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
    for (const item of replay) onEvent(item);
  });
  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  return hook;
}

type Hook = Awaited<ReturnType<typeof mountOnPage>>;

const live = (hook: Hook) => hook.result.current.liveMessages.get() as unknown[];

/** Sends `message` as a fresh turn the server answers with `answer`. */
async function sendTurn(hook: Hook, streams: OpenStream[], message: string, answer: string) {
  await act(async () => {
    const send = hook.result.current.handleSendMessage(message);
    await flush();
    const stream = streams.at(-1)!;
    stream.emit(chunk(answer));
    stream.end();
    await send;
  });
  await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
}

/** Turn 43 asks a question, and the answer resumes it as turn 44. */
async function askThenResume(hook: Hook, streams: OpenStream[]) {
  await act(async () => {
    const send = hook.result.current.handleSendMessage('screen chip names');
    await flush();
    streams.at(-1)!.emit(chunk('checking'));
    streams.at(-1)!.emit({
      event: 'interrupt',
      interrupt_id: 'int-ask',
      action_requests: [{ type: 'ask_user_question', question: 'Which tickers?', options: [], allow_multiple: false }],
    });
    streams.at(-1)!.end();
    await send;
  });
  await waitFor(() => expect(hook.result.current.pendingInterrupt).not.toBeNull());

  mockSendHitl.mockImplementation(async (...args: unknown[]) => {
    (args[3] as { onEvent: Emit }).onEvent(chunk('resumed answer'));
    return { disconnected: false };
  });
  await act(async () => {
    hook.result.current.handleAnswerQuestion('NVDA and AMD', 'int-ask', 'int-ask');
    await flush();
  });
  await waitFor(() => expect(mockSendHitl).toHaveBeenCalledTimes(1));
  await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
}

describe('useChatMessages: live bubbles land on their backend turn', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 42 }));
  });

  it('a steering message and the continuation it starts sit in the turn they steered', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);

    let send!: Promise<unknown>;
    await act(async () => {
      send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams[0].emit(chunk('working'));
      await flush();
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('also cover margins');
      await flush();
      streams[1].emit({ event: 'steering_accepted', position: 1 });
      streams[1].end();
      await flush();
    });
    expect(turnOfText(live(hook), 'user', 'also cover margins')).toBe(43);

    await act(async () => {
      streams[0].emit({ event: 'steering_delivered', messages: [{ content: 'also cover margins', timestamp: 1_700_000_000 }] });
      streams[0].emit(chunk('covering margins'));
      streams[0].end();
      await send;
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(turnOfText(live(hook), 'user', 'go on')).toBe(43);
    expect(turnOfText(live(hook), 'assistant', 'working')).toBe(43);
    expect(turnOfText(live(hook), 'user', 'also cover margins')).toBe(43);
    expect(turnOfText(live(hook), 'assistant', 'covering margins')).toBe(43);

    await sendTurn(hook, streams, 'next question', 'next answer');
    expect(turnOfText(live(hook), 'user', 'next question')).toBe(44);
    expect(turnOfText(live(hook), 'assistant', 'next answer')).toBe(44);
  });

  it('a steering message the server opened a new turn for takes the turn after', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);

    let send!: Promise<unknown>;
    await act(async () => {
      send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams[0].emit(chunk('working'));
      await flush();
    });
    let steer!: Promise<unknown>;
    await act(async () => {
      steer = hook.result.current.handleSendMessage('and then this');
      await flush();
      // The running turn ended before the POST landed: it opens turn 44.
      streams[1].emit({ event: 'metadata', thread_id: 'th-1', run_id: 'run-44' });
      streams[1].emit(chunk('demoted answer'));
      await flush();
    });

    await waitFor(() => expect(turnOfText(live(hook), 'assistant', 'demoted answer')).toBe(44));
    expect(turnOfText(live(hook), 'assistant', 'working')).toBe(43);
    expect(turnOfText(live(hook), 'user', 'and then this')).toBe(44);

    await act(async () => {
      streams[1].end();
      streams[0].end();
      await Promise.all([send, steer]);
    });
    expect(turnOfText(live(hook), 'user', 'and then this')).toBe(44);
    expect(turnOfText(live(hook), 'assistant', 'demoted answer')).toBe(44);
  });

  it('a resumed interrupt is a turn of its own, and the next send opens the one after', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    await askThenResume(hook, streams);

    expect(turnOfText(live(hook), 'assistant', 'checking')).toBe(43);
    expect(turnOfText(live(hook), 'assistant', 'resumed answer')).toBe(44);

    await sendTurn(hook, streams, 'next question', 'next answer');
    expect(turnOfText(live(hook), 'user', 'next question')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'next answer')).toBe(45);
  });

  it('a send counts the turn it opened as rendered, so the next check reloads nothing', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    await askThenResume(hook, streams);
    await sendTurn(hook, streams, 'next question', 'next answer');

    // The thread is at turn 45, and so is the transcript.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 45 }));
    await act(async () => {
      await hook.result.current.reconnectIfStaleRun();
    });
    expect(mockReplay).toHaveBeenCalledTimes(1);

    // A turn the view did not render still reloads.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 46 }));
    await act(async () => {
      await hook.result.current.reconnectIfStaleRun();
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
  });

  it('a stream that drops mid-turn continues on the same turn', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-43', latest_turn_index: 43 }));
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(chunk('part two'));
      return { disconnected: false, aborted: false };
    });

    await act(async () => {
      const send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams[0].emit({ event: 'metadata', thread_id: 'th-1', run_id: 'run-43' });
      streams[0].emit(chunk('part one'));
      streams[0].end({ disconnected: true });
      await send;
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 43 }));

    expect(turnOfText(live(hook), 'assistant', 'part one')).toBe(43);
    expect(turnOfText(live(hook), 'assistant', 'part two')).toBe(43);

    await sendTurn(hook, streams, 'next question', 'next answer');
    expect(turnOfText(live(hook), 'user', 'next question')).toBe(44);
    expect(turnOfText(live(hook), 'assistant', 'next answer')).toBe(44);
  });

  it('regenerating a turn a reconnect continued re-runs it whole', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-43', latest_turn_index: 43 }));
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(chunk('part two'));
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      const send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams[0].emit({ event: 'metadata', thread_id: 'th-1', run_id: 'run-43' });
      streams[0].emit(chunk('part one'));
      streams[0].end({ disconnected: true });
      await send;
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 43 }));
    mockFetchTurns.mockResolvedValue({
      turns: [{ turn_index: 43, edit_checkpoint_id: 'cp-42', regenerate_checkpoint_id: 'cp-43' }],
    });

    const tail = (live(hook) as MessageRecord[]).find((m) => String(m.content ?? '').includes('part two'));
    await act(async () => {
      const regenerate = hook.result.current.handleRegenerate(tail!.id as string);
      await flush();
      streams.at(-1)!.emit(chunk('fresh answer'));
      streams.at(-1)!.end();
      await regenerate;
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const text = JSON.stringify(live(hook));
    expect(text).not.toContain('part one');
    expect(text).not.toContain('part two');
    expect(turnOfText(live(hook), 'user', 'go on')).toBe(43);
    expect(turnOfText(live(hook), 'assistant', 'fresh answer')).toBe(43);
  });

  it('steering another tab delivered sits in the turn the reconnect continues', async () => {
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-43', latest_turn_index: 43 }));
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(chunk('part one'));
      onEvent({ event: 'steering_delivered', messages: [{ content: 'also cover margins', timestamp: 1_700_000_000 }] });
      onEvent(chunk('covering margins'));
      return { disconnected: false, aborted: false };
    });
    const running = [{ event: 'user_message', turn_index: 43, content: 'go on', run_id: 'run-43' }];
    const hook = await mountOnPage(pageOf(40, 42, running));
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 43 }));

    expect(turnOfText(live(hook), 'user', 'go on')).toBe(43);
    expect(turnOfText(live(hook), 'assistant', 'part one')).toBe(43);
    expect(turnOfText(live(hook), 'user', 'also cover margins')).toBe(43);
    expect(turnOfText(live(hook), 'assistant', 'covering margins')).toBe(43);

    const streams = captureStreams(mockSendStream);
    await sendTurn(hook, streams, 'next question', 'next answer');
    expect(turnOfText(live(hook), 'user', 'next question')).toBe(44);
    expect(turnOfText(live(hook), 'assistant', 'next answer')).toBe(44);
  });

  it('a send the replay already holds takes the turn the replay names', async () => {
    const opened = { open: () => {} };
    const gate = new Promise<void>((resolve) => {
      opened.open = resolve;
    });
    const running = [{ event: 'user_message', turn_index: 43, content: 'go on', run_id: 'run-43' }];
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      await gate;
      for (const item of pageOf(40, 42, running)) onEvent(item);
    });
    const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    const streams = captureStreams(mockSendStream);

    // Sent while the transcript is still on its way: nothing on screen counts
    // the turn, so the replay is what names the live bubble's.
    let send!: Promise<unknown>;
    await act(async () => {
      send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams[0].emit(chunk('working'));
      await flush();
    });
    await act(async () => {
      opened.open();
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    expect(turnOfText(live(hook), 'assistant', 'answer 42')).toBe(42);
    expect(turnOfText(live(hook), 'assistant', 'working')).toBe(43);
    // The send's own bubble is the only one for the turn: the replay's is dropped.
    expect(turnOfText(live(hook), 'user', 'go on')).toBe(43);

    await act(async () => {
      streams[0].end();
      await send;
    });
  });
});
