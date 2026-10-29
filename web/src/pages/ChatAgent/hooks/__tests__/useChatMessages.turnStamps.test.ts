/**
 * Every bubble a live action creates has to land on its backend turn, and a
 * paged thread gives no count to fall back on: its first bubble is turn 40,
 * not turn 0. Each action is pinned by the turn the projection gives its
 * bubbles, the reading edit, regenerate and feedback address the turn with.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach, onTestFinished } from 'vitest';
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
  if (!entry || entry.turnIndex === null) throw new Error(`no ${role} bubble shows ${text}`);
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

  it('a steer the server refused as not_running takes the turn its resend opens', async () => {
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
      const steer = hook.result.current.handleSendMessage('and then this');
      await flush();
      // The running turn ended before the POST landed: the steer is refused.
      streams[1].emit({ event: 'error', error_type: 'admission_conflict', code: 'not_running', error: 'The turn has ended.' });
      streams[1].end();
      await steer;
      streams[0].end();
      await send;
      await flush();
    });
    // Once the view lets turn 43 go, the steer goes out as a send: turn 44.
    await waitFor(() => expect(streams).toHaveLength(3));
    await act(async () => {
      streams[2].emit({ event: 'metadata', thread_id: 'th-1', run_id: 'run-44' });
      streams[2].emit(chunk('resent answer'));
      await flush();
    });

    await waitFor(() => expect(turnOfText(live(hook), 'assistant', 'resent answer')).toBe(44));
    expect(turnOfText(live(hook), 'assistant', 'working')).toBe(43);
    expect(turnOfText(live(hook), 'user', 'and then this')).toBe(44);

    await act(async () => {
      streams[2].end();
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    expect(turnOfText(live(hook), 'user', 'and then this')).toBe(44);
    expect(turnOfText(live(hook), 'assistant', 'resent answer')).toBe(44);
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

  it('regenerating the turn after a refused send keeps the question it ran', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    // Refused before admission: the server holds no turn for it.
    mockSendStream.mockRejectedValueOnce(Object.assign(new Error('Server error 500'), { status: 500 }));
    await act(async () => {
      await hook.result.current.handleSendMessage('first try');
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    await act(async () => {
      const send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams.at(-1)!.emit({ event: 'metadata', thread_id: 'th-1', run_id: 'run-43', turn_index: 43 });
      streams.at(-1)!.emit(chunk('the answer'));
      streams.at(-1)!.end();
      await send;
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    // The refused send holds no turn; the send after it ran turn 43.
    const firstTry = projectTurns(live(hook) as MessageRecord[]).find((p) => p.message.content === 'first try');
    expect(firstTry?.turnIndex).toBeNull();
    expect(turnOfText(live(hook), 'user', 'go on')).toBe(43);

    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 43 }));
    mockFetchTurns.mockResolvedValue({
      turns: [{ turn_index: 43, edit_checkpoint_id: 'cp-42', regenerate_checkpoint_id: 'cp-43' }],
    });
    const answer = (live(hook) as MessageRecord[]).find((m) => String(m.content ?? '').includes('the answer'));
    await act(async () => {
      const regenerate = hook.result.current.handleRegenerate(answer!.id as string);
      await flush();
      streams.at(-1)!.emit(chunk('fresh answer'));
      streams.at(-1)!.end();
      await regenerate;
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    // The re-run answers the question the turn ran, which stays above it.
    const messages = live(hook) as MessageRecord[];
    const fresh = messages.findIndex((m) => String(m.content ?? '').includes('fresh answer'));
    const asked = messages.slice(0, fresh).findLast((m) => m.role === 'user');
    expect(asked?.content).toBe('go on');
    expect(JSON.stringify(messages)).not.toContain('the answer');
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

  it('a steer another surface delivered keeps the label of the chat app it came from', async () => {
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-43', latest_turn_index: 43 }));
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(chunk('part one'));
      onEvent({
        event: 'steering_delivered',
        messages: [
          { content: 'from the phone', timestamp: 1_700_000_000, platform: 'telegram' },
          { content: 'from the web', timestamp: 1_700_000_001, platform: 'web' },
        ],
      });
      return { disconnected: false, aborted: false };
    });
    const running = [{ event: 'user_message', turn_index: 43, content: 'go on', run_id: 'run-43' }];
    const hook = await mountOnPage(pageOf(40, 42, running));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const users = live(hook) as MessageRecord[];
    expect(users.find((m) => m.content === 'from the phone')?.provenance).toEqual({ kind: 'channel', platform: 'telegram' });
    expect(users.find((m) => m.content === 'from the web')?.provenance).toBeUndefined();
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

/**
 * Turns this view never saw (another tab, an automation, a channel message, a
 * report-back) put the server ahead of anything the view can count. The run's
 * `metadata` frame names the turn admission gave it, and every bubble of the
 * run takes that one. Once the run's stream lets go, the view reloads to show
 * the turns it missed; each test holds that reload until it has read the
 * stamps, then lets it through.
 */
describe('useChatMessages: a view that missed turns stamps the turn the server admitted', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    // The view loaded through turn 42; turns 43 and 44 were added elsewhere.
    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 42 }));
  });

  const admitted = (turn: number) => ({ event: 'metadata', thread_id: 'th-1', run_id: `run-${turn}`, turn_index: turn });
  const answerRow = (k: number, content: string) => ({ event: 'message_chunk', turn_index: k, role: 'assistant', agent: 'main', content_type: 'text', content });
  /** Turn `k` as the server replays it. */
  const turnRows = (k: number, question: string, ...answers: string[]) => [
    { event: 'user_message', turn_index: k, content: question, run_id: `run-${k}` },
    ...answers.map((content) => answerRow(k, content)),
  ];
  const missedTurn = (k: number) => turnRows(k, `question ${k}`, `answer ${k}`);
  const steeredTurn = [
    ...turnRows(45, 'go on', 'working'),
    { event: 'steering_delivered', turn_index: 45, messages: [{ content: 'also cover margins', timestamp: 1_700_000_000 }] },
    answerRow(45, 'covering margins'),
  ];

  /**
   * The thread as the server holds it once the view's own turns ran: turns
   * 40-42, then `turns`. `runEnds` goes in just before the run's last stream
   * ends, and holds the status read the reload starts with until `release`,
   * so a test reads the stamps the stream gave before the replay names them.
   */
  function serverHolds(...turns: Record<string, unknown>[][]) {
    let release!: () => void;
    const released = new Promise<void>((resolve) => {
      release = resolve;
    });
    const rows = turns.flat();
    const latest = Math.max(...rows.map((row) => row.turn_index as number));
    const page = [...pageOf(40, 42).slice(0, -1), ...rows, { event: 'replay_done', thread_id: 'th-1' }];
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      for (const item of page) onEvent(item);
    });
    const runEnds = () => mockStatus.mockImplementation(() => released.then(() => threadStatus({ latest_turn_index: latest })));
    return { runEnds, release };
  }

  /** Lets through the reload the run asked for, which shows the missed turns. */
  async function showsMissedTurns(hook: Hook, server: ReturnType<typeof serverHolds>, missed = [43, 44], replays = 2) {
    await act(async () => {
      server.release();
      await flush();
    });
    await waitFor(() => expect(mockReplay.mock.calls.length).toBeGreaterThanOrEqual(replays));
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
    for (const k of missed) expect(turnOfText(live(hook), 'user', `question ${k}`)).toBe(k);
  }

  it('a send lands on the admitted turn, and regenerating it re-runs that turn', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const server = serverHolds(missedTurn(43), missedTurn(44), turnRows(45, 'go on', 'fresh answer'));
    await act(async () => {
      const send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams[0].emit(admitted(45));
      streams[0].emit(chunk('fresh answer'));
      server.runEnds();
      streams[0].end();
      await send;
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(turnOfText(live(hook), 'user', 'go on')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'fresh answer')).toBe(45);
    await showsMissedTurns(hook, server);
    expect(turnOfText(live(hook), 'assistant', 'fresh answer')).toBe(45);

    mockStatus.mockResolvedValue(threadStatus({ latest_turn_index: 45 }));
    mockFetchTurns.mockResolvedValue({
      turns: [{ turn_index: 45, edit_checkpoint_id: 'cp-44', regenerate_checkpoint_id: 'cp-45' }],
    });
    const answer = (live(hook) as MessageRecord[]).find((m) => String(m.content ?? '').includes('fresh answer'));
    await act(async () => {
      const regenerate = hook.result.current.handleRegenerate(answer!.id as string);
      await flush();
      streams.at(-1)!.emit(admitted(45));
      streams.at(-1)!.emit(chunk('regenerated answer'));
      streams.at(-1)!.end();
      await regenerate;
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    expect(mockSendStream.mock.calls.at(-1)![3]).toMatchObject({ checkpointId: 'cp-45', forkFromTurn: 45 });

    // An older backend names no turn: the count carries on from the admitted one.
    await sendTurn(hook, streams, 'next question', 'next answer');
    expect(turnOfText(live(hook), 'user', 'next question')).toBe(46);
  });

  it('a stream that drops after the admitted turn continues on it', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const server = serverHolds(missedTurn(43), missedTurn(44), turnRows(45, 'go on', 'part one', 'part two'));
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-45', latest_turn_index: 45 }));
    // Resumed past the cursor, so the reconnect replays no metadata of its own.
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(chunk('part two'));
      server.runEnds();
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      const send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams[0].emit(admitted(45));
      streams[0].emit(chunk('part one'));
      streams[0].end({ disconnected: true });
      await send;
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(turnOfText(live(hook), 'user', 'go on')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'part one')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'part two')).toBe(45);
    await showsMissedTurns(hook, server);
    expect(mockReconnect).toHaveBeenCalledTimes(1);
  });

  it('a stream that drops before the admitted turn lands the whole turn on it once a reconnect sees it', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const server = serverHolds(missedTurn(43), missedTurn(44), turnRows(45, 'go on', 'the answer'));
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-45', latest_turn_index: 45 }));
    // Nothing reached the cursor, so the reconnect replays the run from its start.
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(admitted(45));
      onEvent(chunk('the answer'));
      server.runEnds();
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      const send = hook.result.current.handleSendMessage('go on');
      await flush();
      // The POST names its run, then drops while the sandbox is still starting.
      const { onRunIdResolved } = mockSendStream.mock.calls[0][3] as { onRunIdResolved: (rid: string, tid: string) => void };
      onRunIdResolved('run-45', 'th-1');
      streams[0].end({ disconnected: true });
      await send;
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    // The question, the send's bubble and the reconnect's.
    const projected = projectTurns(live(hook) as MessageRecord[]);
    const asked = projected.findIndex((p) => p.message.role === 'user' && p.message.content === 'go on');
    expect(projected.slice(asked).map((p) => [p.message.role, p.turnIndex])).toEqual([
      ['user', 45], ['assistant', 45], ['assistant', 45],
    ]);
    expect(turnOfText(live(hook), 'assistant', 'the answer')).toBe(45);
    await showsMissedTurns(hook, server);
  });

  it('steering sent before that drop joins the turn the reconnect sees', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const server = serverHolds(missedTurn(43), missedTurn(44), steeredTurn);
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-45', latest_turn_index: 45 }));
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(admitted(45));
      onEvent(chunk('working'));
      onEvent({ event: 'steering_delivered', messages: [{ content: 'also cover margins', timestamp: 1_700_000_000 }] });
      onEvent(chunk('covering margins'));
      server.runEnds();
      return { disconnected: false, aborted: false };
    });
    let send!: Promise<unknown>;
    await act(async () => {
      send = hook.result.current.handleSendMessage('go on');
      await flush();
      const { onRunIdResolved } = mockSendStream.mock.calls[0][3] as { onRunIdResolved: (rid: string, tid: string) => void };
      onRunIdResolved('run-45', 'th-1');
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('also cover margins');
      await flush();
      streams[1].emit({ event: 'steering_accepted', position: 1 });
      streams[1].end();
      await flush();
    });
    await act(async () => {
      streams[0].end({ disconnected: true });
      await send;
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    const projected = projectTurns(live(hook) as MessageRecord[]);
    const asked = projected.findIndex((p) => p.message.role === 'user' && p.message.content === 'go on');
    expect(projected.slice(asked).map((p) => p.turnIndex)).toEqual([45, 45, 45, 45, 45]);
    expect(turnOfText(live(hook), 'user', 'also cover margins')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'covering margins')).toBe(45);
    await showsMissedTurns(hook, server);
  });

  it('a steer that failed to send before that drop stays in the turn the reconnect sees', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const server = serverHolds(missedTurn(43), missedTurn(44), turnRows(45, 'go on', 'working'));
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-45', latest_turn_index: 45 }));
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(admitted(45));
      onEvent(chunk('working'));
      server.runEnds();
      return { disconnected: false, aborted: false };
    });
    let send!: Promise<unknown>;
    await act(async () => {
      send = hook.result.current.handleSendMessage('go on');
      await flush();
      const { onRunIdResolved } = mockSendStream.mock.calls[0][3] as { onRunIdResolved: (rid: string, tid: string) => void };
      onRunIdResolved('run-45', 'th-1');
    });
    mockSendStream.mockRejectedValueOnce(new Error('Server error 500'));
    await act(async () => {
      await hook.result.current.handleSendMessage('also cover margins');
      await flush();
    });
    expect((live(hook) as MessageRecord[]).find((m) => m.content === 'also cover margins')?.queueError).toBeTruthy();
    await act(async () => {
      streams[0].end({ disconnected: true });
      await send;
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    // The turn opens at the question, not at the steer typed into it.
    expect(turnOfText(live(hook), 'user', 'go on')).toBe(45);
    expect(turnOfText(live(hook), 'user', 'also cover margins')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'working')).toBe(45);
    await showsMissedTurns(hook, server);
  });

  it('a delivered steer before that drop stays in the turn the reconnect sees', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const server = serverHolds(missedTurn(43), missedTurn(44), steeredTurn);
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-45', latest_turn_index: 45 }));
    // Resumed past the steer, so the reconnect brings the run's turn and no steer.
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent(admitted(45));
      onEvent(chunk('covering margins'));
      server.runEnds();
      return { disconnected: false, aborted: false };
    });
    let send!: Promise<unknown>;
    await act(async () => {
      send = hook.result.current.handleSendMessage('go on');
      await flush();
      const { onRunIdResolved } = mockSendStream.mock.calls[0][3] as { onRunIdResolved: (rid: string, tid: string) => void };
      onRunIdResolved('run-45', 'th-1');
      streams[0].emit(chunk('working'));
    });
    await act(async () => {
      void hook.result.current.handleSendMessage('also cover margins');
      await flush();
      streams[1].emit({ event: 'steering_accepted', position: 1 });
      streams[1].end();
      await flush();
      streams[0].emit({ event: 'steering_delivered', messages: [{ content: 'also cover margins', timestamp: 1_700_000_000 }] });
      await flush();
    });
    expect((live(hook) as MessageRecord[]).find((m) => m.content === 'also cover margins')?.steeringDelivered).toBe(true);
    await act(async () => {
      streams[0].end({ disconnected: true });
      await send;
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(turnOfText(live(hook), 'user', 'go on')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'working')).toBe(45);
    expect(turnOfText(live(hook), 'user', 'also cover margins')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'covering margins')).toBe(45);
    await showsMissedTurns(hook, server);
  });

  it('a steering message sent before the run announced its turn joins the admitted turn', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const server = serverHolds(missedTurn(43), missedTurn(44), steeredTurn);
    // Both messages are made in one millisecond, which a fast machine does.
    const clock = vi.spyOn(Date, 'now').mockReturnValue(1_790_000_000_000);
    onTestFinished(() => clock.mockRestore());
    let send!: Promise<unknown>;
    await act(async () => {
      send = hook.result.current.handleSendMessage('go on');
      await flush();
    });
    // The run is still being admitted (a sandbox starting) when the user steers.
    await act(async () => {
      void hook.result.current.handleSendMessage('also cover margins');
      await flush();
      streams[1].emit({ event: 'steering_accepted', position: 1 });
      streams[1].end();
      await flush();
    });
    clock.mockRestore();
    await act(async () => {
      streams[0].emit(admitted(45));
      streams[0].emit(chunk('working'));
      streams[0].emit({ event: 'steering_delivered', messages: [{ content: 'also cover margins', timestamp: 1_700_000_000 }] });
      streams[0].emit(chunk('covering margins'));
      server.runEnds();
      streams[0].end();
      await send;
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(turnOfText(live(hook), 'user', 'go on')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'working')).toBe(45);
    expect(turnOfText(live(hook), 'user', 'also cover margins')).toBe(45);
    expect(turnOfText(live(hook), 'assistant', 'covering margins')).toBe(45);
    await showsMissedTurns(hook, server);
  });

  it('a steer the server refused as not_running takes the turn its resend was admitted to', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const server = serverHolds(
      missedTurn(43), missedTurn(44), turnRows(45, 'go on', 'working'),
      missedTurn(46), turnRows(47, 'and then this', 'resent answer'),
    );
    let send!: Promise<unknown>;
    await act(async () => {
      send = hook.result.current.handleSendMessage('go on');
      await flush();
      streams[0].emit(admitted(45));
      streams[0].emit(chunk('working'));
      await flush();
    });
    await act(async () => {
      const steer = hook.result.current.handleSendMessage('and then this');
      await flush();
      streams[1].emit({ event: 'error', error_type: 'admission_conflict', code: 'not_running', error: 'The turn has ended.' });
      streams[1].end();
      await steer;
      streams[0].end();
      await send;
      await flush();
    });
    // Another tab's turn 46 ran between the two: the resend opens turn 47.
    await waitFor(() => expect(streams).toHaveLength(3));
    await act(async () => {
      streams[2].emit(admitted(47));
      streams[2].emit(chunk('resent answer'));
      server.runEnds();
      streams[2].end();
      await flush();
    });
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(turnOfText(live(hook), 'assistant', 'working')).toBe(45);
    expect(turnOfText(live(hook), 'user', 'and then this')).toBe(47);
    expect(turnOfText(live(hook), 'assistant', 'resent answer')).toBe(47);
    await showsMissedTurns(hook, server, [43, 44, 46]);
  });

  it('a resumed interrupt lands on the admitted turn', async () => {
    const hook = await mountOnPage();
    const streams = captureStreams(mockSendStream);
    const ask = {
      event: 'interrupt',
      interrupt_id: 'int-ask',
      action_requests: [{ type: 'ask_user_question', question: 'Which tickers?', options: [], allow_multiple: false }],
    };
    const askedTurn = turnRows(45, 'screen chip names', 'checking');
    const asked = serverHolds(missedTurn(43), missedTurn(44), [...askedTurn, { ...ask, turn_index: 45 }]);
    await act(async () => {
      const send = hook.result.current.handleSendMessage('screen chip names');
      await flush();
      streams.at(-1)!.emit(admitted(45));
      streams.at(-1)!.emit(chunk('checking'));
      streams.at(-1)!.emit(ask);
      asked.runEnds();
      streams.at(-1)!.end();
      await send;
    });
    await waitFor(() => expect(hook.result.current.pendingInterrupt).not.toBeNull());
    expect(turnOfText(live(hook), 'assistant', 'checking')).toBe(45);
    // The question still waits once the reload shows the turns before it.
    await showsMissedTurns(hook, asked);
    await waitFor(() => expect(hook.result.current.pendingInterrupt).not.toBeNull());

    // An automation ran turn 46 while the question waited.
    const resumed = serverHolds(missedTurn(43), missedTurn(44), askedTurn, missedTurn(46), [answerRow(47, 'resumed answer')]);
    mockSendHitl.mockImplementation(async (...args: unknown[]) => {
      const { onEvent } = args[3] as { onEvent: Emit };
      onEvent(admitted(47));
      onEvent(chunk('resumed answer'));
      resumed.runEnds();
      return { disconnected: false };
    });
    await act(async () => {
      hook.result.current.handleAnswerQuestion('NVDA and AMD', 'int-ask', 'int-ask');
      await flush();
    });
    await waitFor(() => expect(mockSendHitl).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(turnOfText(live(hook), 'assistant', 'resumed answer')).toBe(47);
    await showsMissedTurns(hook, resumed, [46], 3);
    expect(turnOfText(live(hook), 'assistant', 'checking')).toBe(45);
  });
});
