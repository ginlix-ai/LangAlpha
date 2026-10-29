/**
 * A turn that fails, or is stopped from elsewhere, after its stream dropped.
 * The reconnect resumes the run on a bubble of its own, which stays empty when
 * nothing but the end follows (a tool's result goes to the bubble that made
 * the call), and a reconnect drops its own bubble when it ends empty. So the
 * end has to land on the bubble that holds the turn, where its card, chip and
 * Retry belong, and a bubble that holds nothing but the end has to stay.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus } from './chatHookHarness';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { getWorkflowStatus, reconnectToWorkflowStream, replayThreadHistory, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { MessageRecord } from '../utils/types';

const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSendStream = sendChatMessageStream as Mock;

type Emit = (e: Record<string, unknown>) => void;
type Frame = Record<string, unknown>;

const chunk = (content: string, extra: Frame = {}): Frame => ({
  event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content, ...extra,
});
const thought = (content_type: 'reasoning_signal' | 'reasoning', content: string, extra: Frame = {}): Frame => ({
  event: 'message_chunk', role: 'assistant', agent: 'main', content_type, content, ...extra,
});
const toolCall = (extra: Frame = {}): Frame => ({
  event: 'tool_calls', agent: 'main', tool_calls: [{ id: 'call-1', name: 'execute_code', args: { code: 'run()' } }], ...extra,
});
const toolResult = (extra: Frame = {}): Frame => ({
  event: 'tool_call_result', agent: 'main', tool_call_id: 'call-1', content: 'done', ...extra,
});
/** The hard stop's frame: unclassified, so its card is drawn on the turn. */
const timeLimit = (extra: Frame = {}): Frame => ({
  event: 'error', error: 'Turn timed out after 21600 seconds', type: 'timeout_error', ...extra,
});
const upstream = (extra: Frame = {}): Frame => ({
  event: 'error', error: 'Error code: 503', error_kind: 'upstream', status_code: 503, ...extra,
});
const runEnd = { event: 'run_end', thread_id: 'th-1', run_id: 'run-1', outcome: 'error' };

const flush = () => new Promise((r) => setTimeout(r, 0));

const running = () => threadStatus({ can_reconnect: true, status: 'running', run_id: 'run-1', latest_turn_index: 1 });

async function mount(status = threadStatus()) {
  mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
    onEvent({ event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' });
    onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'answer 0' });
    onEvent({ event: 'replay_done', thread_id: 'th-1' });
  });
  mockStatus.mockResolvedValue(status);
  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  return hook;
}

type Hook = Awaited<ReturnType<typeof mount>>;

/** A reconnect that plays `frames` and ends with the run. */
const endsAfter = (frames: Frame[]) =>
  async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
    for (const frame of frames) onEvent(frame);
    return { disconnected: false, aborted: false };
  };

/** Sends a turn whose stream plays `sent` and then drops, and reconnects with `reconnect`. */
async function sendThenDrop(hook: Hook, sent: Frame[], reconnect: (...args: never[]) => Promise<unknown>) {
  mockStatus.mockImplementation(async () => running());
  mockReconnect.mockImplementation(reconnect);
  mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
    onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', _eventId: 1 });
    for (const frame of sent) onEvent(frame);
    return { disconnected: true };
  });
  await act(async () => {
    await hook.result.current.handleSendMessage('go on');
    await flush();
  });
  await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
  await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
}

const transcript = (hook: Hook) => hook.result.current.liveMessages.get() as MessageRecord[];

const assistants = (hook: Hook) => transcript(hook).filter((m) => m.role === 'assistant');

const emptyBubbles = (hook: Hook) =>
  assistants(hook).filter((m) => !m.content && !(m.contentSegments as unknown[] | undefined)?.length);

function bubbleShowing(hook: Hook, text: string): MessageRecord {
  const bubble = assistants(hook).find((m) => String(m.content ?? '').includes(text));
  if (!bubble) throw new Error(`no assistant bubble shows ${text}`);
  return bubble;
}

const failed = (hook: Hook) => assistants(hook).filter((m) => m.error);

describe('useChatMessages: a failure heard after a reconnect lands on the turn', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('a time limit that stops a long tool call across a drop settles the bubble that made the call', async () => {
    const hook = await mount();
    await sendThenDrop(
      hook,
      [{ ...chunk('the first half'), _eventId: 2 }, { ...toolCall(), _eventId: 3 }],
      endsAfter([{ ...toolResult(), _eventId: 4 }, { ...timeLimit(), _eventId: 5 }, runEnd]),
    );

    const turn = bubbleShowing(hook, 'the first half');
    expect(turn).toMatchObject({ error: true, isStreaming: false, structuredError: { message: 'Turn timed out after 21600 seconds' } });
    expect((turn.toolCallProcesses as Record<string, Record<string, unknown>>)['call-1']).toMatchObject({ isComplete: true });
    // One failed bubble, the turn's, and no empty one beside it.
    expect(failed(hook)).toEqual([turn]);
    expect(emptyBubbles(hook)).toEqual([]);
    expect(hook.result.current.messageError).toBeNull();
  });

  it('a provider failure before the reconnect brings any text settles the bubble that holds the turn', async () => {
    const hook = await mount();
    await sendThenDrop(
      hook,
      [{ ...chunk('the first half'), _eventId: 2 }],
      endsAfter([{ ...upstream(), _eventId: 3 }, runEnd]),
    );

    expect(bubbleShowing(hook, 'the first half')).toMatchObject({
      error: true, structuredError: { kind: 'upstream', statusCode: 503 },
    });
    expect(failed(hook)).toHaveLength(1);
    expect(emptyBubbles(hook)).toEqual([]);
  });

  it('a failure after a drop mid-thought closes the thought the dropped stream left open', async () => {
    const hook = await mount();
    await sendThenDrop(
      hook,
      [
        { ...chunk('the first half'), _eventId: 2 },
        { ...thought('reasoning_signal', 'start'), _eventId: 3 },
        { ...thought('reasoning', 'Checking the curve'), _eventId: 4 },
      ],
      endsAfter([{ ...upstream(), _eventId: 5 }, runEnd]),
    );

    const turn = bubbleShowing(hook, 'the first half');
    expect(turn.error).toBe(true);
    const thoughts = Object.values(turn.reasoningProcesses as Record<string, Record<string, unknown>>);
    expect(thoughts).toHaveLength(1);
    expect(thoughts[0]).toMatchObject({ isReasoning: false, reasoningComplete: true });
  });

  it('a failure after a steer lands on the steer\'s reply, never on the bubble above the steer', async () => {
    const hook = await mount();
    await sendThenDrop(
      hook,
      [
        { ...chunk('the first half'), _eventId: 2 },
        { event: 'steering_delivered', messages: [{ content: 'also this', timestamp: 1_700_000_000 }], _eventId: 3 },
      ],
      endsAfter([{ ...upstream(), _eventId: 4 }, runEnd]),
    );

    const messages = transcript(hook);
    const steer = messages.findIndex((m) => m.role === 'user' && m.content === 'also this');
    const [reply] = failed(hook);
    expect(reply).toMatchObject({ structuredError: { kind: 'upstream' } });
    expect(messages.indexOf(reply)).toBeGreaterThan(steer);
    expect(bubbleShowing(hook, 'the first half').error).toBeUndefined();
    expect(failed(hook)).toHaveLength(1);
  });

  it('an attach that hears nothing of its run but the failure keeps the bubble that shows it', async () => {
    mockReconnect.mockImplementation(endsAfter([{ ...timeLimit(), _eventId: 9 }]));
    const hook = await mount(running());
    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(failed(hook)).toEqual([
      expect.objectContaining({ isStreaming: false, structuredError: { message: 'Turn timed out after 21600 seconds' } }),
    ]);
  });
});

describe('useChatMessages: the turn shows one failure across a reconnect', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('a drop between a provider failure and the run\'s end leaves the card as the only report', async () => {
    const hook = await mount();
    await sendThenDrop(
      hook,
      [{ ...chunk('the first half'), _eventId: 2 }, { ...upstream(), _eventId: 3 }],
      endsAfter([runEnd]),
    );

    expect(bubbleShowing(hook, 'the first half')).toMatchObject({ error: true, structuredError: { kind: 'upstream' } });
    expect(hook.result.current.messageError).toBeNull();
    expect(failed(hook)).toHaveLength(1);
    expect(emptyBubbles(hook)).toEqual([]);
  });

  it('a server that frames its time limit and then an internal error across a drop shows only the card', async () => {
    const hook = await mount();
    await sendThenDrop(
      hook,
      [{ ...chunk('the first half'), _eventId: 2 }, { ...timeLimit(), _eventId: 3 }],
      endsAfter([{ event: 'error', error: 'Stream ended unexpectedly', error_kind: 'internal', _eventId: 4 }, runEnd]),
    );

    expect(bubbleShowing(hook, 'the first half')).toMatchObject({
      error: true, structuredError: { message: 'Turn timed out after 21600 seconds' },
    });
    expect(hook.result.current.messageError).toBeNull();
    expect(failed(hook)).toHaveLength(1);
  });

  it('a run that fails without a frame after a drop mid-thought fails the bubble that holds the turn', async () => {
    const hook = await mount();
    await sendThenDrop(
      hook,
      [
        { event: 'model_fallback', agent: 'main', from_model: 'primary', to_model: 'backup', _eventId: 2 },
        { ...chunk('the first half'), _eventId: 3 },
        { ...thought('reasoning_signal', 'start'), _eventId: 4 },
        { ...thought('reasoning', 'Checking the curve'), _eventId: 5 },
      ],
      endsAfter([runEnd]),
    );

    const turn = bubbleShowing(hook, 'the first half');
    expect(turn).toMatchObject({ error: true, isStreaming: false });
    const thoughts = Object.values(turn.reasoningProcesses as Record<string, Record<string, unknown>>);
    expect(thoughts[0]).toMatchObject({ isReasoning: false, reasoningComplete: true });
    expect(failed(hook)).toEqual([turn]);
    expect(emptyBubbles(hook)).toEqual([]);
    expect(hook.result.current.messageError).toEqual({ message: '', kind: 'internal' });
    expect(hook.result.current.fallbackSuggestion).toBeNull();
  });
});

const cancelled = { event: 'run_end', thread_id: 'th-1', run_id: 'run-1', outcome: 'cancelled' };

const stopped = (hook: Hook) => assistants(hook).filter((m) => m.stopped);

describe('useChatMessages: a stop from elsewhere heard after a reconnect lands on the turn', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('a cancel after a drop mid-thought stops the bubble that holds the turn and closes its thought', async () => {
    const hook = await mount();
    await sendThenDrop(
      hook,
      [
        { ...chunk('the first half'), _eventId: 2 },
        { ...toolCall(), _eventId: 3 },
        { ...thought('reasoning_signal', 'start'), _eventId: 4 },
        { ...thought('reasoning', 'Checking the curve'), _eventId: 5 },
      ],
      endsAfter([cancelled]),
    );

    const turn = bubbleShowing(hook, 'the first half');
    expect(turn).toMatchObject({ stopped: true, isStreaming: false });
    const thoughts = Object.values(turn.reasoningProcesses as Record<string, Record<string, unknown>>);
    expect(thoughts[0]).toMatchObject({ isReasoning: false, reasoningComplete: true });
    expect((turn.toolCallProcesses as Record<string, Record<string, unknown>>)['call-1']).toMatchObject({ isInProgress: false, isComplete: true });
    expect(stopped(hook)).toEqual([turn]);
    expect(emptyBubbles(hook)).toEqual([]);
    expect(failed(hook)).toEqual([]);
  });

  it('an attach that hears nothing of its run but the cancel keeps the bubble that shows the stop', async () => {
    mockReconnect.mockImplementation(endsAfter([cancelled]));
    const hook = await mount(running());
    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));

    expect(stopped(hook)).toEqual([expect.objectContaining({ isStreaming: false })]);
  });
});
