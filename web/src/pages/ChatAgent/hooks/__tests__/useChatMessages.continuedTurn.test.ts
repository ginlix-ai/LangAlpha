/**
 * A bubble that continues another one (a reconnect after a dropped stream, a
 * steering continuation) belongs to the turn of the bubble it continues. A
 * report-back run attaches on a bubble with no stamped turn, since its turn is
 * the one after the last the transcript shows, so its continuations cannot
 * copy a stamp off it: they have to take the turn the transcript projects for
 * it, or the count puts them on the turn after.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus, captureWatchCalls } from './chatHookHarness';

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

import {
  getWorkflowStatus,
  getReportBackStatus,
  replayThreadHistory,
  reconnectToWorkflowStream,
  watchThread,
} from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { projectTurns } from '../../components/messageList/turnProjection';
import type { MessageRecord } from '../utils/types';

const mockStatus = getWorkflowStatus as Mock;
const mockReportBackStatus = getReportBackStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockWatch = watchThread as Mock;

type Emit = (e: Record<string, unknown>) => void;

/** Replay of turns 0..last, each a question and its answer. */
function historyThrough(last: number) {
  return async (_tid: string, onEvent: Emit) => {
    for (let k = 0; k <= last; k++) {
      onEvent({ event: 'user_message', turn_index: k, content: `question ${k}`, run_id: `run-${k}` });
      onEvent({
        event: 'message_chunk', turn_index: k, role: 'assistant', agent: 'main', content_type: 'text', content: `answer ${k}`,
      });
    }
    onEvent({ event: 'replay_done', thread_id: 'th-1' });
  };
}

/**
 * A per-run reader that delivers `frames` past the backlog marker, then holds
 * the socket open until its signal aborts, as the server does for a live run.
 */
function liveReader(frames: Record<string, unknown>[]) {
  return (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit, signal: AbortSignal) =>
    new Promise((resolve) => {
      for (const frame of frames) onEvent(frame);
      onEvent({ event: 'caught_up' });
      const onAbort = () => resolve({ disconnected: false, aborted: true });
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    });
}

const chunk = (content: string) => ({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content });

const flush = () => new Promise((r) => setTimeout(r, 0));

/** The turn the transcript projects for the assistant bubble showing `text`. */
function turnOfAnswer(messages: unknown[], text: string): number {
  const entry = projectTurns(messages as MessageRecord[]).find(
    (p) => p.message.role === 'assistant' && String(p.message.content ?? '').includes(text),
  );
  if (!entry) throw new Error(`no assistant bubble shows ${text}`);
  return entry.turnIndex;
}

/** Turns 0..2 on screen, then a report-back run attached live on a bubble of its own. */
async function mountWithLiveReportBack(frames: Record<string, unknown>[], replay = historyThrough(2)) {
  mockReplay.mockImplementation(replay);
  mockStatus.mockResolvedValue(threadStatus({ pending_report_back: true, latest_turn_index: 2 }));
  mockReconnect.mockImplementation(liveReader(frames));
  const watchCalls = captureWatchCalls(mockWatch);

  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
  await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
  await settleMountEffect();
  expect(turnOfAnswer(hook.result.current.messages, 'answer 2')).toBe(2);

  await act(async () => {
    void watchCalls[0].cb({ run_id: 'rb-run-1' });
    await flush();
  });
  await waitFor(() => expect(hook.result.current.isLoading).toBe(true));
  return hook;
}

describe('useChatMessages: a continued bubble keeps the turn it continues', () => {
  const visibility = { value: 'visible' };

  beforeEach(() => {
    vi.clearAllMocks();
    mockReportBackStatus.mockImplementation((...args: unknown[]) => mockStatus(...args));
    visibility.value = 'visible';
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => visibility.value,
    });
  });

  it('a report-back that reconnects mid-run streams the rest of its turn onto the same turn', async () => {
    const { result } = await mountWithLiveReportBack([chunk('summary so far')]);
    await waitFor(() => expect(JSON.stringify(result.current.messages)).toContain('summary so far'));
    expect(turnOfAnswer(result.current.messages, 'summary so far')).toBe(3);

    // The tab is frozen and comes back while the run is still going: the reader
    // is dropped and the same run resumes from its cursor on a new bubble. The
    // status read is a round trip, so the new bubble is minted later.
    mockStatus.mockImplementation(async () => {
      await new Promise((r) => setTimeout(r, 5));
      return threadStatus({ can_reconnect: true, status: 'running', run_id: 'rb-run-1', latest_turn_index: 3 });
    });
    mockReconnect.mockImplementation(liveReader([chunk('and the rest')]));
    await act(async () => {
      window.dispatchEvent(new Event('pagehide'));
      document.dispatchEvent(new Event('visibilitychange'));
      await flush();
    });
    await waitFor(() => expect(mockReconnect).toHaveBeenCalledTimes(2));
    expect(mockReconnect.mock.calls[1][1]).toBe('rb-run-1');
    await waitFor(() => expect(JSON.stringify(result.current.messages)).toContain('and the rest'));

    expect(turnOfAnswer(result.current.messages, 'summary so far')).toBe(3);
    expect(turnOfAnswer(result.current.messages, 'and the rest')).toBe(3);
  });

  it('a steering message another tab delivered to a report-back sits in the turn it steered', async () => {
    const { result } = await mountWithLiveReportBack([
      chunk('summary so far'),
      { event: 'steering_delivered', messages: [{ content: 'also cover margins', timestamp: 1_700_000_000 }] },
      chunk('covering margins'),
    ]);
    await waitFor(() => expect(JSON.stringify(result.current.messages)).toContain('covering margins'));

    const projected = projectTurns(result.current.messages as unknown as MessageRecord[]);
    const steering = projected.find((p) => p.message.role === 'user' && p.message.content === 'also cover margins');
    expect(steering?.turnIndex).toBe(3);
    expect(turnOfAnswer(result.current.messages, 'covering margins')).toBe(3);
  });

  it('a report-back after a turn whose steering continuation settled empty opens a turn of its own', async () => {
    // Turn 2 was steered, and the run ended before the continuation wrote
    // anything: the transcript ends on an empty history bubble of a settled run.
    const steeredEmpty = async (_tid: string, onEvent: Emit) => {
      for (let k = 0; k <= 2; k++) {
        onEvent({ event: 'user_message', turn_index: k, content: `question ${k}`, run_id: `run-${k}`, run_completed_at: '2026-10-09T12:00:00Z' });
        onEvent({
          event: 'message_chunk', turn_index: k, role: 'assistant', agent: 'main', content_type: 'text', content: `answer ${k}`,
        });
      }
      onEvent({ event: 'steering_delivered', turn_index: 2, messages: [{ content: 'also cover margins', timestamp: 1_700_000_000 }] });
      onEvent({ event: 'replay_done', thread_id: 'th-1' });
    };
    const { result } = await mountWithLiveReportBack([chunk('summary of the task')], steeredEmpty);
    await waitFor(() => expect(JSON.stringify(result.current.messages)).toContain('summary of the task'));

    expect(turnOfAnswer(result.current.messages, 'summary of the task')).toBe(3);
    expect(result.current.messages.some((m) => m.id === 'history-assistant-steering-2-1')).toBe(true);
  });
});
