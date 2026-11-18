/**
 * A view lets go of a question it shows once another run of the thread
 * attaches, because only an answer starts one: the sender answered in a
 * shared workspace, or the person answered in another tab. The run that
 * raised the question coming back is not that signal. Its stream resumes past
 * the pause frame, so a cleared question would never be raised again and the
 * person would lose one they have not answered.
 *
 * Drives the REAL hook (api module mocked), the way the reconnect suites do.
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

const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSendStream = sendChatMessageStream as Mock;

type Emit = (e: Record<string, unknown>) => void;

const QUESTION = {
  event: 'interrupt',
  interrupt_id: 'int-1',
  action_requests: [{ type: 'ask_user_question', question: 'Red or blue?', options: ['Red', 'Blue'] }],
};

const live = (runId: string) => threadStatus({ can_reconnect: true, status: 'running', run_id: runId, latest_turn_index: 0 });
const paused = () => threadStatus({ can_reconnect: false, status: 'interrupted', latest_turn_index: 0 });
const flush = () => new Promise((r) => setTimeout(r, 0));

/** A transcript whose newest turn stopped on the question, raised by `runId`. */
const replayPausedOn = (runId: string) => async (_tid: string, onEvent: Emit) => {
  onEvent({ event: 'user_message', turn_index: 0, content: 'pick a colour', run_id: runId });
  onEvent({ ...QUESTION, turn_index: 0 });
  onEvent({ event: 'replay_done', thread_id: 'th-1', latest_run_id: runId });
};

describe('useChatMessages: a question this view shows, and another run of its thread', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReconnect.mockResolvedValue({ disconnected: false, aborted: false });
  });

  it('lets go once a run that answered it attaches', async () => {
    mockReplay.mockImplementation(replayPausedOn('run-1'));
    mockStatus.mockResolvedValue(paused());
    const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
    await settleMountEffect();
    await waitFor(() => expect(hook.result.current.pendingInterrupt?.interruptId).toBe('int-1'));

    // Its sender answered elsewhere: the answer's run is live now.
    mockStatus.mockResolvedValue(live('run-2'));
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'Red it is.' });
      return { disconnected: false, aborted: false };
    });
    await act(async () => {
      await hook.result.current.reconnectIfStaleRun();
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    expect(mockReconnect.mock.calls[0][1]).toBe('run-2');
    await waitFor(() => expect(hook.result.current.pendingInterrupt).toBeNull());
  });

  it('keeps it answerable when its own run is attached again after a drop', async () => {
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      onEvent({ event: 'replay_done', thread_id: 'th-1', latest_run_id: null });
    });
    mockStatus.mockResolvedValue(threadStatus());
    const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
    await settleMountEffect();
    await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));

    // The turn raises the question and the link drops before the run ends.
    mockSendStream.mockImplementation(async (_msg: string, _ws: string, _tid: string | null, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-1', _eventId: 1 });
      onEvent({ ...QUESTION, _eventId: 2 });
      return { disconnected: true };
    });
    // The drop's status read finds no stream to resume, so the view reloads the
    // thread, which finds the same run still live and attaches to it by id. Its
    // stream picks up after the pause frame, so nothing raises the question again.
    mockStatus
      .mockResolvedValueOnce(threadStatus({ can_reconnect: false, status: 'running' }))
      .mockResolvedValue(live('run-1'));
    mockReplay.mockImplementation(replayPausedOn('run-1'));
    mockReconnect.mockImplementation(async (_tid: string, _rid: string, _cursor: unknown, onEvent: Emit) => {
      onEvent({ event: 'run_end', thread_id: 'th-1', run_id: 'run-1', outcome: 'interrupted' });
      return { disconnected: false, aborted: false };
    });

    await act(async () => {
      await hook.result.current.handleSendMessage('pick a colour');
      await flush();
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    expect(mockReconnect.mock.calls.every((call) => call[1] === 'run-1')).toBe(true);
    await waitFor(() => expect(hook.result.current.isLoading).toBe(false));
    expect(hook.result.current.pendingInterrupt?.interruptId).toBe('int-1');
  });
});
