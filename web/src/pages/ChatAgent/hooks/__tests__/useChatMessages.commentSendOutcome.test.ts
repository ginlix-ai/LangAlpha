/**
 * How a send carrying file comments ends for them, as the chat tells it
 * (`watchCommentSends`): a copy the server already had counts as taken, and
 * a resend the client schedules on its own holds them until it is answered,
 * never letting them go between attempts.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { ArtifactCommentsShown } from '@/types/api';
import { carriedIds } from '../utils/requestKey';
import { settleMountEffect } from './chatHookHarness';

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { getWorkflowStatus, replayThreadHistory, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockReplay = replayThreadHistory as Mock;
const mockSend = sendChatMessageStream as Mock;
const mockStatus = getWorkflowStatus as Mock;

type Emit = (e: Record<string, unknown>) => void;
type SendOptions = { onEvent: Emit; onRunIdResolved: (runId: string, threadId: string | null) => void };

const SHOWN: ArtifactCommentsShown = {
  note: 'tighten these',
  reading: 'chat',
  comments: [{
    comment_id: 'c-1', artifact_id: 'a-1', workspace_id: 'ws', path: 'reports/q3.html', title: 'Q3',
    number: 1, text: 'too dense', follow_up: false, label: null, quote: null,
  }],
};

async function mount() {
  mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
    onEvent({ event: 'history_page', first_turn_index: 0, has_more: false });
    onEvent({ event: 'replay_done', thread_id: 'th-1', latest_run_id: null });
  });
  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  const heard: [string[], unknown][] = [];
  hook.result.current.watchCommentSends((comments, outcome) => { heard.push([carriedIds(comments), outcome]); });
  return { ...hook, heard };
}

describe('useChatMessages: how a comment send ends for its comments', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
  });

  afterEach(() => {
    vi.useRealTimers();
  });

  it('counts a copy the server already accepted as taken', async () => {
    mockSend.mockRejectedValue(Object.assign(new Error('This request was already accepted'), {
      status: 409,
      errorInfo: {
        code: 'duplicate_request',
        message: 'This request was already accepted; reconnect to the existing run instead of resending.',
        thread_id: 'th-1', run_id: 'run-dup', run_status: 'in_progress',
      },
    }));
    const { result, heard } = await mount();
    await act(async () => {
      await result.current.handleSendMessage('tighten these', null, null, { artifactComments: SHOWN });
    });
    expect(heard).toEqual([
      [['c-1'], 'began'],
      [['c-1'], 'accepted'],
    ]);
  });

  it('takes no answer from the headers, which come ahead of the checks that can still refuse', async () => {
    mockSend.mockImplementationOnce(async (_m: string, _ws: string, _t: string, { onEvent, onRunIdResolved }: SendOptions) => {
      onRunIdResolved('run-1', 'th-1');
      onEvent({ event: 'error', code: 'stopping', error: 'The last reply is stopping.', thread_id: 'th-1' });
      return {};
    });
    const { result, heard } = await mount();
    await act(async () => {
      await result.current.handleSendMessage('tighten these', null, null, { artifactComments: SHOWN });
    });
    expect(heard[0][1]).toBe('began');
    expect(heard[1][1]).toHaveProperty('refused');
  });

  it('holds the comments through a resend it schedules, until that resend is answered', async () => {
    // The first attempt fails before the server names a run, and asks for
    // the same send again; the second is accepted.
    mockSend.mockImplementationOnce(async (_m: string, _ws: string, _t: string, { onEvent }: SendOptions) => {
      onEvent({
        event: 'retry', thread_id: 'th-1', auto_retry: true, error_type: 'timeout_error',
        retry_count: 1, max_retries: 3, recovery: 'resend',
      });
      return { disconnected: true };
    });
    mockSend.mockImplementationOnce(async (_m: string, _ws: string, _t: string, { onEvent, onRunIdResolved }: SendOptions) => {
      onRunIdResolved('run-2', 'th-1');
      onEvent({ event: 'metadata', thread_id: 'th-1', run_id: 'run-2' });
      return {};
    });
    const { result, heard } = await mount();
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout'] });
    await act(async () => {
      await result.current.handleSendMessage('tighten these', null, null, { artifactComments: SHOWN });
    });
    // Waiting on the resend: still on their way, not back in the queue.
    expect(heard.map(([, outcome]) => outcome)).toEqual(['began']);

    // waitFor polls on the faked clock, so the resend's chain is run out here.
    for (const ms of [1000, 0, 0, 0, 0, 0]) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(ms);
      });
    }
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend.mock.calls[1][3].artifactComments).toEqual({ ids: ['c-1'], reading: 'chat' });
    expect(heard.map(([, outcome]) => outcome)).toEqual(['began', 'began', 'accepted']);
  });
});
