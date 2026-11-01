/**
 * A send carrying file comments that the server refuses, inside its stream
 * (an admission refusal frame, before any run) or as the answer to a Retry,
 * ends for its comments as refused, with the chat's reason, and nothing after:
 * whether the composer sits in the chat or docked under a file.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { ArtifactCommentsShown } from '@/types/api';
import type { ComposerDraft } from '@/components/ui/chat-input.types';
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

import { replayThreadHistory, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockReplay = replayThreadHistory as Mock;
const mockSend = sendChatMessageStream as Mock;

type Emit = (e: Record<string, unknown>) => void;

const SHOWN: ArtifactCommentsShown = {
  note: 'tighten these',
  reading: 'chat',
  comments: [{
    comment_id: 'c-1', artifact_id: 'a-1', workspace_id: 'ws', path: 'reports/q3.html', title: 'Q3',
    number: 1, text: 'too dense', follow_up: false, label: null, quote: null,
  }],
};

const BUSY = { kind: 'notice', message: 'chat.busyRunning' };

/** Refused at admission, inside the stream: no run was started. */
const refusedInStream = async (_m: string, _ws: string, _t: string, { onEvent }: { onEvent: Emit }) => {
  onEvent({ event: 'error', error: 'A reply is still running.', code: 'running', error_type: 'admission_conflict' });
  return {};
};

async function mountWith(docked: boolean) {
  mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
    onEvent({ event: 'history_page', first_turn_index: 0, has_more: false });
    onEvent({ event: 'replay_done', thread_id: 'th-1', latest_run_id: null });
  });
  const hook = renderHookWithProviders(() => useChatMessages('ws', 'th-1', { platform: 'web', returnsUnsent: () => docked }));
  await settleMountEffect();
  await waitFor(() => expect(hook.result.current.isLoadingHistory).toBe(false));
  // Each outcome the chat tells for the comments, by their ids.
  const heard: [string[], unknown][] = [];
  hook.result.current.watchCommentSends((comments, outcome) => { heard.push([carriedIds(comments), outcome]); });
  return { ...hook, heard };
}

describe('useChatMessages: a comment send refused', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('inside its stream leaves the comments with the reason while the composer sits in the chat', async () => {
    mockSend.mockImplementation(refusedInStream);
    const { result, heard } = await mountWith(false);
    await act(async () => {
      await result.current.handleSendMessage('tighten these', null, null, { artifactComments: SHOWN });
    });
    // Refused, never taken, and not let go after: the reason, in the chat's
    // words for a busy chat, stands.
    expect(heard).toEqual([
      [['c-1'], 'began'],
      [['c-1'], { refused: BUSY }],
    ]);
    expect(mockSend.mock.calls[0][3].artifactComments).toEqual({ ids: ['c-1'], reading: 'chat' });
  });

  it('inside its stream does the same while the composer is docked, which takes the send back', async () => {
    mockSend.mockImplementation(refusedInStream);
    const { result, heard } = await mountWith(true);
    const draft = { text: 'tighten these' } as unknown as ComposerDraft;
    await act(async () => {
      await result.current.handleSendMessage('tighten these', null, null, { artifactComments: SHOWN, draft });
    });
    expect(result.current.returnedDraft).toBe(draft);
    expect(heard.map(([, outcome]) => outcome)).toEqual(['began', { refused: BUSY }]);
  });

  it('as the answer to its Retry gives the reason the Retry was refused with', async () => {
    // The first send fails before the server keeps it, and leaves Retry.
    mockSend.mockImplementationOnce(async (_m: string, _ws: string, _t: string, { onEvent }: { onEvent: Emit }) => {
      onEvent({ event: 'error', thread_id: 'th-1', error: 'boom', type: 'workflow_error', error_type: 'workspace_not_found', recovery: 'resend' });
      return {};
    });
    mockSend.mockRejectedValueOnce(Object.assign(new Error('Request failed with status code 409'), {
      status: 409,
      errorInfo: { code: 'workspace_archived', message: 'This workspace is archived.' },
    }));
    const { result, heard } = await mountWith(false);
    await act(async () => {
      await result.current.handleSendMessage('tighten these', null, null, { artifactComments: SHOWN });
    });
    expect(heard.map(([, outcome]) => outcome)).toEqual(['began', 'dropped']);

    await act(async () => {
      await result.current.handleRetry();
    });
    expect(mockSend).toHaveBeenCalledTimes(2);
    expect(mockSend.mock.calls[1][3].artifactComments).toEqual({ ids: ['c-1'], reading: 'chat' });
    expect(heard.slice(2)).toEqual([
      [['c-1'], 'began'],
      [['c-1'], { refused: 'This workspace is archived.' }],
    ]);
  });
});
