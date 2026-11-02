/**
 * A send of file comments stores the block the agent read as its query, and
 * the chat shows its note. A replayed row carries the note as its content, so
 * a Retry whose turn must be sent again gives the note back to the composer,
 * never the agent's block.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect } from './chatHookHarness';

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { replayThreadHistory, sendRetryStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage, UserMessage } from '@/types/chat';

const mockReplay = replayThreadHistory as Mock;
const mockRetryStream = sendRetryStream as Mock;

type Emit = (e: Record<string, unknown>) => void;

const NOTE = 'tighten these';
const SHOWN = {
  note: NOTE,
  reading: 'chat',
  comments: [{
    comment_id: 'c-1', artifact_id: 'a-1', workspace_id: 'ws', path: 'reports/q3.html', title: 'Q3',
    number: 1, text: 'too dense', follow_up: false, label: null, quote: null,
  }],
};

/** Turn 0 answered; turn 1 a comment send whose run failed. */
const page = [
  { event: 'history_page', first_turn_index: 0, has_more: false },
  { event: 'user_message', turn_index: 0, content: 'question 0', run_id: 'run-0' },
  { event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'answer 0' },
  {
    event: 'user_message', turn_index: 1, run_id: 'run-1',
    content: `${NOTE}\n<file-comments>1. too dense</file-comments>`,
    metadata: { artifact_comments: SHOWN },
  },
  { event: 'error', turn_index: 1, thread_id: 'th-1', error: 'boom', type: 'workflow_error' },
  { event: 'replay_done', thread_id: 'th-1', latest_run_id: 'run-1' },
];

describe('useChatMessages: a replayed comment send', () => {
  it('shows its note, and a Retry that must be sent again gives the note back', async () => {
    const pages = [page, page];
    mockReplay.mockImplementation(async (_tid: string, onEvent: Emit) => {
      for (const item of pages.shift() ?? []) onEvent(item);
    });
    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
    await settleMountEffect();
    await waitFor(() => expect(result.current.isLoadingHistory).toBe(false));

    const sent = result.current.messages.find((m) => m.role === 'user' && m.turnIndex === 1) as UserMessage;
    expect(sent.content).toBe(NOTE);
    expect(sent.artifactComments?.comments).toHaveLength(1);
    expect((result.current.messages.at(-1) as AssistantMessage).error).toBeTruthy();

    const detail = { code: 'resend_required', message: "The failed turn's input was more than its stored message; send it again." };
    mockRetryStream.mockRejectedValue(Object.assign(new Error(detail.message), { status: 409, errorInfo: detail }));
    await act(async () => {
      await result.current.handleRetry();
    });

    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(2));
    expect(result.current.returnedEdit).toBe(NOTE);
  });
});
