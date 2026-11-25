/**
 * The thread a send carrying file comments tells its sender (`onTaken`) is
 * always one the comments can be found in: a new chat's send with no words
 * takes the legacy door, where the server names the thread in its first
 * frame, and that id is the one told, never the chat's placeholder.
 *
 * Real hook internals; only the api module (the transport) is mocked.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { ArtifactCommentsShown } from '@/types/api';
import { settleMountEffect } from './chatHookHarness';

vi.mock('react-i18next', () => ({ useTranslation: () => ({ t: (k: string) => k }) }));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule({
  createThreadWithTitle: vi.fn(),
}));

import { createThreadWithTitle, sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSend = sendChatMessageStream as Mock;
const mockCreateThread = createThreadWithTitle as Mock;

type Emit = (e: Record<string, unknown>) => void;
type SendOptions = { onEvent: Emit; onRunIdResolved: (runId: string, threadId: string | null) => void };

const SHOWN: ArtifactCommentsShown = {
  note: '',
  reading: 'chat',
  comments: [{
    comment_id: 'c-1', artifact_id: 'a-1', workspace_id: 'ws', kind: 'chart', path: 'NVDA:1D', title: 'NVDA',
    number: 1, text: 'why this dip', follow_up: false, label: null, quote: null,
  }],
};

describe('useChatMessages: the thread a comment send tells', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('from a new chat with no words, tells the thread the server names, not the placeholder', async () => {
    mockSend.mockImplementationOnce(async (_m: string, _ws: string, _t: string, { onEvent, onRunIdResolved }: SendOptions) => {
      onRunIdResolved('run-1', 'th-new');
      onEvent({ event: 'metadata', thread_id: 'th-new', run_id: 'run-1' });
      return {};
    });
    const hook = renderHookWithProviders(() => useChatMessages('ws', '__default__'));
    await settleMountEffect();
    const onTaken = vi.fn();

    await act(async () => {
      await hook.result.current.handleSendMessage('', null, null, { artifactComments: SHOWN, onTaken });
    });

    expect(mockCreateThread).not.toHaveBeenCalled();
    expect(mockSend.mock.calls[0][2]).toBe('__default__');
    expect(onTaken.mock.calls).toEqual([['th-new']]);
  });
});
