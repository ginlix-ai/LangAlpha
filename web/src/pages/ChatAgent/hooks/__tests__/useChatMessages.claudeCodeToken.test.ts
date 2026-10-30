/**
 * A Claude Code turn refused for want of a working setup token, at admission
 * or mid-turn, links to where the user adds one; any other Claude Code
 * refusal keeps the server's words alone.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { CLAUDE_CODE_TOKEN_LINK } from '@/lib/claudeCode';
import { queryKeys } from '@/lib/queryKeys';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../session/stream/mainEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).mainHandlersMockModule(await importOriginal()));

vi.mock('../../session/subagents/liveEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).subagentHandlersMockModule(await importOriginal()));

vi.mock('../../session/streamRefs', async (importOriginal) =>
  (await import('./chatHookHarness')).streamRefsMockModule(await importOriginal()));

vi.mock('../../session/history/historyHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).historyHandlersMockModule(await importOriginal()));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;
const WORDS = 'Claude refused your setup token.';

const refuseAtAdmission = (reason: string) => mockSendStream.mockRejectedValue(
  Object.assign(new Error(WORDS), { status: 400, errorInfo: { type: 'claude_code_off', message: WORDS, reason } }),
);

async function send() {
  const hook = renderHookWithProviders(() => useChatMessages('ws-test'));
  const invalidate = vi.spyOn(hook.queryClient, 'invalidateQueries');
  await act(async () => {
    await hook.result.current.handleSendMessage('hello');
  });
  // Settings and the picker reread the token, which the server now calls rejected.
  const reread = () => invalidate.mock.calls.some(([filters]) => JSON.stringify(filters?.queryKey) === JSON.stringify(queryKeys.features.claudeCodeStatus()));
  return { result: hook.result, reread };
}

describe('useChatMessages: a Claude Code turn without a working token', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('at admission links to adding a token, and rereads the token', async () => {
    refuseAtAdmission('signed_out');
    const { result, reread } = await send();
    await waitFor(() => expect(result.current.messageError).toEqual({ message: WORDS, links: [CLAUDE_CODE_TOKEN_LINK] }));
    expect(reread()).toBe(true);
  });

  it('at admission for another reason keeps the words alone', async () => {
    refuseAtAdmission('chief_of_staff');
    const { result, reread } = await send();
    await waitFor(() => expect(result.current.messageError).toBe(WORDS));
    expect(reread()).toBe(false);
  });

  it('mid-turn links the failed turn to adding a token, and rereads the token', async () => {
    mockSendStream.mockImplementation(async (_m: string, _w: string, _t: string | null, { onEvent }: { onEvent: (e: Record<string, unknown>) => void }) => {
      onEvent({ event: 'error', error: WORDS, message: WORDS, type: 'claude_code_off', model: 'claude-x-code', reason: 'signed_out' });
      return { disconnected: false };
    });
    const { result, reread } = await send();
    expect(reread()).toBe(true);
    await waitFor(() => {
      const reply = result.current.messages.find((m) => m.role === 'assistant') as { structuredError?: unknown } | undefined;
      expect(reply?.structuredError).toEqual({ message: WORDS, links: [CLAUDE_CODE_TOKEN_LINK] });
    });
  });
});
