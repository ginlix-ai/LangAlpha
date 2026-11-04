/**
 * A Claude Code turn the server cannot run, refused at admission or by
 * Anthropic mid-turn, reads as the server's own answer: its words, and no
 * second "Failed to send message" under them that a retry would not fix.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string, o?: Record<string, unknown>) => (o ? `${k}:${JSON.stringify(o)}` : k) }),
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
import { CLAUDE_CODE_CHOICE_URL } from '@/lib/claudeCode';
import { queryKeys } from '@/lib/queryKeys';

const mockSendStream = sendChatMessageStream as Mock;


const SIGNED_OUT = 'Add a setup token for LangAlpha on the server to use this model.';

describe('useChatMessages: a Claude Code refusal', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('at admission shows the refusal alone, with no failed reply under it', async () => {
    mockSendStream.mockRejectedValue(
      Object.assign(new Error(SIGNED_OUT), { status: 400, errorInfo: { type: 'claude_code_off', message: SIGNED_OUT } }),
    );
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await waitFor(() => expect(result.current.messageError).toEqual({ message: SIGNED_OUT }));
    expect(result.current.messages.filter((m) => m.role === 'assistant')).toEqual([]);
  });

  it('at admission words a refused token in the app, linking its sheet', async () => {
    const worded = 'Claude refused your setup token.';
    mockSendStream.mockRejectedValue(
      Object.assign(new Error(worded), { status: 400, errorInfo: { type: 'claude_code_off', message: worded, reason: 'token_refused' } }),
    );
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await waitFor(() => expect(result.current.messageError).toMatchObject({
      message: 'chat.claudeCode.off.token_refused',
      links: [{ labelKey: 'modelAccess.claudeCode.add' }],
    }));
  });

  it("at admission names the user's cap on turns in the app's words", async () => {
    const worded = 'You already have 3 Claude Code threads running.';
    mockSendStream.mockRejectedValue(
      Object.assign(new Error(worded), { status: 400, errorInfo: { type: 'claude_code_off', message: worded, reason: 'too_many_turns', limit: 3 } }),
    );
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await waitFor(() => expect(result.current.messageError).toEqual({ message: 'chat.claudeCode.off.too_many_turns:{"n":3}' }));
  });

  it('at admission links a cloud computer kept off to Model access, and reads the choice for it again', async () => {
    const worded = "Claude Code doesn't run on cloud computers for your account yet.";
    mockSendStream.mockRejectedValue(
      Object.assign(new Error(worded), { status: 400, errorInfo: { type: 'claude_code_off', message: worded, reason: 'hosted_off' } }),
    );
    const { result, queryClient } = renderHookWithProviders(() => useChatMessages('ws-test'));
    const reread = vi.spyOn(queryClient, 'invalidateQueries');
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await waitFor(() => expect(result.current.messageError).toMatchObject({
      message: 'chat.claudeCode.off.hosted_off',
      links: [{ url: CLAUDE_CODE_CHOICE_URL, labelKey: 'modelAccess.title' }],
    }));
    expect(reread).toHaveBeenCalledWith({ queryKey: queryKeys.user.claudeCodeCustody() });
  });

  it.each(['api_key_refused', 'needs_bigger_computer'])("mid-turn words %s on the turn in the app's words", async (reason) => {
    mockSendStream.mockImplementation(async (_m: string, _w: string, _t: string | null, { onEvent }: { onEvent: (e: Record<string, unknown>) => void }) => {
      onEvent({ event: 'error', error: 'Server words.', message: 'Server words.', type: 'claude_code_off', reason });
      return { disconnected: false };
    });
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await waitFor(() => {
      const reply = result.current.messages.find((m) => m.role === 'assistant') as { structuredError?: unknown } | undefined;
      expect(reply?.structuredError).toEqual({ message: `chat.claudeCode.off.${reason}` });
    });
  });

  it('mid-turn shows the same words on the turn', async () => {
    mockSendStream.mockImplementation(async (_m: string, _w: string, _t: string | null, { onEvent }: { onEvent: (e: Record<string, unknown>) => void }) => {
      onEvent({ event: 'error', error: SIGNED_OUT, message: SIGNED_OUT, type: 'claude_code_off' });
      return { disconnected: false };
    });
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    await waitFor(() => {
      const reply = result.current.messages.find((m) => m.role === 'assistant') as { structuredError?: unknown } | undefined;
      expect(reply?.structuredError).toEqual({ message: SIGNED_OUT });
    });
  });
});
