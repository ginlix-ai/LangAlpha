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
    await waitFor(() => expect(result.current.messageError).toBe(SIGNED_OUT));
    expect(result.current.messages.filter((m) => m.role === 'assistant')).toEqual([]);
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
