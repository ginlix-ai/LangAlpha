/**
 * A send hands the stream the thread settings its caller named, in the
 * trailing settings object, and names none of its own: whether a send carries
 * the subagents setting is the composer's call, since only it knows the mode
 * and whether this send creates the thread.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect } from './chatHookHarness';

vi.mock('react-i18next', () => {
  const t = (k: string) => k;
  return { useTranslation: () => ({ t }) };
});

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { sendChatMessageStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;

/** The `threadSettings` option of a send. */
const settingsOf = (args: unknown[]) => (args[3] as { threadSettings?: { subagentsAllowed?: boolean } }).threadSettings as { subagentsAllowed?: boolean };

describe('useChatMessages: thread settings on a send', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSendStream.mockResolvedValue({ disconnected: false, aborted: false, contentLocation: null });
  });

  it('passes the subagents setting the caller named', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test', 'thread-1'));
    await settleMountEffect();

    await act(async () => {
      await result.current.handleSendMessage('hello', null, null, { subagentsAllowed: false });
    });
    expect(settingsOf(mockSendStream.mock.calls[0])).toEqual({ subagentsAllowed: false });
  });

  it('names none when the caller does not', async () => {
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test', 'thread-1'));
    await settleMountEffect();

    await act(async () => {
      await result.current.handleSendMessage('hello', null, null, { model: 'm1' });
    });
    expect(settingsOf(mockSendStream.mock.calls[0]).subagentsAllowed).toBeUndefined();
  });
});
