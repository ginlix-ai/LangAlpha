/**
 * An edit or regenerate can come long after the transcript last changed, so it
 * must send what the hook holds now (platform, locale, timezone, runtime), not
 * what it held at that change.
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
  getStoredThreadId: vi.fn().mockReturnValue('thread-1'),
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

import { sendChatMessageStream, fetchThreadTurns, replayThreadHistory } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSendStream = sendChatMessageStream as Mock;

describe('useChatMessages – a fork sends the current render', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (fetchThreadTurns as Mock).mockResolvedValue({
      turns: [{ turn_index: 0, edit_checkpoint_id: 'cp-in', regenerate_checkpoint_id: 'cp-0' }],
    });
    mockSendStream.mockImplementation(async (...args: unknown[]) => {
      (args[3] as { onEvent: (e: Record<string, unknown>) => void }).onEvent({ event: 'thread_id', thread_id: 'thread-1' });
      return { disconnected: false };
    });
  });

  it('regenerates and edits with a platform that changed after the last message', async () => {
    let platform = 'web';
    const { result, rerender } = renderHookWithProviders(() =>
      useChatMessages('ws-test', null, null, null, null, null, null, 'ptc', null, null, platform));
    await waitFor(() => expect(replayThreadHistory).toHaveBeenCalled());
    await act(async () => {});

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });
    expect((mockSendStream.mock.lastCall?.[3] as { platform: string } | undefined)?.platform).toBe('web');

    // Nothing in the transcript moves with it.
    platform = 'desktop';
    rerender();

    const assistantId = result.current.messages.find((m) => m.role === 'assistant')!.id;
    await act(async () => {
      await result.current.handleRegenerate(assistantId);
    });
    expect((mockSendStream.mock.lastCall?.[3] as { platform: string } | undefined)?.platform).toBe('desktop');

    platform = 'mobile';
    rerender();
    const userId = result.current.messages.find((m) => m.role === 'user')!.id;
    await act(async () => {
      await result.current.handleEditMessage(userId, 'hello again');
    });
    expect((mockSendStream.mock.lastCall?.[3] as { platform: string } | undefined)?.platform).toBe('mobile');
  });
});
