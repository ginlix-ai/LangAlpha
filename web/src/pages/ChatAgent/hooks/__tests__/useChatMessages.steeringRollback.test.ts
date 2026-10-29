/**
 * steering_delivered trims the old bubble back to the steering point. The
 * boundary is nulled right after the rollback is queued, so it has to be read
 * when the event arrives: React only runs an updater at dispatch when the
 * component has no other update pending, and any chunk earlier in the same
 * task leaves one pending.
 */
import { describe, it, expect, vi } from 'vitest';
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

vi.mock('../../session/stream/mainEventHandlers', async (importOriginal) => {
  const original = await importOriginal<typeof import('../../session/stream/mainEventHandlers')>();
  return (await import('./chatHookHarness')).mainHandlersMockModule(original, {
    handleTextContent: original.handleTextContent,
  });
});

vi.mock('../../session/subagents/liveEventHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).subagentHandlersMockModule(await importOriginal()));

vi.mock('../../session/streamRefs', async (importOriginal) =>
  (await import('./chatHookHarness')).streamRefsMockModule(await importOriginal()));

vi.mock('../../session/history/historyHandlers', async (importOriginal) =>
  (await import('./chatHookHarness')).historyHandlersMockModule(await importOriginal()));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { sendChatMessageStream, replayThreadHistory } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage } from '@/types/chat';

const mockSendStream = sendChatMessageStream as Mock;
const mockReplay = replayThreadHistory as Mock;

const text = (id: number, content: string) => ({ event: 'message_chunk', content_type: 'text', content, _eventId: id });

describe('useChatMessages – steering rollback', () => {
  it('drops text that reached the old bubble after the steering point', async () => {
    mockSendStream.mockImplementation(async (...args: unknown[]) => {
      const onEvent = (args[3] as { onEvent: (e: Record<string, unknown>) => void }).onEvent;
      onEvent({ event: 'thread_id', thread_id: 'thread-1' });
      onEvent(text(1, 'kept'));
      onEvent({ event: 'steering_accepted', _eventId: 2 });
      onEvent(text(3, ' leaked'));
      onEvent({ event: 'steering_delivered', _eventId: 4, messages: [{ content: 'follow-up', timestamp: Date.now() / 1000 }] });
      return { disconnected: false };
    });
    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
    await waitFor(() => expect(mockReplay).toHaveBeenCalled());
    await act(async () => {});

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      const [original] = result.current.messages.filter((m): m is AssistantMessage => m.role === 'assistant');
      expect(original.content).toBe('kept');
    });
  });
});
