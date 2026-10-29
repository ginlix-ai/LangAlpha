/**
 * An edit or regenerate draws its bubbles before it reads the checkpoint it
 * forks from, and the fork's own bubbles take their ids: a new id remounts the
 * bubble, so a message just sent and its turn's row would play their entrance
 * a second time once the read came back.
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
const turns = { turns: [{ turn_index: 0, edit_checkpoint_id: 'cp-in', regenerate_checkpoint_id: 'cp-0' }] };

type Hook = { current: ReturnType<typeof useChatMessages> };
const ids = (result: Hook) => result.current.messages.map((m) => ({ id: m.id, role: m.role }));

async function sent() {
  const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));
  await waitFor(() => expect(replayThreadHistory).toHaveBeenCalled());
  await act(async () => {});
  await act(async () => {
    await result.current.handleSendMessage('hello');
  });
  return result;
}

/** Holds the checkpoint read open until the bubbles it draws are on screen,
 * then lets a millisecond pass, so an id minted after the read would differ. */
async function forkHeldAtRead(result: Hook, fork: () => Promise<void>) {
  let release!: (value: typeof turns) => void;
  (fetchThreadTurns as Mock).mockImplementationOnce(() => new Promise((resolve) => { release = resolve; }));
  let done!: Promise<void>;
  act(() => {
    done = fork();
  });
  await waitFor(() => expect(fetchThreadTurns).toHaveBeenCalled());
  const drawn = ids(result);
  await new Promise((resolve) => setTimeout(resolve, 2));
  await act(async () => {
    release(turns);
    await done;
  });
  return drawn;
}

describe('useChatMessages – a fork keeps the bubbles it drew', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    (fetchThreadTurns as Mock).mockResolvedValue(turns);
    mockSendStream.mockImplementation(async (...args: unknown[]) => {
      (args[3] as { onEvent: (e: Record<string, unknown>) => void }).onEvent({ event: 'thread_id', thread_id: 'thread-1' });
      return { disconnected: false };
    });
  });

  it('keeps the edited message and its reply under the ids they were drawn with', async () => {
    const result = await sent();
    const userId = result.current.messages.find((m) => m.role === 'user')!.id;

    const drawn = await forkHeldAtRead(result, () => result.current.handleEditMessage(userId, 'hello again'));
    expect(drawn.map((m) => m.role)).toEqual(['user', 'assistant']);
    expect(drawn[0].id).not.toBe(userId);
    expect(ids(result)).toEqual(drawn);
    expect(mockSendStream.mock.lastCall?.[0]).toBe('hello again');
  });

  it('keeps the regenerated reply under the id it was drawn with', async () => {
    const result = await sent();
    const before = ids(result);

    const drawn = await forkHeldAtRead(result, () => result.current.handleRegenerate(before[1].id));
    expect(drawn[0]).toEqual(before[0]);
    expect(drawn[1].id).not.toBe(before[1].id);
    expect(ids(result)).toEqual(drawn);
  });
});
