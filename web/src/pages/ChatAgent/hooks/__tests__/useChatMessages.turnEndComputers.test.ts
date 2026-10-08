/**
 * A turn's end is when the server measures the machine's disk, onto the
 * computer row; the open page has no other reason to re-read that row, so the
 * stream's end has to ask for it. Once per turn, and only where there is a
 * machine: a flash thread has none.
 *
 * The same machine is where a renamed workspace's folder moves, during the
 * sandbox acquisition that precedes a PTC run's first event. The page folds
 * agent paths against the workspace rows' folders, and a settle rewrites a
 * sibling's too, so it re-reads every workspace row when the run's `metadata`
 * event arrives, and again at the turn's end for a stream that joined past it.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock, MockInstance } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { createTestQueryClient, renderHookWithProviders } from '@/test/utils';
import { queryKeys } from '@/lib/queryKeys';
import { settleMountEffect } from './chatHookHarness';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

vi.mock('../useComputers', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../useComputers')>()),
  refreshComputersAfterTurn: vi.fn(),
}));

import { getWorkflowStatus, replayThreadHistory, sendChatMessageStream } from '../../utils/api';
import { refreshComputersAfterTurn } from '../useComputers';
import { useChatMessages } from '../useChatMessages';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSend = sendChatMessageStream as Mock;
const mockRefresh = refreshComputersAfterTurn as Mock;

const CHUNK = { event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'done' };
const METADATA = { event: 'metadata', thread_id: 'th-x', run_id: 'run-1' };

/** Mount a thread with a spied query client. */
async function mountThread(agentMode: string) {
  const queryClient = createTestQueryClient();
  const invalidate = vi.spyOn(queryClient, 'invalidateQueries');
  const rendered = renderHookWithProviders(
    () => useChatMessages('ws-x', 'th-x', null, null, null, null, null, agentMode),
    { queryClient },
  );
  await waitFor(() => expect(mockReplay).toHaveBeenCalled());
  await settleMountEffect();
  return { ...rendered, invalidate };
}

/** Mount a thread and run one turn that streams `events` and completes. */
async function completeOneTurn(agentMode: string, events: Record<string, unknown>[] = [CHUNK]) {
  mockSend.mockImplementation(async (...args: unknown[]) => {
    const onEvent = (args[3] as { onEvent: (e: Record<string, unknown>) => void }).onEvent;
    for (const e of events) onEvent(e);
    return { disconnected: false };
  });
  const mounted = await mountThread(agentMode);
  await act(async () => {
    await mounted.result.current.handleSendMessage('measure');
  });
  await waitFor(() => expect(mounted.result.current.isLoading).toBe(false));
  return mounted;
}

/** How many times the page asked to re-read both this workspace's row and a
 * sibling's: a settle that lands one folder releases it from another row. */
function workspaceRowReads(invalidate: MockInstance) {
  const covers = (prefix: unknown, key: readonly unknown[]) =>
    Array.isArray(prefix) && prefix.every((part, i) => part === key[i]);
  return invalidate.mock.calls.filter(([filters]) => {
    const prefix = (filters as { queryKey?: unknown } | undefined)?.queryKey;
    return covers(prefix, queryKeys.workspaces.detail('ws-x'))
      && covers(prefix, queryKeys.workspaces.detail('ws-sibling'));
  }).length;
}

beforeEach(() => {
  vi.clearAllMocks();
  mockReplay.mockReset();
  mockReplay.mockResolvedValue(undefined);
  mockSend.mockReset();
  mockStatus.mockReset();
  mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
});

describe('useChatMessages, turn end re-reads the machine rows', () => {
  it('asks for the rows once when a PTC turn completes', async () => {
    const { queryClient } = await completeOneTurn('ptc');
    expect(mockRefresh).toHaveBeenCalledTimes(1);
    expect(mockRefresh).toHaveBeenCalledWith(queryClient);
  });

  it('asks for nothing on a flash thread, which has no machine', async () => {
    await completeOneTurn('flash');
    expect(mockRefresh).not.toHaveBeenCalled();
  });
});

describe('useChatMessages, a PTC run re-reads the workspace rows for their folders', () => {
  it('re-reads it when the turn ends, for a stream that never saw the run start', async () => {
    const { invalidate } = await completeOneTurn('ptc', [CHUNK]);
    expect(workspaceRowReads(invalidate)).toBe(1);
  });

  it('re-reads it once when the run starts, before any turn end', async () => {
    let end: () => void = () => {};
    mockSend.mockImplementation(async (...args: unknown[]) => {
      const onEvent = (args[3] as { onEvent: (e: Record<string, unknown>) => void }).onEvent;
      onEvent(METADATA);
      onEvent(CHUNK);
      onEvent(CHUNK);
      return new Promise((r) => { end = () => r({ disconnected: false }); });
    });
    const { result, invalidate } = await mountThread('ptc');
    act(() => {
      void result.current.handleSendMessage('measure');
    });
    await waitFor(() => expect(workspaceRowReads(invalidate)).toBe(1));
    expect(result.current.isLoading).toBe(true);

    await act(async () => { end(); });
    await waitFor(() => expect(result.current.isLoading).toBe(false));
    expect(workspaceRowReads(invalidate)).toBe(2);
  });

  it('re-reads nothing on a flash thread, whose runs move no folder', async () => {
    const { invalidate } = await completeOneTurn('flash', [METADATA, CHUNK]);
    expect(workspaceRowReads(invalidate)).toBe(0);
  });
});
