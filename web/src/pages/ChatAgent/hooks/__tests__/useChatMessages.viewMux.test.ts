/**
 * A view's subagent mux belongs to the view. A new chat's view streams its
 * first turn throughout, so its load skips and registers no cleanup; a later
 * load or view of the thread then rebuilds the turn's cards as running, and
 * only a mux that asks for the open turn again closes the ones that settled.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus, captureMuxConnections } from './chatHookHarness';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (k: string) => k }),
}));

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('../utils/threadStorage', () => ({
  getStoredThreadId: vi.fn().mockReturnValue(null),
  setStoredThreadId: vi.fn(),
  removeStoredThreadId: vi.fn(),
}));

vi.mock('../../utils/api', async () =>
  (await import('./chatHookHarness')).apiMockModule({
    createThreadWithTitle: vi.fn().mockResolvedValue({ thread_id: 'th' }),
  }),
);

import { getWorkflowStatus, replayThreadHistory, sendChatMessageStream, openThreadMuxStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockSend = sendChatMessageStream as Mock;

type OnEvent = (e: Record<string, unknown>) => void;

const spawn = (taskId: string): Record<string, unknown> => ({
  event: 'artifact',
  artifact_type: 'task',
  agent: 'main',
  thread_id: 'th',
  tool_call_id: `tc-${taskId}`,
  payload: { task_id: taskId, action: 'spawned' },
});

/** A new chat whose first turn is still streaming after launching a task,
 * its view moved onto the thread's id with its load skipped. */
async function newChatMidTurn() {
  const muxConns = captureMuxConnections(openThreadMuxStream as Mock);
  let emit!: OnEvent;
  let endTurn!: () => void;
  mockSend.mockImplementation((...args: unknown[]) => {
    emit = (args[3] as { onEvent: OnEvent }).onEvent;
    return new Promise((resolve) => {
      endTurn = () => resolve({ disconnected: false });
    });
  });
  let tid = '__default__';
  const view = renderHookWithProviders(() => useChatMessages('ws', tid));
  await settleMountEffect();
  act(() => {
    void view.result.current.handleSendMessage('run the analyses');
  });
  await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(1));
  act(() => emit(spawn('t1')));
  await waitFor(() => expect(muxConns.length).toBe(1));
  tid = 'th';
  view.rerender();
  await settleMountEffect();
  expect(mockReplay).not.toHaveBeenCalled();
  return { view, muxConns, emit: (e: Record<string, unknown>) => emit(e), endTurn: () => endTurn() };
}

describe('useChatMessages: the subagent mux belongs to its view', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockResolvedValue(undefined);
    mockStatus.mockResolvedValue(threadStatus());
  });

  it('ends the mux on unmount, so the next view of the thread asks for the open turn again', async () => {
    const { view, muxConns, emit } = await newChatMidTurn();

    act(() => view.unmount());
    expect(muxConns[0].signal.aborted).toBe(true);

    // The unmounted view's turn reader runs on and launches another task.
    act(() => emit(spawn('t2')));
    await settleMountEffect();
    expect(muxConns.length).toBe(1);

    mockStatus.mockResolvedValue(threadStatus({ active_tasks: ['t1', 't2'] }));
    renderHookWithProviders(() => useChatMessages('ws', 'th'));
    await settleMountEffect();
    await waitFor(() => expect(muxConns.length).toBe(2));
    expect(muxConns[1].openTurn).toBe(true);
  });

  it('starts a new mux when the same view reloads after its turn', async () => {
    const { muxConns, endTurn } = await newChatMidTurn();

    // The server asks for a reload mid-turn; it runs when the turn ends.
    mockStatus.mockResolvedValue(threadStatus({ active_tasks: ['t1'] }));
    await act(async () => {
      muxConns[0].push('event: resync_required\ndata: {"scope":"thread"}\n\n');
      endTurn();
      await new Promise((r) => setTimeout(r, 0));
    });
    await waitFor(() => expect(mockReplay).toHaveBeenCalledTimes(1));
    await settleMountEffect();
    expect(muxConns[0].signal.aborted).toBe(true);
    await waitFor(() => expect(muxConns.length).toBe(2));
    expect(muxConns[1].openTurn).toBe(true);
  });
});
