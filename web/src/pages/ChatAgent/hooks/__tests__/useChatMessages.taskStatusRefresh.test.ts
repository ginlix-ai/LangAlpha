/**
 * Refresh-window regression locks for the report-back + task-card contract.
 *
 * Bug A — duplicate report-back bubble: a report-back turn is persisted (so a
 * reload's replay renders it) while its outbox job is still open, so /status
 * still names the run and recents don't list it yet. The replay stamps the
 * terminal turn's run_id on its user_message; the client records those ids as
 * rendered BEFORE arming the watch, so neither the load-time seed nor a late
 * wake re-attaches a run whose turn is already on screen. The wake-queue path
 * bypasses /status entirely, so this client-side dedup is the structural fix
 * (the server-side recents union only narrows the window).
 *
 * Bug B — false Completed: a stream-end sweep used to advance EVERY running
 * card absent from muxOpenTaskIds() (absence-of-channel-as-terminal). Terminal
 * status must arrive per task — a chan_close carrying the run's real outcome —
 * and a sibling with no channel event stays running until its own closure.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import { settleMountEffect, threadStatus, captureWatchCalls, captureMuxConnections } from './chatHookHarness';

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

import { getWorkflowStatus, replayThreadHistory, reconnectToWorkflowStream, watchThread, openThreadMuxStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import type { AssistantMessage } from '@/types/chat';

const mockStatus = getWorkflowStatus as Mock;
const mockReplay = replayThreadHistory as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;
const mockWatch = watchThread as Mock;

/** A reconnect reader that streams one chunk and resolves — a successful attach. */
function streamedReconnect() {
  return (...args: unknown[]) => {
    const onEvent = args[3] as (e: Record<string, unknown>) => void;
    onEvent({ event: 'message_chunk', role: 'assistant', agent: 'main', content_type: 'text', content: 'summary…' });
    return Promise.resolve({ disconnected: false, aborted: false });
  };
}

describe('useChatMessages — refresh-window locks (replay dedup + per-task closure)', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockReplay.mockResolvedValue(undefined);
    mockStatus.mockResolvedValue(threadStatus());
  });

  it('refresh in the post-finalize/pre-ack window does NOT re-attach the replayed report-back run', async () => {
    // The measured ~3s window: turn persisted (replay carries it, run_id
    // stamped), job still open (/status names the run, recents blind).
    mockStatus.mockResolvedValue(threadStatus({
      pending_report_back: true,
      report_back_run_id: 'rb-window',
      recent_report_back_run_ids: [],
      latest_turn_index: 1,
    }));
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'dispatch instruction', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'dispatched' });
      onEvent({ event: 'user_message', turn_index: 1, content: 'report back instruction', role: 'user', run_id: 'rb-window' });
      onEvent({ event: 'message_chunk', turn_index: 1, role: 'assistant', agent: 'main', content_type: 'text', content: 'summary…' });
      return Promise.resolve();
    });
    mockReconnect.mockImplementation(streamedReconnect());
    const watchCalls = captureWatchCalls(mockWatch);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-vp', 'th-vp'));
    await waitFor(() => expect(mockWatch).toHaveBeenCalledTimes(1));
    await settleMountEffect();

    // The turn rendered exactly once, and the load-time seed (status still
    // naming rb-window) was suppressed by the replay stamp.
    const serialized = result.current.messages.map((m) => JSON.stringify(m));
    expect(serialized.filter((s) => s.includes('summary…'))).toHaveLength(1);
    expect(mockReconnect).not.toHaveBeenCalled();

    // A late wake re-naming the rendered run (delayed pub/sub) is a no-op.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-window' });
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockReconnect).not.toHaveBeenCalled();

    // The dedup is per run, not a dead watch: the NEXT report-back attaches.
    await act(async () => {
      await watchCalls[0].cb({ run_id: 'rb-next' });
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(mockReconnect).toHaveBeenCalledTimes(1);
    expect(mockReconnect.mock.calls[0][1]).toBe('rb-next');
  });

  it('a task-run close flips ONLY its own card; a sibling with no channel event stays running', async () => {
    const muxConns = captureMuxConnections(openThreadMuxStream as Mock);
    mockStatus.mockResolvedValue(threadStatus({ active_tasks: ['t1', 't2'], latest_turn_index: 0 }));
    // Reload mid-run: two Task spawns replay as running cards (no ledger
    // stamp yet), and the artifact join maps tool calls to task ids.
    mockReplay.mockImplementation((_tid: string, onEvent: (e: Record<string, unknown>) => void) => {
      onEvent({ event: 'user_message', turn_index: 0, content: 'run two analyses', role: 'user' });
      onEvent({ event: 'message_chunk', turn_index: 0, role: 'assistant', agent: 'main', content_type: 'text', content: 'spawning' });
      onEvent({
        event: 'tool_calls',
        turn_index: 0,
        tool_calls: [
          { id: 'tc-1', name: 'Task', args: { description: 'analysis one', subagent_type: 'research' } },
          { id: 'tc-2', name: 'Task', args: { description: 'analysis two', subagent_type: 'research' } },
        ],
        _eventId: 1,
      });
      onEvent({ event: 'tool_call_result', turn_index: 0, tool_call_id: 'tc-1', content: 'spawned', artifact: { task_id: 't1' } });
      onEvent({ event: 'tool_call_result', turn_index: 0, tool_call_id: 'tc-2', content: 'spawned', artifact: { task_id: 't2' } });
      return Promise.resolve();
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws-vp', 'th-vp'));
    await settleMountEffect();
    await waitFor(() => expect(muxConns.length).toBe(1));

    const chips = () => {
      const aMsg = result.current.messages.find(
        (m) => m.role === 'assistant' && Object.keys((m as AssistantMessage).subagentTasks ?? {}).length > 0,
      ) as AssistantMessage;
      const tasks = aMsg.subagentTasks ?? {};
      return {
        t1: tasks['tc-1'].status,
        t2: tasks['tc-2'].status,
      };
    };
    await waitFor(() => expect(chips()).toEqual({ t1: 'running', t2: 'running' }));

    // Both channels open; t1's run reaches its ledger-terminal close. Only
    // t1 flips — t2's open channel keeps writing.
    await act(async () => {
      muxConns[0].push('event: chan_open\ndata: {"chan":"run:r1","lane":"task:t1","mode":"replay"}\n\n');
      muxConns[0].push('event: chan_open\ndata: {"chan":"run:r2","lane":"task:t2","mode":"replay"}\n\n');
      muxConns[0].push('event: chan_close\ndata: {"chan":"run:r1","reason":"terminal","outcome":"completed"}\n\n');
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(chips()).toEqual({ t1: 'completed', t2: 'running' });

    // Positive closure honors the real outcome — never painted as success.
    await act(async () => {
      muxConns[0].push('event: chan_close\ndata: {"chan":"run:r2","reason":"terminal","outcome":"error"}\n\n');
      await new Promise((r) => setTimeout(r, 0));
    });
    expect(chips()).toEqual({ t1: 'completed', t2: 'error' });
  });
});
