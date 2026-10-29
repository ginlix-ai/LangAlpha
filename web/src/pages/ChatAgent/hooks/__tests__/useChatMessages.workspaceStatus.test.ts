/**
 * Tests for useChatMessages workspace_status SSE event handling.
 *
 * The hook translates SSE `workspace_status` events into the
 * `workspaceStarting` state consumed by ChatView. We need to verify:
 * - `starting` with `sandbox_state: "archived"` -> 'archived' (slow restore copy)
 * - `starting` without `sandbox_state`          -> 'starting' (generic copy)
 * - `starting` with an unknown `sandbox_state`  -> 'starting' (graceful fallback)
 * - `ready`                                     -> false (spinner clears)
 * - `turn_held` held/released drive `turnHeld` the same way; a reconnect also clears it
 *
 * Note: the hook's `cleanupAfterStreamEnd` resets `workspaceStarting` to
 * false when the stream completes, so we keep the stream open with a
 * controllable promise while asserting mid-flight state.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, waitFor } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks (mirrors useChatMessages.turnIndex.test.ts scaffolding)
// ---------------------------------------------------------------------------

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

import { sendChatMessageStream, getWorkflowStatus, reconnectToWorkflowStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';
import { threadStatus } from './chatHookHarness';

const mockSendStream = sendChatMessageStream as Mock;
const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

type StreamCallback = (e: Record<string, unknown>) => void;

/**
 * Drive the hook with a stream that delivers `events` synchronously via
 * onEvent, then HANGS without resolving so the hook stays mid-stream.
 * Call the returned `resolve` to end the stream.
 */
function mockHangingStream(events: Array<Record<string, unknown>>) {
  let resolve: () => void = () => {};
  const hang = new Promise<{ disconnected: false }>((r) => {
    resolve = () => r({ disconnected: false });
  });
  mockSendStream.mockImplementation(
    async (
      _msg: string,
      _ws: string,
      _tid: string | null,
      { onEvent }: { onEvent: StreamCallback },
    ) => {
      onEvent({ event: 'thread_id', thread_id: 'thread-1' });
      for (const e of events) {
        onEvent(e);
      }
      return hang;
    },
  );
  return { resolve };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

describe('useChatMessages – workspace_status SSE handling', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('starting + sandbox_state="archived" sets workspaceStarting to "archived"', async () => {
    const { resolve } = mockHangingStream([
      { event: 'workspace_status', status: 'starting' },
      { event: 'workspace_status', status: 'starting', sandbox_state: 'archived' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    // Fire without awaiting so stream stays mid-flight.
    act(() => {
      void result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe('archived');
    });

    resolve();
  });

  it('starting without sandbox_state sets workspaceStarting to "starting"', async () => {
    const { resolve } = mockHangingStream([
      { event: 'workspace_status', status: 'starting' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe('starting');
    });

    resolve();
  });

  it('unknown sandbox_state falls back to "starting" (graceful degradation)', async () => {
    // If backend adds a new sandbox_state value the frontend doesn't know about,
    // we must render the generic spinner copy instead of crashing or showing nothing.
    const { resolve } = mockHangingStream([
      { event: 'workspace_status', status: 'starting', sandbox_state: 'destroyed' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe('starting');
    });

    resolve();
  });

  it('ready after starting clears workspaceStarting to false mid-stream', async () => {
    // Explicit ready event must clear the spinner even before the stream ends.
    const { resolve } = mockHangingStream([
      { event: 'workspace_status', status: 'starting', sandbox_state: 'archived' },
      { event: 'workspace_status', status: 'ready' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe(false);
    });

    resolve();
  });

  it('no workspace_status events leaves workspaceStarting at false (warm path)', async () => {
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: StreamCallback },
      ) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    expect(result.current.workspaceStarting).toBe(false);
  });

  it('stream cleanup clears workspaceStarting regardless of last seen state', async () => {
    // Safety net: if ready never comes (backend crashed, network died), the
    // hook's cleanupAfterStreamEnd must still clear the spinner when the stream
    // closes so the UI does not get stuck.
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: StreamCallback },
      ) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        onEvent({ event: 'workspace_status', status: 'starting', sandbox_state: 'archived' });
        // Stream ends without a ready event.
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    // After cleanupAfterStreamEnd runs, spinner should be cleared.
    await waitFor(() => {
      expect(result.current.workspaceStarting).toBe(false);
    });
  });

  it('turn_held held sets turnHeld to the reason', async () => {
    const { resolve } = mockHangingStream([
      { event: 'turn_held', status: 'held', reason: 'handoff' },
    ]);

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      expect(result.current.turnHeld).toBe('handoff');
    });

    resolve();
  });

  it('turn_held released clears turnHeld mid-stream', async () => {
    let emit: StreamCallback = () => {};
    const hang = new Promise<{ disconnected: false }>(() => {});
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: StreamCallback },
      ) => {
        emit = onEvent;
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        onEvent({ event: 'turn_held', status: 'held', reason: 'compact' });
        return hang;
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      expect(result.current.turnHeld).toBe('compact');
    });

    act(() => {
      emit({ event: 'turn_held', status: 'released' });
    });

    expect(result.current.turnHeld).toBeNull();
  });

  it('a reconnect clears turnHeld when the stream dropped before released', async () => {
    mockStatus.mockResolvedValue(threadStatus({ can_reconnect: true, status: 'running' }));
    // The resumed run stream stays open, so only the attach can clear the line.
    mockReconnect.mockImplementation(() => new Promise(() => {}));
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: StreamCallback },
      ) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        onEvent({ event: 'turn_held', status: 'held', reason: 'handoff' });
        return { disconnected: true };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    act(() => {
      void result.current.handleSendMessage('hello');
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    expect(result.current.turnHeld).toBeNull();
  });

  it('a thread switch clears the held line of the thread it leaves', async () => {
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: StreamCallback },
      ) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-A' });
        onEvent({ event: 'turn_held', status: 'held', reason: 'handoff' });
        return new Promise(() => {});
      },
    );
    let tid = 'thread-A';
    const { result, rerender } = renderHookWithProviders(() => useChatMessages('ws-test', tid));

    act(() => {
      void result.current.handleSendMessage('hello');
    });
    await waitFor(() => expect(result.current.turnHeld).toBe('handoff'));

    await act(async () => {
      tid = 'thread-B';
      rerender();
      await Promise.resolve();
    });

    expect(result.current.turnHeld).toBeNull();
  });

  it('stream cleanup clears turnHeld when released never comes', async () => {
    mockSendStream.mockImplementation(
      async (
        _msg: string,
        _ws: string,
        _tid: string | null,
        { onEvent }: { onEvent: StreamCallback },
      ) => {
        onEvent({ event: 'thread_id', thread_id: 'thread-1' });
        onEvent({ event: 'turn_held', status: 'held', reason: 'offload' });
        return { disconnected: false };
      },
    );

    const { result } = renderHookWithProviders(() => useChatMessages('ws-test'));

    await act(async () => {
      await result.current.handleSendMessage('hello');
    });

    await waitFor(() => {
      expect(result.current.turnHeld).toBeNull();
    });
  });
});
