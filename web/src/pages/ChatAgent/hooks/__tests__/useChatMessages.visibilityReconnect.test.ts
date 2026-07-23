/**
 * Regression: returning a backgrounded tab to the foreground must proactively
 * recover an in-flight main stream rather than hang.
 *
 * iOS Safari freezes a backgrounded tab and tears down its SSE socket; the
 * frozen `reader.read()` may not reject promptly on resume, so nothing kicks
 * the existing reconnect and the UI looks frozen. `useChatMessages` registers a
 * `visibilitychange`/`pageshow` handler that — only when a main stream is
 * genuinely active and reconnectable — flags a background abort and aborts the
 * (likely dead) reader. The stream's result handler then re-kicks the existing
 * `attemptReconnectAfterDisconnect` machinery (status check → reconnect stream)
 * instead of treating the abort as a user stop.
 *
 * We use the REAL hook internals and mock only the api module: the send path and
 * reconnect path touch sendChatMessageStream / getWorkflowStatus /
 * reconnectToWorkflowStream, none of which need real network.
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

vi.mock('../../utils/api', async () => (await import('./chatHookHarness')).apiMockModule());

import { sendChatMessageStream, getWorkflowStatus, reconnectToWorkflowStream } from '../../utils/api';
import { useChatMessages } from '../useChatMessages';

const mockSend = sendChatMessageStream as Mock;
const mockStatus = getWorkflowStatus as Mock;
const mockReconnect = reconnectToWorkflowStream as Mock;

// A sendChatMessageStream impl that mimics a long-lived SSE stream: it latches
// the run_id (3rd-to-last arg, before signal + requestKey) synchronously so
// currentRunIdRef is set, and the returned promise stays pending until the
// AbortController.signal fires — then it resolves `{ aborted: true }`, exactly
// as the real streamFetch does when the reader is aborted. This keeps
// isLoading true until the foreground handler aborts the stream.
function deferredStreamMock(runId = 'run-1', threadId = 'th-1') {
  return vi.fn((...args: unknown[]) => {
    const latch = args[args.length - 3] as (rid: string, tid: string) => void;
    const signal = args[args.length - 2] as AbortSignal;
    latch(runId, threadId);
    return new Promise((resolve) => {
      const onAbort = () => resolve({ disconnected: false, aborted: true, contentLocation: null });
      if (signal.aborted) onAbort();
      else signal.addEventListener('abort', onAbort, { once: true });
    });
  });
}

const flush = () => new Promise((r) => setTimeout(r, 0));

// Simulate a GENUINE tab suspend — iOS app-background / bfcache fires `pagehide`,
// Chromium background-tab freeze fires `freeze`. The foreground reconnect only
// arms after one of these; a bare `visibilitychange` (desktop alt-tab) must not.
const suspendTab = () => window.dispatchEvent(new Event('pagehide'));

// Start an in-flight main stream and wait until it is live (isLoading true,
// run_id latched). Returns the rendered hook result.
async function startActiveStream(visibility: { value: string }) {
  // Thread is NOT reconnectable on mount so the thread-load effect doesn't
  // reconnect — isolates the post-resume reconnect under test.
  mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
  mockSend.mockImplementation(deferredStreamMock('run-1', 'th-1'));

  const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));

  // Let mount/history settle (no reconnect — can_reconnect:false).
  await act(async () => { await flush(); });

  await act(async () => {
    void result.current.handleSendMessage('hello');
    await flush();
  });
  await waitFor(() => expect(result.current.isLoading).toBe(true));
  expect(mockSend).toHaveBeenCalledTimes(1);

  // From here the run IS reconnectable; isolate the calls the resume produces.
  mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', run_id: 'run-1', active_tasks: [] });
  mockStatus.mockClear();
  mockReconnect.mockClear();

  void visibility; // visibility getter is wired by the suite's beforeEach
  return result;
}

describe('useChatMessages — foreground (visibility) reconnect', () => {
  const visibility = { value: 'visible' };

  beforeEach(() => {
    vi.clearAllMocks();
    visibility.value = 'visible';
    Object.defineProperty(document, 'visibilityState', {
      configurable: true,
      get: () => visibility.value,
    });
  });

  it('re-kicks reconnect for an in-flight main stream when the tab returns to foreground', async () => {
    await startActiveStream(visibility);

    await act(async () => {
      suspendTab();
      document.dispatchEvent(new Event('visibilitychange'));
      await flush();
    });

    // The frozen stream was aborted and the existing reconnect machinery fired.
    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    expect(mockStatus).toHaveBeenCalled();
    expect(mockReconnect.mock.calls[0][0]).toBe('th-1');
    expect(mockReconnect.mock.calls[0][1]).toBe('run-1');
  });

  it('re-kicks reconnect on a pageshow event too', async () => {
    await startActiveStream(visibility);

    await act(async () => {
      suspendTab();
      window.dispatchEvent(new Event('pageshow'));
      await flush();
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    expect(mockStatus).toHaveBeenCalled();
  });

  it('does nothing when the tab becomes visible but no main stream is active', async () => {
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
    renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
    await act(async () => { await flush(); });

    mockStatus.mockClear();
    mockReconnect.mockClear();

    await act(async () => {
      document.dispatchEvent(new Event('visibilitychange'));
      await flush();
    });

    expect(mockReconnect).not.toHaveBeenCalled();
  });

  it('does nothing while the tab is still hidden (event fired but not visible)', async () => {
    await startActiveStream(visibility);

    visibility.value = 'hidden';
    await act(async () => {
      document.dispatchEvent(new Event('visibilitychange'));
      await flush();
    });

    expect(mockReconnect).not.toHaveBeenCalled();
  });

  // Regression guard for the rest of users: a desktop tab-switch fires
  // `visibilitychange` (and sometimes `pageshow`) but NOT `pagehide`/`freeze`,
  // and the SSE socket stays alive. Without a genuine suspend the handler must
  // NOT abort the live stream — else every alt-tab during a turn would needlessly
  // tear it down and flash "Reconnecting…".
  it('does NOT reconnect on a bare visibilitychange with no genuine suspend (desktop alt-tab)', async () => {
    const result = await startActiveStream(visibility);

    // No pagehide/freeze — just the visibility flip a desktop tab-switch produces.
    await act(async () => {
      document.dispatchEvent(new Event('visibilitychange'));
      window.dispatchEvent(new Event('pageshow'));
      await flush();
    });

    expect(mockReconnect).not.toHaveBeenCalled();
    expect(mockStatus).not.toHaveBeenCalled();
    // The live stream was left intact (still loading), not aborted.
    expect(result.current.isLoading).toBe(true);
  });

  // Regression: a brand-new chat sends its FIRST message while the route prop is
  // still '__default__' (it only flips to the real id on the first SSE event,
  // which for a PTC turn can be 5–30s out during sandbox spin-up). The real
  // server-assigned id is latched into threadIdRef by the Content-Location
  // callback BEFORE the first byte. If the user backgrounds the tab in that
  // window and returns, recovery must reconnect to the REAL thread — not bail
  // because the prop reads '__default__'. Pre-fix, attemptReconnectAfterDisconnect
  // (and reconnectToStream) keyed off the prop and broke immediately, so the
  // first-answer window — the most common "ask, switch apps, come back" moment —
  // never recovered.
  it('reconnects to the latched real thread id when a first-turn ("__default__") send is backgrounded', async () => {
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });
    // Latch a REAL id while the prop stays '__default__' — what Content-Location
    // does mid-send before the route updates.
    mockSend.mockImplementation(deferredStreamMock('run-first', 'th-real'));

    const { result } = renderHookWithProviders(() => useChatMessages('ws', '__default__'));
    await act(async () => { await flush(); });

    await act(async () => {
      void result.current.handleSendMessage('hello');
      await flush();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));
    expect(mockSend).toHaveBeenCalledTimes(1);
    // The POST itself carried the '__default__' prop as its thread arg…
    expect(mockSend.mock.calls[0][2]).toBe('__default__');

    // …but the run is now reconnectable under the real id.
    mockStatus.mockResolvedValue({ can_reconnect: true, status: 'running', run_id: 'run-first', active_tasks: [] });
    mockStatus.mockClear();
    mockReconnect.mockClear();

    await act(async () => {
      suspendTab();
      document.dispatchEvent(new Event('visibilitychange'));
      await flush();
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    // Status check + reconnect both target the latched real id, never '__default__'.
    expect(mockStatus.mock.calls[0][0]).toBe('th-real');
    expect(mockReconnect.mock.calls[0][0]).toBe('th-real');
    expect(mockReconnect.mock.calls[0][1]).toBe('run-first');
  });

  // Regression: a steering follow-up sent mid-turn can be DEMOTED to a fresh
  // backend turn (the prior turn went terminal before the POST landed). That
  // demoted turn registers its own controller on mainStreamAbortRef, so the
  // foreground handler can abort it on resume. Its result site must re-kick the
  // existing reconnect — not fall through and finalize the live turn as
  // truncated-complete (the silent-loss bug for the demote path).
  it('re-kicks reconnect for a DEMOTED steering turn when the tab returns to foreground', async () => {
    mockStatus.mockResolvedValue({ can_reconnect: false, status: 'completed' });

    let steeringOnEvent: ((e: Record<string, unknown>) => void) | null = null;
    mockSend.mockImplementation((...args: unknown[]) => {
      const onEvent = args[5] as (e: Record<string, unknown>) => void;
      const latch = args[args.length - 3] as (rid: string, tid: string) => void;
      const signal = args[args.length - 2] as AbortSignal;
      if (mockSend.mock.calls.length === 1) {
        // Primary turn: latch a run so isLoading stays true, then hang. We never
        // abort this stream — demotion reassigns mainStreamAbortRef away from it.
        latch('run-main', 'th-1');
        return new Promise(() => {});
      }
      // Steering POST: capture its onEvent so the test can drive demotion, and
      // resolve { aborted } when its signal fires (what streamFetch does).
      steeringOnEvent = onEvent;
      return new Promise((resolve) => {
        const onAbort = () =>
          resolve({ disconnected: false, aborted: true, contentLocation: null });
        if (signal.aborted) onAbort();
        else signal.addEventListener('abort', onAbort, { once: true });
      });
    });

    const { result } = renderHookWithProviders(() => useChatMessages('ws', 'th-1'));
    await act(async () => { await flush(); });

    // Primary turn → isLoading true.
    await act(async () => {
      void result.current.handleSendMessage('hello');
      await flush();
    });
    await waitFor(() => expect(result.current.isLoading).toBe(true));

    // Follow-up while streaming → handleSendSteering POSTs a second stream.
    await act(async () => {
      void result.current.handleSendMessage('steer me');
      await flush();
    });
    await waitFor(() => expect(mockSend).toHaveBeenCalledTimes(2));
    expect(steeringOnEvent).not.toBeNull();

    // First non-steering frame = backend opened a NEW turn → demotion. The
    // metadata frame carries the demoted run_id, latched into currentRunIdRef.
    await act(async () => {
      steeringOnEvent!({ event: 'metadata', thread_id: 'th-1', run_id: 'run-demoted' });
      await flush();
    });

    // The demoted run is now reconnectable; isolate the resume's calls.
    mockStatus.mockResolvedValue({
      can_reconnect: true,
      status: 'running',
      run_id: 'run-demoted',
      active_tasks: [],
    });
    mockStatus.mockClear();
    mockReconnect.mockClear();

    // Tab returns → foreground handler aborts the demoted stream → re-kick.
    await act(async () => {
      suspendTab();
      document.dispatchEvent(new Event('visibilitychange'));
      await flush();
    });

    await waitFor(() => expect(mockReconnect).toHaveBeenCalled());
    expect(mockStatus).toHaveBeenCalled();
    expect(mockReconnect.mock.calls[0][0]).toBe('th-1');
    expect(mockReconnect.mock.calls[0][1]).toBe('run-demoted');
  });
});
