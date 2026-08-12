/**
 * Tests for the hard-stop API surface: cancelWorkflow() and the AbortError
 * handling threaded through streamFetch via sendChatMessageStream.
 *
 * - cancelWorkflow POSTs to /threads/{id}/cancel.
 * - An AbortController.abort() during the stream is treated as an intentional
 *   stop: streamFetch returns { aborted: true } instead of throwing, so callers
 *   never show an error toast or run double cleanup.
 * - The retired softInterruptWorkflow export is gone.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import type { Mock } from 'vitest';

vi.mock('@/api/client', () => {
  const mockPost = vi.fn().mockResolvedValue({ data: {} });
  return {
    api: {
      get: vi.fn().mockResolvedValue({ data: {} }),
      post: mockPost,
      put: vi.fn().mockResolvedValue({ data: {} }),
      delete: vi.fn().mockResolvedValue({ data: {} }),
      patch: vi.fn().mockResolvedValue({ data: {} }),
      defaults: { baseURL: 'http://localhost:8000' },
    },
  };
});

vi.mock('@/lib/supabase', () => ({ supabase: null }));

import { api } from '@/api/client';
import * as apiModule from '../api';
import { cancelWorkflow, sendChatMessageStream, parseThreadIdFromContentLocation } from '../api';

const mockPost = api.post as Mock;

describe('cancelWorkflow', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('throws when threadId is falsy', async () => {
    await expect(cancelWorkflow('')).rejects.toThrow('Thread ID is required');
  });

  it('POSTs to /threads/{id}/cancel and returns response data', async () => {
    mockPost.mockResolvedValue({ data: { success: true } });
    const result = await cancelWorkflow('t-1');
    // Bounded by a 5s timeout so a network-level hang can't block the stop retries.
    // No runId → no run_id query param (backend falls back to latest active run).
    expect(mockPost).toHaveBeenCalledWith('/api/v1/threads/t-1/cancel', undefined, {
      timeout: 5000,
      params: undefined,
    });
    expect(result).toEqual({ success: true });
  });

  it('targets a specific run via the run_id query param when given a runId', async () => {
    mockPost.mockResolvedValue({ data: { success: true } });
    // Pinning the run prevents a slow/retried cancel from hard-cancelling a
    // newer turn the user started after the stopped one tore down.
    await cancelWorkflow('t-1', 'run-9');
    expect(mockPost).toHaveBeenCalledWith('/api/v1/threads/t-1/cancel', undefined, {
      timeout: 5000,
      params: { run_id: 'run-9' },
    });
  });
});

describe('softInterruptWorkflow removal', () => {
  it('is no longer exported', () => {
    expect(
      (apiModule as Record<string, unknown>).softInterruptWorkflow,
    ).toBeUndefined();
  });
});

describe('sendChatMessageStream — AbortError handling', () => {
  let originalFetch: typeof global.fetch;

  beforeEach(() => {
    originalFetch = global.fetch;
  });

  afterEach(() => {
    global.fetch = originalFetch;
  });

  it('returns { aborted: true } when the reader is aborted (no throw)', async () => {
    const abortErr = new DOMException('aborted', 'AbortError');
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      body: {
        getReader: () => ({
          read: vi.fn().mockRejectedValue(abortErr),
        }),
      },
    }) as unknown as typeof fetch;

    const controller = new AbortController();
    const result = await sendChatMessageStream(
      'hi', 'ws-1', 't-1', [], false, () => {}, null, 'ptc',
      'en-US', 'America/New_York', null, null, null, null, null, null, null,
      controller.signal,
    );

    expect(result).toMatchObject({ aborted: true, disconnected: false });
  });

  it('returns { aborted: true } when fetch itself rejects with AbortError', async () => {
    const abortErr = new DOMException('aborted', 'AbortError');
    global.fetch = vi.fn().mockRejectedValue(abortErr) as unknown as typeof fetch;

    const controller = new AbortController();
    const result = await sendChatMessageStream(
      'hi', 'ws-1', 't-1', [], false, () => {}, null, 'ptc',
      'en-US', 'America/New_York', null, null, null, null, null, null, null,
      controller.signal,
    );

    expect(result).toMatchObject({ aborted: true });
  });

  it('still rethrows non-abort stream errors', async () => {
    const boom = new Error('kaboom');
    global.fetch = vi.fn().mockResolvedValue({
      ok: true,
      status: 200,
      headers: new Headers(),
      body: {
        getReader: () => ({
          read: vi.fn().mockRejectedValue(boom),
        }),
      },
    }) as unknown as typeof fetch;

    await expect(
      sendChatMessageStream('hi', 'ws-1', 't-1', [], false, () => {}),
    ).rejects.toThrow('kaboom');
  });
});

describe('parseThreadIdFromContentLocation', () => {
  it('extracts the thread id from a Content-Location stream URL', () => {
    expect(
      parseThreadIdFromContentLocation(
        '/api/v1/threads/abc-123/messages/stream?run_id=run-9',
      ),
    ).toBe('abc-123');
  });

  it('decodes percent-encoded ids and ignores the query string', () => {
    expect(
      parseThreadIdFromContentLocation(
        '/api/v1/threads/t%2F1/messages/stream?run_id=r',
      ),
    ).toBe('t/1');
  });

  it('returns null for missing / malformed values', () => {
    expect(parseThreadIdFromContentLocation(null)).toBeNull();
    expect(parseThreadIdFromContentLocation(undefined)).toBeNull();
    expect(parseThreadIdFromContentLocation('')).toBeNull();
    expect(parseThreadIdFromContentLocation('/api/v1/threads/messages')).toBeNull();
  });

  it('returns null (does not throw) on malformed percent-encoding', () => {
    expect(
      parseThreadIdFromContentLocation(
        '/api/v1/threads/%ZZ/messages/stream?run_id=r',
      ),
    ).toBeNull();
  });
});
