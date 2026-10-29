import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor, act } from '@testing-library/react';
import { QueryClient } from '@tanstack/react-query';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  listWorkspaceFiles: vi.fn(),
}));

import { listWorkspaceFiles } from '@/pages/ChatAgent/utils/api';
import { useWorkspaceFiles } from '../useWorkspaceFiles';

const mockListFiles = listWorkspaceFiles as Mock;

/** QueryClient with zero retry delay so retried errors resolve instantly. */
function createFastRetryClient() {
  return new QueryClient({
    defaultOptions: {
      queries: { retry: false, gcTime: 0, retryDelay: 0 },
      mutations: { retry: false },
    },
  });
}

describe('useWorkspaceFiles', () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it('returns files from query', async () => {
    const mockData = {
      workspace_id: 'ws-1',
      path: '.',
      files: ['report.md', 'chart.png'],
    };
    mockListFiles.mockResolvedValue(mockData);

    const { result } = renderHookWithProviders(() => useWorkspaceFiles('ws-1'));

    await waitFor(() => expect(result.current.loading).toBe(false));
    expect(result.current.files).toEqual(['report.md', 'chart.png']);
    expect(result.current.error).toBeNull();
    expect(mockListFiles).toHaveBeenCalledWith('ws-1', '.', { autoStart: false, includeSystem: false });
  });

  it('refresh calls listWorkspaceFiles with autoStart: true', async () => {
    mockListFiles.mockResolvedValue({ files: ['file1.txt'] });

    const { result } = renderHookWithProviders(() => useWorkspaceFiles('ws-1'));

    await waitFor(() => expect(result.current.loading).toBe(false));

    mockListFiles.mockResolvedValue({ files: ['file1.txt', 'file2.txt'] });

    await act(async () => {
      await result.current.refresh();
    });

    expect(mockListFiles).toHaveBeenCalledWith('ws-1', '.', { autoStart: true, includeSystem: false });
  });

  it('a burst of refreshes shares the fetch in flight plus one trailing fetch', async () => {
    mockListFiles.mockResolvedValue({ files: [] });
    const { result } = renderHookWithProviders(() => useWorkspaceFiles('ws-1'));
    await waitFor(() => expect(result.current.loading).toBe(false));
    mockListFiles.mockClear();

    let release!: () => void;
    mockListFiles.mockImplementationOnce(
      () => new Promise((resolve) => { release = () => resolve({ files: ['a.md'] }); }),
    );
    mockListFiles.mockResolvedValue({ files: ['a.md', 'b.md'] });

    // A subagent's replayed backlog pings once per write, all in one tick.
    let burst!: Promise<void[]>;
    act(() => {
      burst = Promise.all(Array.from({ length: 20 }, () => result.current.refresh()));
    });
    expect(mockListFiles).toHaveBeenCalledTimes(1);

    await act(async () => {
      release();
      await burst;
    });
    expect(mockListFiles).toHaveBeenCalledTimes(2);
    await waitFor(() => expect(result.current.files).toEqual(['a.md', 'b.md']));
  });

  it('a failed fetch lets its refetch land before the trailing fetch writes', async () => {
    mockListFiles.mockResolvedValue({ files: [] });
    const { result } = renderHookWithProviders(() => useWorkspaceFiles('ws-1'));
    await waitFor(() => expect(result.current.loading).toBe(false));

    let failFirst!: () => void;
    let releaseRefetch!: () => void;
    mockListFiles.mockImplementation((_ws: string, _path: string, opts: { autoStart: boolean }) =>
      new Promise((resolve, reject) => {
        if (!opts.autoStart) {
          // The query's own refetch, started by the failure path, lists the older tree.
          releaseRefetch = () => resolve({ files: ['a.md'] });
        } else if (!failFirst) {
          failFirst = () => reject(new Error('blip'));
        } else {
          resolve({ files: ['a.md', 'b.md'] });
        }
      }),
    );

    let burst!: Promise<void[]>;
    act(() => {
      burst = Promise.all([result.current.refresh(), result.current.refresh()]);
    });
    await act(async () => {
      failFirst();
      await Promise.resolve();
    });
    await act(async () => {
      releaseRefetch();
      await burst;
    });
    await waitFor(() => expect(result.current.files).toEqual(['a.md', 'b.md']));
  });

  it('refresh is no-op when workspaceId is null', async () => {
    const { result } = renderHookWithProviders(() => useWorkspaceFiles(null));

    // Query should be disabled — no API call
    expect(mockListFiles).not.toHaveBeenCalled();

    await act(async () => {
      await result.current.refresh();
    });

    // Still no call after refresh — guard prevents it
    expect(mockListFiles).not.toHaveBeenCalled();
  });

  it('returns error message for non-503 errors', async () => {
    // Use 400 status — the hook only retries 500/503, so 400 fails immediately
    const error: Error & { response?: { status?: number } } = new Error('Bad request');
    error.response = { status: 400 };
    mockListFiles.mockRejectedValue(error);

    const queryClient = createFastRetryClient();
    const { result } = renderHookWithProviders(() => useWorkspaceFiles('ws-1'), { queryClient });

    await waitFor(() => expect(result.current.error).not.toBeNull());
    expect(result.current.error).toBe('Failed to load files');
  });

  it("returns the server's sentence when the machine refused", async () => {
    // An owner's SSH machine that answered and refused is a 422 whose detail
    // says what to fix; a generic line would hide it.
    const error: Error & { response?: { status?: number; data?: { detail?: unknown } } } =
      new Error('Unprocessable');
    error.response = { status: 422, data: { detail: 'dev@203.0.113.9 did not accept our key.' } };
    mockListFiles.mockRejectedValue(error);

    const queryClient = createFastRetryClient();
    const { result } = renderHookWithProviders(() => useWorkspaceFiles('ws-1'), { queryClient });

    await waitFor(() => expect(result.current.error).not.toBeNull());
    expect(result.current.error).toBe('dev@203.0.113.9 did not accept our key.');
    expect(mockListFiles).toHaveBeenCalledTimes(1);
  });

  it('returns sandbox unavailable for 503', async () => {
    // The hook retries 503 errors up to 3 times with delays of 1s+2s+3s = 6s
    // wall clock. Use fake timers and step through them so the test runs in
    // milliseconds instead of starving the worker pool.
    vi.useFakeTimers();
    try {
      const error: Error & { response?: { status?: number } } = new Error('Service unavailable');
      error.response = { status: 503 };
      mockListFiles.mockRejectedValue(error);

      const queryClient = createFastRetryClient();
      const { result } = renderHookWithProviders(() => useWorkspaceFiles('ws-1'), { queryClient });

      // Drain initial query + 3 retry delays. advanceTimersByTimeAsync flushes
      // microtasks between ticks so each retry's promise rejection settles.
      await act(async () => {
        await vi.advanceTimersByTimeAsync(7_000);
      });
      expect(result.current.error).toBe('Sandbox not available');
    } finally {
      vi.useRealTimers();
    }
  });
});
