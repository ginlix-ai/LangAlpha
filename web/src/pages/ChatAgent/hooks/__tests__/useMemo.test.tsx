import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { waitFor, act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  listUserMemos: vi.fn(),
  readUserMemo: vi.fn(),
  uploadUserMemo: vi.fn(),
  writeUserMemo: vi.fn(),
  deleteUserMemo: vi.fn(),
  regenerateUserMemo: vi.fn(),
}));

import {
  listUserMemos,
  readUserMemo,
  uploadUserMemo,
  writeUserMemo,
  deleteUserMemo,
  regenerateUserMemo,
} from '@/pages/ChatAgent/utils/api';
import {
  useUserMemoList,
  useReadUserMemo,
  useUploadUserMemo,
  useWriteUserMemo,
  useDeleteUserMemo,
  useRegenerateUserMemo,
} from '../useMemo';

const mockList = listUserMemos as Mock;
const mockRead = readUserMemo as Mock;
const mockUpload = uploadUserMemo as Mock;
const mockWrite = writeUserMemo as Mock;
const mockDelete = deleteUserMemo as Mock;
const mockRegen = regenerateUserMemo as Mock;

beforeEach(() => {
  vi.clearAllMocks();
});

describe('useUserMemoList', () => {
  it('returns the list payload from listUserMemos', async () => {
    const data = { entries: [], truncated: false };
    mockList.mockResolvedValue(data);

    const { result } = renderHookWithProviders(() => useUserMemoList());

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(data);
    expect(mockList).toHaveBeenCalledTimes(1);
  });

  it('does not fetch when disabled', async () => {
    mockList.mockResolvedValue({ entries: [], truncated: false });

    const { result } = renderHookWithProviders(() => useUserMemoList(false));

    // Allow microtasks to flush; the query should remain disabled
    await waitFor(() => expect(result.current.fetchStatus).toBe('idle'));
    expect(mockList).not.toHaveBeenCalled();
  });
});

describe('useReadUserMemo', () => {
  it('does not fetch when key is null', async () => {
    const { result } = renderHookWithProviders(() => useReadUserMemo(null));

    await waitFor(() => expect(result.current.fetchStatus).toBe('idle'));
    expect(mockRead).not.toHaveBeenCalled();
  });

  it('fetches and returns content for a given key', async () => {
    const data = {
      key: 'abc.md',
      content: '# hi',
      encoding: 'utf-8',
      mime_type: 'text/markdown',
      size_bytes: 4,
      original_filename: 'abc.md',
      description: null,
      summary: null,
      metadata_status: 'ready' as const,
      metadata_error: null,
      created_at: null,
      modified_at: null,
      source_kind: null,
      source_workspace_id: null,
      source_path: null,
    };
    mockRead.mockResolvedValue(data);

    const { result } = renderHookWithProviders(() => useReadUserMemo('abc.md'));

    await waitFor(() => expect(result.current.isSuccess).toBe(true));
    expect(result.current.data).toEqual(data);
    expect(mockRead).toHaveBeenCalledWith('abc.md');
  });
});

describe('useUploadUserMemo', () => {
  it('uploads a bare File and invalidates the memo list', async () => {
    mockList.mockResolvedValue({ entries: [], truncated: false });
    mockUpload.mockResolvedValue({
      key: 'abc.md',
      original_filename: 'abc.md',
      metadata_status: 'pending',
    });

    // Pre-populate the list query so we can observe re-fetch on invalidation
    const { result, queryClient } = renderHookWithProviders(() => ({
      list: useUserMemoList(),
      upload: useUploadUserMemo(),
    }));

    await waitFor(() => expect(result.current.list.isSuccess).toBe(true));
    expect(mockList).toHaveBeenCalledTimes(1);

    const file = new File(['x'], 'abc.md', { type: 'text/markdown' });
    await act(async () => {
      await result.current.upload.mutateAsync(file);
    });

    // Was called with the bare File and no extra args
    expect(mockUpload).toHaveBeenCalledWith(file);
    // The memo list cache was invalidated → refetch
    await waitFor(() => expect(mockList).toHaveBeenCalledTimes(2));
    // Sanity: queryClient still wired
    expect(queryClient).toBeDefined();
  });

  it('uploads with source when given { file, source }', async () => {
    mockUpload.mockResolvedValue({
      key: 'abc.md',
      original_filename: 'abc.md',
      metadata_status: 'pending',
    });

    const { result } = renderHookWithProviders(() => useUploadUserMemo());

    const file = new File(['x'], 'abc.md', { type: 'text/markdown' });
    const source = {
      source_kind: 'sandbox' as const,
      source_workspace_id: 'ws-1',
      source_path: 'work/abc.md',
    };
    await act(async () => {
      await result.current.mutateAsync({ file, source });
    });

    expect(mockUpload).toHaveBeenCalledWith(file, null, source);
  });
});

describe('useWriteUserMemo', () => {
  it('calls writeUserMemo and invalidates list + read queries', async () => {
    mockList.mockResolvedValue({ entries: [], truncated: false });
    mockRead.mockResolvedValue({ key: 'abc.md', content: 'old', encoding: 'utf-8' });
    mockWrite.mockResolvedValue({
      key: 'abc.md',
      original_filename: 'abc.md',
      metadata_status: 'pending',
    });

    const { result } = renderHookWithProviders(() => ({
      list: useUserMemoList(),
      read: useReadUserMemo('abc.md'),
      write: useWriteUserMemo(),
    }));

    await waitFor(() => expect(result.current.list.isSuccess).toBe(true));
    await waitFor(() => expect(result.current.read.isSuccess).toBe(true));
    expect(mockList).toHaveBeenCalledTimes(1);
    expect(mockRead).toHaveBeenCalledTimes(1);

    await act(async () => {
      await result.current.write.mutateAsync({ key: 'abc.md', content: '# new' });
    });

    expect(mockWrite).toHaveBeenCalledWith('abc.md', '# new');
    // Both queries should refetch as part of invalidation. The read query is
    // covered by both the broad memo.all invalidation and the specific
    // memo.read(key) invalidation, so the call count just needs to grow.
    await waitFor(() => expect(mockList.mock.calls.length).toBeGreaterThanOrEqual(2));
    await waitFor(() => expect(mockRead.mock.calls.length).toBeGreaterThanOrEqual(2));
  });
});

describe('useDeleteUserMemo', () => {
  it('calls deleteUserMemo and invalidates the list', async () => {
    mockList.mockResolvedValue({ entries: [], truncated: false });
    mockDelete.mockResolvedValue(undefined);

    const { result } = renderHookWithProviders(() => ({
      list: useUserMemoList(),
      del: useDeleteUserMemo(),
    }));

    await waitFor(() => expect(result.current.list.isSuccess).toBe(true));

    await act(async () => {
      await result.current.del.mutateAsync('abc.md');
    });

    expect(mockDelete).toHaveBeenCalledWith('abc.md');
    await waitFor(() => expect(mockList).toHaveBeenCalledTimes(2));
  });
});

describe('useRegenerateUserMemo', () => {
  it('calls regenerateUserMemo with the key', async () => {
    mockRegen.mockResolvedValue({
      key: 'abc.md',
      original_filename: 'abc.md',
      metadata_status: 'pending',
    });

    const { result } = renderHookWithProviders(() => useRegenerateUserMemo());

    await act(async () => {
      await result.current.mutateAsync('abc.md');
    });

    expect(mockRegen).toHaveBeenCalledWith('abc.md');
  });
});
