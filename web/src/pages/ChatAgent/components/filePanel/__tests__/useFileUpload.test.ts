import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, renderHook } from '@testing-library/react';

vi.mock('../../../utils/workspaceUpload', async (importOriginal) => {
  const real = await importOriginal<typeof import('../../../utils/workspaceUpload')>();
  return { ...real, uploadToWorkspace: vi.fn() };
});

import { ApiError } from '@/api/client';
import { uploadToWorkspace, UploadSendError, type WorkspaceUploadOptions } from '../../../utils/workspaceUpload';
import { useFileUpload } from '../useFileUpload';

const upload = uploadToWorkspace as Mock;
const file = new File(['x'], 'a.bin');

/** An upload the test drives: report progress through it, then settle it. */
function heldUpload() {
  let options!: WorkspaceUploadOptions;
  let settle!: { resolve: (v: unknown) => void; reject: (e: unknown) => void };
  upload.mockImplementation((_ws: string, _file: File, opts: WorkspaceUploadOptions) => {
    options = opts;
    return new Promise((resolve, reject) => { settle = { resolve, reject }; });
  });
  return {
    get options() { return options; },
    get settle() { return settle; },
  };
}

beforeEach(() => {
  upload.mockReset();
});

describe('useFileUpload', () => {
  it('shows the percentage while sending, then the saving state, then refreshes the listing', async () => {
    const held = heldUpload();
    const onRefreshFiles = vi.fn();
    const { result } = renderHook(() => useFileUpload({ workspaceId: 'ws-1', onRefreshFiles }));

    let done!: Promise<void>;
    act(() => { done = result.current.handleUpload(file); });
    act(() => held.options.onProgress?.({ phase: 'sending', percent: 42 }));
    expect([result.current.uploadProgress, result.current.uploadSaving]).toEqual([42, false]);
    act(() => held.options.onProgress?.({ phase: 'saving' }));
    expect([result.current.uploadProgress, result.current.uploadSaving]).toEqual([null, true]);

    await act(async () => { held.settle.resolve({ path: 'a.bin' }); await done; });
    expect([result.current.uploadProgress, result.current.uploadSaving]).toEqual([null, false]);
    expect(onRefreshFiles).toHaveBeenCalledTimes(1);
  });

  it('shows the parts sent again after saving as sending, cancellable again', () => {
    const held = heldUpload();
    const { result } = renderHook(() => useFileUpload({ workspaceId: 'ws-1' }));
    act(() => { void result.current.handleUpload(file); });
    act(() => held.options.onProgress?.({ phase: 'saving' }));
    act(() => held.options.onProgress?.({ phase: 'sending', percent: 97 }));
    expect([result.current.uploadProgress, result.current.uploadSaving]).toEqual([97, false]);
  });

  it('takes no second upload while one is running', () => {
    heldUpload();
    const { result } = renderHook(() => useFileUpload({ workspaceId: 'ws-1' }));
    act(() => { void result.current.handleUpload(file); });
    act(() => { void result.current.handleUpload(file); });
    expect(upload).toHaveBeenCalledTimes(1);
  });

  it('cancelling aborts the upload and shows no error', async () => {
    const held = heldUpload();
    const { result } = renderHook(() => useFileUpload({ workspaceId: 'ws-1' }));
    let done!: Promise<void>;
    act(() => { done = result.current.handleUpload(file); });

    act(() => result.current.cancelUpload());
    expect(held.options.signal?.aborted).toBe(true);
    await act(async () => { held.settle.reject(new ApiError('canceled', 'ERR_CANCELED')); await done; });
    expect(result.current.uploadError).toBeNull();
    expect(result.current.uploadProgress).toBeNull();
  });

  it.each([
    ['a string detail', new ApiError('Request failed with status code 409', 'ERR_BAD_REQUEST', {
      data: { detail: 'This upload has ended. Start it again.' }, status: 409, statusText: '', headers: {},
    }), 'This upload has ended. Start it again.'],
    ['a structured detail', new ApiError('Request failed with status code 409', 'ERR_BAD_REQUEST', {
      data: { detail: { message: 'Some parts of the file did not arrive.', missing_parts: [2] } },
      status: 409, statusText: '', headers: {},
    }), 'Some parts of the file did not arrive.'],
    ['parts that could not be sent', new UploadSendError(),
      'The file could not be sent. Check your connection and try again.'],
  ])('shows the reason for %s', async (_label, error, shown) => {
    const held = heldUpload();
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { result } = renderHook(() => useFileUpload({ workspaceId: 'ws-1' }));
    let done!: Promise<void>;
    act(() => { done = result.current.handleUpload(file); });
    await act(async () => { held.settle.reject(error); await done; });
    expect(result.current.uploadError).toBe(shown);
  });
});
