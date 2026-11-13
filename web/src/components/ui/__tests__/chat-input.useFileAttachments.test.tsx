/**
 * Composer attachments upload as they are added: each tile goes uploading →
 * ready (or failed, and back on Retry), a removed upload is aborted and
 * discarded on the server, an unmount ends every upload and starts none, and
 * a send waits until every file is ready. The attachment routes are mocked;
 * the uploader between them is real.
 */
import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { act, fireEvent, render, renderHook, screen } from '@testing-library/react';
import { createRef } from 'react';
import { MemoryRouter } from 'react-router';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';

vi.mock('@/lib/supabase', () => ({ supabase: null }));

vi.mock('@/lib/authToken', () => ({
  authGeneration: vi.fn(() => 0),
  bearerTokenOf: vi.fn(() => null),
  getAccessToken: vi.fn(async () => null),
  getAuthHeaders: vi.fn(async () => ({})),
  refreshAccessToken: vi.fn(async () => null),
}));

vi.mock('@/pages/ChatAgent/utils/api/attachments', () => ({
  beginAttachment: vi.fn(),
  sendAttachmentContent: vi.fn(),
  signAttachmentParts: vi.fn(),
  completeAttachment: vi.fn(),
  checkAttachment: vi.fn(),
  deleteAttachment: vi.fn(),
  deleteAttachmentOnUnload: vi.fn(),
}));

vi.mock('@/pages/ChatAgent/utils/api', () => ({
  getSkills: vi.fn().mockResolvedValue([]),
  getModelMetadata: vi.fn().mockResolvedValue({}),
}));

vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ data: undefined, isLoading: false }),
}));

vi.mock('../use-toast', () => ({
  useToast: () => ({ toast: vi.fn() }),
  toast: vi.fn(),
}));

import { ApiError } from '@/api/client';
import i18n from '@/i18n';
import { reloadHeld } from '@/lib/reloadHolds';
import {
  beginAttachment,
  checkAttachment,
  deleteAttachment,
  sendAttachmentContent,
} from '@/pages/ChatAgent/utils/api/attachments';
import ChatInput, { type ChatInputHandle } from '../chat-input';
import { MAX_FILES } from '../chat-input.helpers';
import type { ReadyAttachment } from '../chat-input.types';
import { RESTAGE_AFTER_MS, useFileAttachments } from '../chat-input.useFileAttachments';

const begin = beginAttachment as Mock;
const relay = sendAttachmentContent as Mock;
const discard = deleteAttachment as Mock;
const check = checkAttachment as Mock;

const csv = (name = 'data.csv') => new File(['a,b\n1,2\n'], name, { type: 'text/csv' });
const ready = (id: string) => ({ attachment_id: id, state: 'ready', name: 'data.csv', size: 8, mime_type: 'text/csv', error: null });

/** A relay upload the test finishes by hand, its progress reported as it goes. */
function heldRelay() {
  const sends: Array<{
    id: string;
    progress: (percent: number) => void;
    signal: AbortSignal;
    resolve: (v: unknown) => void;
    reject: (e: unknown) => void;
  }> = [];
  relay.mockImplementation((id: string, _file: File, progress: (p: number) => void, signal: AbortSignal) =>
    new Promise((resolve, reject) => {
      sends.push({ id, progress, signal, resolve, reject });
      signal.addEventListener('abort', () => reject(new ApiError('canceled', 'ERR_CANCELED')));
    }));
  return sends;
}

const flush = () => act(async () => { await Promise.resolve(); await Promise.resolve(); await Promise.resolve(); });

let nextId = 0;
beforeEach(() => {
  vi.resetAllMocks();
  nextId = 0;
  begin.mockImplementation(async () => ({ attachment_id: `att-${++nextId}`, mode: 'relay', max_bytes: 1 << 20 }));
  discard.mockResolvedValue(undefined);
});

describe('useFileAttachments', () => {
  it('uploads a file as it is added, shows its progress, and holds its id once ready', async () => {
    const sends = heldRelay();
    const { result } = renderHook(() => useFileAttachments());

    act(() => result.current.handleFiles([csv()]));
    expect(result.current.attachedFiles[0].upload).toEqual({ status: 'uploading', percent: 0 });
    // The bytes would be lost to a reload.
    expect(reloadHeld()).toBe(true);
    await flush();
    expect(begin).toHaveBeenCalledWith({ name: 'data.csv', size: 8, mime_type: 'text/csv' });

    act(() => sends[0].progress(42));
    expect(result.current.attachedFiles[0].upload).toEqual({ status: 'uploading', percent: 42 });
    expect(result.current.uploadingCount).toBe(1);

    // All sent: the server is storing it, which has no percent.
    act(() => sends[0].progress(100));
    expect(result.current.attachedFiles[0].upload).toEqual({ status: 'uploading', percent: null });

    await act(async () => { sends[0].resolve(ready('att-1')); });
    expect(result.current.attachedFiles[0].upload).toEqual({ status: 'ready', attachmentId: 'att-1' });
    expect(result.current.uploadingCount).toBe(0);
    expect(reloadHeld()).toBe(false);
    expect(discard).not.toHaveBeenCalled();
  });

  it("shows the server's reason for a refusal, and sends the file again on Retry", async () => {
    const sends = heldRelay();
    const { result } = renderHook(() => useFileAttachments());
    act(() => result.current.handleFiles([csv()]));
    await flush();

    await act(async () => {
      sends[0].reject(new ApiError('Request failed with status code 413', 'ERR_BAD_REQUEST', {
        data: { detail: 'This file is over the 20 MB limit.' }, status: 413, statusText: '', headers: {},
      }));
    });
    expect(result.current.attachedFiles[0].upload).toEqual({
      status: 'failed',
      failure: { kind: 'refused', message: 'This file is over the 20 MB limit.' },
    });
    expect(result.current.failedCount).toBe(1);
    // What it had begun on the server is discarded.
    expect(discard).toHaveBeenCalledWith('att-1');

    act(() => result.current.retryFile(result.current.attachedFiles[0].id));
    expect(result.current.attachedFiles[0].upload).toEqual({ status: 'uploading', percent: 0 });
    await flush();
    await act(async () => { sends[1].resolve(ready('att-2')); });
    expect(result.current.attachedFiles[0].upload).toEqual({ status: 'ready', attachmentId: 'att-2' });
    expect(result.current.failedCount).toBe(0);
  });

  it('aborts an upload removed while it goes, and discards it on the server', async () => {
    const sends = heldRelay();
    const { result } = renderHook(() => useFileAttachments());
    act(() => result.current.handleFiles([csv()]));
    await flush();

    act(() => result.current.removeFile(result.current.attachedFiles[0].id));
    await flush();
    expect(sends[0].signal.aborted).toBe(true);
    expect(discard).toHaveBeenCalledWith('att-1');
    expect(result.current.attachedFiles).toEqual([]);
  });

  it('discards a ready file removed from the composer', async () => {
    const sends = heldRelay();
    const { result } = renderHook(() => useFileAttachments());
    act(() => result.current.handleFiles([csv()]));
    await flush();
    await act(async () => { sends[0].resolve(ready('att-1')); });

    act(() => result.current.removeFile(result.current.attachedFiles[0].id));
    expect(discard).toHaveBeenCalledWith('att-1');
  });

  it('ends every upload with the composer and starts none after it', async () => {
    const sends = heldRelay();
    const { result, unmount } = renderHook(() => useFileAttachments());
    act(() => result.current.handleFiles([csv('a.csv'), csv('b.csv')]));
    await flush();
    expect(sends).toHaveLength(2);
    // Three more whose starts the server has not answered yet.
    act(() => result.current.handleFiles([csv('c.csv'), csv('d.csv'), csv('e.csv')]));

    unmount();
    await flush();
    expect(sends.every((s) => s.signal.aborted)).toBe(true);
    // No upload began after the unmount, and each that had is discarded.
    expect(sends).toHaveLength(2);
    expect(begin).toHaveBeenCalledTimes(5);
    expect(discard.mock.calls.map(([id]) => id).sort()).toEqual(['att-1', 'att-2', 'att-3', 'att-4', 'att-5']);
    expect(reloadHeld()).toBe(false);
  });

  it('discards the given-back files past the cap rather than dropping them unseen', async () => {
    const sends = heldRelay();
    const { result } = renderHook(() => useFileAttachments());
    act(() => result.current.handleFiles([csv('a.csv'), csv('b.csv'), csv('c.csv')]));
    await flush();
    expect(sends).toHaveLength(3);

    const back = ['x', 'y', 'z'].map((n) => ({ file: csv(`${n}.csv`), type: 'text/csv', attachmentId: `att-${n}` }));
    act(() => result.current.restoreFiles(back));
    expect(result.current.attachedFiles).toHaveLength(MAX_FILES);
    expect(result.current.attachedFiles.slice(3).map((f) => f.upload)).toEqual([
      { status: 'ready', attachmentId: 'att-x' },
      { status: 'ready', attachmentId: 'att-y' },
    ]);
    expect(discard).toHaveBeenCalledExactlyOnceWith('att-z');
  });

  it('takes the ready files for a send without discarding them, and puts back given-back files as ready', async () => {
    const sends = heldRelay();
    const { result } = renderHook(() => useFileAttachments());
    act(() => result.current.handleFiles([csv()]));
    await flush();
    await act(async () => { sends[0].resolve(ready('att-1')); });

    let taken: ReadyAttachment[] = [];
    act(() => { taken = result.current.takeFiles()!; });
    expect(taken).toEqual([{ file: expect.any(File), attachmentId: 'att-1', type: 'text/csv', readyAt: expect.any(Number) }]);
    expect(result.current.attachedFiles).toEqual([]);
    expect(discard).not.toHaveBeenCalled();

    begin.mockClear();
    act(() => result.current.restoreFiles(taken));
    expect(result.current.attachedFiles[0].upload).toEqual({ status: 'ready', attachmentId: 'att-1' });
    expect(begin).not.toHaveBeenCalled();
  });

  it('stages a ready file again before the server lets it go, unseen, and forgets the old copy', async () => {
    vi.useFakeTimers();
    try {
      const sends = heldRelay();
      const { result } = renderHook(() => useFileAttachments());
      act(() => result.current.handleFiles([csv()]));
      await flush();
      await act(async () => { sends[0].resolve(ready('att-1')); });

      await act(() => vi.advanceTimersByTimeAsync(RESTAGE_AFTER_MS - 1000));
      expect(sends).toHaveLength(1);
      await act(() => vi.advanceTimersByTimeAsync(1000));
      await flush();
      expect(sends).toHaveLength(2);
      // Still ready on the copy the server holds, while the new one goes.
      expect(result.current.attachedFiles[0].upload).toEqual({ status: 'ready', attachmentId: 'att-1' });
      expect(result.current.uploadingCount).toBe(0);

      await act(async () => { sends[1].resolve(ready('att-2')); });
      expect(result.current.attachedFiles[0].upload).toEqual({ status: 'ready', attachmentId: 'att-2' });
      expect(discard).toHaveBeenCalledExactlyOnceWith('att-1');
    } finally {
      vi.useRealTimers();
    }
  });

  it('stages nothing again for a file sent, removed, or left with the composer', async () => {
    vi.useFakeTimers();
    try {
      const sends = heldRelay();
      const { result, unmount } = renderHook(() => useFileAttachments());
      act(() => result.current.handleFiles([csv('a.csv'), csv('b.csv'), csv('c.csv')]));
      await flush();
      await act(async () => { sends.forEach((s, i) => s.resolve(ready(`att-${i + 1}`))); });

      act(() => result.current.removeFile(result.current.attachedFiles[0].id));
      await act(() => vi.advanceTimersByTimeAsync(RESTAGE_AFTER_MS - 1000));
      let taken: ReadyAttachment[] | null = null;
      act(() => { taken = result.current.takeFiles(); });
      expect(taken).toHaveLength(2);
      act(() => result.current.handleFiles([csv('d.csv')]));
      await flush();
      await act(async () => { sends[3].resolve(ready('att-4')); });

      await act(() => vi.advanceTimersByTimeAsync(1000));
      unmount();
      await act(() => vi.advanceTimersByTimeAsync(2 * RESTAGE_AFTER_MS));
      expect(sends).toHaveLength(4);
      expect(discard).toHaveBeenCalledExactlyOnceWith('att-1');
    } finally {
      vi.useRealTimers();
    }
  });

  it('uploads again, rather than send, a ready file whose id went stale while the tab slept', async () => {
    const sends = heldRelay();
    const { result } = renderHook(() => useFileAttachments());
    act(() => result.current.handleFiles([csv()]));
    await flush();
    await act(async () => { sends[0].resolve(ready('att-1')); });

    // Asleep past the deadline: the clock moved, the timer has not fired.
    const asleep = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + RESTAGE_AFTER_MS + 5 * 60 * 60 * 1000);
    try {
      let taken: ReadyAttachment[] | null = [];
      act(() => { taken = result.current.takeFiles(); });
      // Nothing taken: the send is held, the file counts as uploading.
      expect(taken).toBeNull();
      expect(result.current.holdsStale()).toBe(false);
      expect(result.current.attachedFiles[0].upload).toEqual({ status: 'uploading', percent: 0 });
      expect(result.current.uploadingCount).toBe(1);
      expect(discard).toHaveBeenCalledWith('att-1');
      await flush();
      expect(sends).toHaveLength(2);

      await act(async () => { sends[1].resolve(ready('att-2')); });
      act(() => { taken = result.current.takeFiles(); });
      expect(taken).toEqual([expect.objectContaining({ attachmentId: 'att-2', readyAt: Date.now() })]);
    } finally {
      asleep.mockRestore();
    }
  });

  it('re-stages a given-back file by when it first became ready, not when it came back', async () => {
    vi.useFakeTimers();
    try {
      const sends = heldRelay();
      const { result } = renderHook(() => useFileAttachments());
      const readyAt = Date.now() - (RESTAGE_AFTER_MS - 1000);
      act(() => result.current.restoreFiles([{ file: csv(), type: 'text/csv', attachmentId: 'att-x', readyAt }]));

      await act(() => vi.advanceTimersByTimeAsync(1000));
      await flush();
      expect(sends).toHaveLength(1);
      expect(result.current.attachedFiles[0].upload).toEqual({ status: 'ready', attachmentId: 'att-x' });
    } finally {
      vi.useRealTimers();
    }
  });

  it('checks a file over 256 MB again once the target is known, and fails it there when that computer cannot take it', async () => {
    const sends = heldRelay();
    const big = csv('big.csv');
    Object.defineProperty(big, 'size', { value: 300 * 1024 * 1024 });
    const { result, rerender } = renderHook(
      ({ workspaceId }: { workspaceId: string | null }) => useFileAttachments({ workspaceId }),
      { initialProps: { workspaceId: null as string | null } },
    );
    act(() => result.current.handleFiles([big, csv()]));
    await flush();
    await act(async () => { sends[0].resolve(ready('att-1')); sends[1].resolve(ready('att-2')); });
    // No target yet: nothing to check against.
    expect(check).not.toHaveBeenCalled();

    check.mockRejectedValueOnce(new ApiError('Request failed with status code 413', 'ERR_BAD_REQUEST', {
      data: { detail: "File is too large for this workspace's computer (300 MB). It takes files up to 256 MB." },
      status: 413, statusText: '', headers: {},
    }));
    rerender({ workspaceId: 'ws-home' });
    await flush();
    // Only the large file is checked; the small one the server copies anywhere.
    expect(check).toHaveBeenCalledTimes(1);
    expect(check).toHaveBeenCalledWith('att-1', 'ws-home');
    expect(result.current.attachedFiles[0].upload).toEqual({
      status: 'failed',
      failure: { kind: 'refused', message: "File is too large for this workspace's computer (300 MB). It takes files up to 256 MB." },
    });
    expect(discard).toHaveBeenCalledWith('att-1');
    expect(result.current.attachedFiles[1].upload).toEqual({ status: 'ready', attachmentId: 'att-2' });
  });

  it('keeps a large file a check found to fit, and checks it again only when the target changes', async () => {
    const sends = heldRelay();
    const big = csv('big.csv');
    Object.defineProperty(big, 'size', { value: 300 * 1024 * 1024 });
    check.mockResolvedValue(ready('att-1'));
    const { result, rerender } = renderHook(
      ({ workspaceId }: { workspaceId: string | null }) => useFileAttachments({ workspaceId }),
      { initialProps: { workspaceId: 'ws-a' as string | null } },
    );
    act(() => result.current.handleFiles([big]));
    await flush();
    await act(async () => { sends[0].resolve(ready('att-1')); });
    // Begun without the ref, so nothing said it fits ws-a yet.
    expect(check).toHaveBeenCalledWith('att-1', 'ws-a');
    rerender({ workspaceId: 'ws-a' });
    await flush();
    expect(check).toHaveBeenCalledTimes(1);
    rerender({ workspaceId: 'ws-b' });
    await flush();
    expect(check).toHaveBeenLastCalledWith('att-1', 'ws-b');
    expect(result.current.attachedFiles[0].upload).toEqual({ status: 'ready', attachmentId: 'att-1' });
    expect(discard).not.toHaveBeenCalled();
  });

  it('takes no files while disabled', () => {
    const { result } = renderHook(() => useFileAttachments({ disabled: true }));
    act(() => result.current.handleFiles([csv()]));
    expect(result.current.attachedFiles).toEqual([]);
    expect(begin).not.toHaveBeenCalled();
  });
});

describe('ChatInput: the send waits for its files', () => {
  function renderInput() {
    const ref = createRef<ChatInputHandle>();
    const onSend = vi.fn();
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } });
    render(
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>
          <ChatInput ref={ref} onSend={onSend} />
        </MemoryRouter>
      </QueryClientProvider>,
    );
    const box = screen.getAllByRole('textbox')[0] as HTMLTextAreaElement;
    return { ref, box, onSend };
  }

  it('holds Send and Enter while a file uploads or has failed, and sends its id once ready', async () => {
    const sends = heldRelay();
    const { ref, box, onSend } = renderInput();
    act(() => ref.current!.addContext({ kind: 'attachment', files: [csv()] }));
    await flush();
    fireEvent.change(box, { target: { value: 'read this' } });

    expect(screen.getByText('Uploading 1 file…')).toBeTruthy();
    expect((screen.getByRole('button', { name: /send/i }) as HTMLButtonElement).disabled).toBe(true);
    fireEvent.keyDown(box, { key: 'Enter' });
    expect(onSend).not.toHaveBeenCalled();

    await act(async () => { sends[0].reject(new ApiError('Network Error', 'ERR_NETWORK')); });
    expect(screen.getByText('Retry or remove 1 file')).toBeTruthy();
    fireEvent.keyDown(box, { key: 'Enter' });
    expect(onSend).not.toHaveBeenCalled();

    fireEvent.click(screen.getByRole('button', { name: 'Retry' }));
    await flush();
    await act(async () => { sends[1].resolve(ready('att-2')); });
    expect(screen.queryByText(/Retry or remove/)).toBeNull();

    fireEvent.keyDown(box, { key: 'Enter' });
    expect(onSend).toHaveBeenCalledTimes(1);
    expect(onSend.mock.calls[0][0].files).toEqual([expect.objectContaining({ attachmentId: 'att-2' })]);
    // The message names it, so it stays on the server.
    expect(discard).not.toHaveBeenCalledWith('att-2');
  });

  it('reports through the handle that files hold the send, and that nothing went', async () => {
    const sends = heldRelay();
    const { ref, box, onSend } = renderInput();
    act(() => ref.current!.addContext({ kind: 'attachment', files: [csv()] }));
    await flush();
    fireEvent.change(box, { target: { value: 'read this' } });

    let sent = true;
    act(() => { sent = ref.current!.send(); });
    expect(sent).toBe(false);
    expect(ref.current!.holdsSend()).toBe(true);
    expect(onSend).not.toHaveBeenCalled();

    await act(async () => { sends[0].resolve(ready('att-1')); });
    expect(ref.current!.holdsSend()).toBe(false);
    act(() => { sent = ref.current!.send(); });
    expect(sent).toBe(true);
    expect(onSend).toHaveBeenCalledTimes(1);
  });

  it('holds the send for a file gone stale while the tab slept, and sends it once uploaded again', async () => {
    const sends = heldRelay();
    const { ref, box, onSend } = renderInput();
    act(() => ref.current!.addContext({ kind: 'attachment', files: [csv()] }));
    await flush();
    await act(async () => { sends[0].resolve(ready('att-1')); });
    fireEvent.change(box, { target: { value: 'read this' } });

    const asleep = vi.spyOn(Date, 'now').mockReturnValue(Date.now() + 25 * 60 * 60 * 1000);
    try {
      expect(ref.current!.holdsSend()).toBe(true);
      let sent = true;
      act(() => { sent = ref.current!.send(); });
      expect(sent).toBe(false);
      expect(ref.current!.holdsSend()).toBe(true);
      expect(onSend).not.toHaveBeenCalled();
      expect(screen.getByText('Uploading 1 file…')).toBeTruthy();

      await flush();
      await act(async () => { sends[1].resolve(ready('att-2')); });
      expect(ref.current!.holdsSend()).toBe(false);
      act(() => { sent = ref.current!.send(); });
      expect(sent).toBe(true);
      expect(onSend.mock.calls[0][0].files).toEqual([expect.objectContaining({ attachmentId: 'att-2' })]);
    } finally {
      asleep.mockRestore();
    }
  });

  it("says why a file failed in the language shown, and follows a switch", async () => {
    const sends = heldRelay();
    const { ref } = renderInput();
    act(() => ref.current!.addContext({ kind: 'attachment', files: [csv()] }));
    await flush();
    await act(async () => { sends[0].reject(new ApiError('Network Error', 'ERR_NETWORK')); });
    expect(screen.getByText('Upload failed')).toBeTruthy();

    try {
      await act(() => i18n.changeLanguage('zh-CN'));
      expect(screen.getByText('上传失败')).toBeTruthy();
      expect(screen.getByRole('button', { name: '重试' })).toBeTruthy();
    } finally {
      await act(() => i18n.changeLanguage('en-US'));
    }
  });
});
