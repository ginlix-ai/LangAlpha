import { describe, expect, it, vi } from 'vitest';
import { ApiError } from '@/api/client';

const mockFind = vi.hoisted(() => vi.fn());
vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/api/drive')>()),
  getEntryById: mockFind,
}));

import { act } from '@testing-library/react';
import { renderHookWithProviders } from '@/test/utils';
import type { ArtifactKind } from '@/types/api';
import { commentRef, type ArtifactCommentOpeners } from '../artifactCommentReply';
import { driveCommentOpen, useCommentOpener, useDriveCommentOpener } from '../driveCommentOpener';

const ENTRY = '0b9e1c52-6f0a-4c7e-9d1e-3a2b5c4d6e7f';

/** A Drive route's refusal as the client rejects it. */
function refused(status: number, type: string): ApiError {
  return new ApiError(`Request failed with status code ${status}`, 'ERR_BAD_REQUEST', {
    status,
    statusText: '',
    data: { detail: { type, message: type } },
    headers: {},
  });
}

describe('opening a Drive comment from a chat', () => {
  it('finds the file by its entry id, so a renamed file still opens, at the comment', async () => {
    const find = vi.fn().mockResolvedValue({ path: 'Reports/renamed.md' });
    const open = await driveCommentOpen({ path: 'old.md', commentId: 'c-1', entryId: ENTRY }, find);
    expect(find).toHaveBeenCalledWith('personal', ENTRY);
    expect(open).toEqual({ loc: { file: 'Reports/renamed.md', ask: { comment: 'c-1' } } });
  });

  it('opens a reply, which names only the entry id, where the file is now', async () => {
    const find = vi.fn().mockResolvedValue({ path: 'a.pdf' });
    expect(await driveCommentOpen({ path: null, commentId: 'c-2', entryId: ENTRY }, find)).toEqual({ loc: { file: 'a.pdf', ask: { comment: 'c-2' } } });
    expect(find).toHaveBeenCalledWith('personal', ENTRY);
  });

  it('opens by the path alone where a payload names no entry id', async () => {
    const find = vi.fn();
    expect(await driveCommentOpen({ path: 'notes/a.md', commentId: 'c-2', entryId: null }, find))
      .toEqual({ loc: { file: 'notes/a.md', ask: { comment: 'c-2' } } });
    expect(find).not.toHaveBeenCalled();
  });

  it('says the file is in the trash, rather than opening nothing', async () => {
    const find = vi.fn().mockRejectedValue(refused(404, 'in_trash'));
    expect(await driveCommentOpen({ path: null, commentId: 'c-3', entryId: ENTRY }, find)).toEqual({ problem: 'trash' });
    expect(await driveCommentOpen({ path: 'notes/a.md', commentId: 'c-3', entryId: ENTRY }, find)).toEqual({ problem: 'trash' });
  });

  it('says a file no longer in the Drive is gone, and never opens its old path, which may hold another file', async () => {
    const find = vi.fn().mockRejectedValue(refused(404, 'not_found'));
    expect(await driveCommentOpen({ path: null, commentId: 'c-4', entryId: ENTRY }, find)).toEqual({ problem: 'gone' });
    expect(await driveCommentOpen({ path: 'notes/a.md', commentId: 'c-4', entryId: ENTRY }, find)).toEqual({ problem: 'gone' });
  });

  it('opens the path the line names where the lookup itself failed, and says so where there is none', async () => {
    const find = vi.fn().mockRejectedValue(new TypeError('Failed to fetch'));
    expect(await driveCommentOpen({ path: 'notes/a.md', commentId: 'c-5', entryId: ENTRY }, find))
      .toEqual({ loc: { file: 'notes/a.md', ask: { comment: 'c-5' } } });
    expect(await driveCommentOpen({ path: null, commentId: 'c-5', entryId: ENTRY }, find)).toEqual({ problem: 'failed' });
  });
});

describe('a listed Drive comment as a ref', () => {
  it('keeps where the file is and the entry id that finds it', () => {
    expect(commentRef({ artifact_id: 'a', workspace_id: 'home', comment_id: 'c', kind: 'drive', path: 'notes/a.md', entry_id: ENTRY }))
      .toEqual({ artifactId: 'a', workspaceId: 'home', commentId: 'c', kind: 'drive', entryId: ENTRY, path: 'notes/a.md' });
  });
});

describe('which comments a view can open', () => {
  const opener = (openers: ArtifactCommentOpeners | undefined, kind: ArtifactKind) =>
    renderHookWithProviders(() => useCommentOpener(openers, kind)).result.current;

  it("opens a Drive file's on the Drive page from any view that opens comments, and leaves the others to the view", () => {
    const file = vi.fn();
    expect(opener({ file }, 'drive')).toBeTypeOf('function');
    expect(opener({ file }, 'file')).toBe(file);
    expect(opener({ file }, 'chart')).toBeUndefined();
  });

  it('opens nothing in a view that opens no comments, a shared chat', () => {
    expect(opener(undefined, 'drive')).toBeUndefined();
  });

  it("leaves a Drive file's to a view that names a place for it, a chat's Drive tab", () => {
    const drive = vi.fn();
    expect(opener({ drive }, 'drive')).toBe(drive);
  });

  it('hands the place to the view that shows the Drive itself, at the comment', async () => {
    const place = vi.fn();
    const { result } = renderHookWithProviders(() => useDriveCommentOpener(place));
    await act(async () => {
      result.current({ artifactId: 'a', workspaceId: 'home', commentId: 'c-9', kind: 'drive', entryId: null, path: 'notes/a.md' });
      await Promise.resolve();
    });
    expect(place).toHaveBeenCalledWith({ file: 'notes/a.md', ask: { comment: 'c-9' } });
  });

  it('opens the comment clicked last when an earlier click is looked up more slowly', async () => {
    const place = vi.fn();
    let answerFirst: (found: { path: string }) => void = () => {};
    mockFind
      .mockImplementationOnce(() => new Promise((resolve) => { answerFirst = resolve; }))
      .mockResolvedValueOnce({ path: 'b.md' });
    const { result } = renderHookWithProviders(() => useDriveCommentOpener(place));
    const ref = (commentId: string) => ({ artifactId: 'a', workspaceId: 'home', commentId, kind: 'drive' as const, entryId: ENTRY, path: null });
    await act(async () => {
      result.current(ref('c-a'));
      result.current(ref('c-b'));
      await Promise.resolve();
    });
    await act(async () => {
      answerFirst({ path: 'a.md' });
      await Promise.resolve();
    });
    expect(place).toHaveBeenCalledTimes(1);
    expect(place).toHaveBeenCalledWith({ file: 'b.md', ask: { comment: 'c-b' } });
  });

  it('opens the comment clicked last across two rows, each with its own opener', async () => {
    const placeA = vi.fn();
    const placeB = vi.fn();
    let answerA: (found: { path: string }) => void = () => {};
    let answerB: (found: { path: string }) => void = () => {};
    mockFind
      .mockImplementationOnce(() => new Promise((resolve) => { answerA = resolve; }))
      .mockImplementationOnce(() => new Promise((resolve) => { answerB = resolve; }));
    const rowA = renderHookWithProviders(() => useDriveCommentOpener(placeA));
    const rowB = renderHookWithProviders(() => useDriveCommentOpener(placeB));
    const ref = (commentId: string) => ({ artifactId: 'a', workspaceId: 'home', commentId, kind: 'drive' as const, entryId: ENTRY, path: null });
    await act(async () => {
      rowA.result.current(ref('c-a'));
      rowB.result.current(ref('c-b'));
      await Promise.resolve();
    });
    // The earlier click's lookup answers first, with the later one still out.
    await act(async () => {
      answerA({ path: 'a.md' });
      await Promise.resolve();
    });
    expect(placeA).not.toHaveBeenCalled();
    await act(async () => {
      answerB({ path: 'b.md' });
      await Promise.resolve();
    });
    expect(placeB).toHaveBeenCalledWith({ file: 'b.md', ask: { comment: 'c-b' } });
  });

  it('opens nothing once its view is gone', async () => {
    const place = vi.fn();
    let answer: (found: { path: string }) => void = () => {};
    mockFind.mockImplementationOnce(() => new Promise((resolve) => { answer = resolve; }));
    const { result, unmount } = renderHookWithProviders(() => useDriveCommentOpener(place));
    await act(async () => {
      result.current({ artifactId: 'a', workspaceId: 'home', commentId: 'c-g', kind: 'drive', entryId: ENTRY, path: null });
      await Promise.resolve();
    });
    unmount();
    await act(async () => {
      answer({ path: 'a.md' });
      await Promise.resolve();
    });
    expect(place).not.toHaveBeenCalled();
  });
});
