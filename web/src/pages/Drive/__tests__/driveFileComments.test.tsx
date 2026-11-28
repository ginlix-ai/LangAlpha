import { waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { driveHref, readDriveLocation } from '@/lib/driveLinks';
import { renderHookWithProviders } from '@/test/utils';
import type { ArtifactComment } from '@/types/artifacts';
import type { DriveEntry, DriveVersion } from '@/types/drive';
import { DRIVE_FILE_TAB, askedComment, commentScope, useDriveFileComments, type CommentScope } from '../useDriveFileComments';
import type { DriveCommentAsk } from '@/lib/driveLinks';

const { findArtifact, listArtifactComments } = vi.hoisted(() => ({ findArtifact: vi.fn(), listArtifactComments: vi.fn() }));
vi.mock('@/pages/ChatAgent/utils/api/artifacts', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/pages/ChatAgent/utils/api/artifacts')>()),
  findArtifact,
  listArtifactComments,
}));
vi.mock('@/hooks/useHomeWorkspace', async (importOriginal) => ({
  ...(await importOriginal<typeof import('@/hooks/useHomeWorkspace')>()),
  useHomeWorkspaceId: () => 'home',
}));

const entry = {
  id: 'e1', path: 'notes/a.md', name: 'a.md', kind: 'file', size: 1, sha256: 'a'.repeat(64), version: 3, deleted_at: null,
} as DriveEntry;
const comment = (number: number, unread: number) => ({ comment_id: `c${number}`, number, unread } as ArtifactComment);

beforeEach(() => {
  findArtifact.mockReset().mockResolvedValue({ artifact_id: 'art' });
  listArtifactComments.mockReset();
});

describe('the address asking for a Drive file\'s comments', () => {
  it('reads and writes the list as its own ask, once beside a comment\'s', () => {
    const read = (href: string) => readDriveLocation(new URL(href, 'http://x').searchParams);
    const href = driveHref({ file: 'notes/a.md', ask: 'list' });
    expect(href).toBe('/drive?file=notes%2Fa.md&comments=open');
    expect(read(href)).toMatchObject({ file: 'notes/a.md', ask: 'list' });
    expect(driveHref({ file: 'a.md', ask: { comment: 'c1' } })).toBe('/drive?file=a.md&comment=c1');
    expect(read('/drive?file=a.md&comment=c1').ask).toEqual({ comment: 'c1' });
    // One comment named is the narrower ask.
    expect(read('/drive?file=a.md&comment=c1&comments=open').ask).toEqual({ comment: 'c1' });
    expect(read('/drive?file=a.md&comment=&comments=other').ask).toBeNull();
  });

  it('opens the list at the first unread answer by pin number, or at none', () => {
    const comments = [comment(4, 1), comment(2, 0), comment(3, 2)];
    expect(askedComment('list', comments)).toBe('c3');
    expect(askedComment('list', [comment(1, 0)])).toBeNull();
    expect(askedComment({ comment: 'c9' }, comments)).toBe('c9');
    // A past version's list opens at its own first unread answer.
    expect(askedComment('list', comments, (c) => c.number === 4)).toBe('c4');
  });

  it('opens the comment list once the comments are read, at the first unread answer, then lets the address go', async () => {
    listArtifactComments.mockResolvedValue([comment(1, 0), comment(2, 1)]);
    const onAsked = vi.fn();
    let ask: DriveCommentAsk | null = 'list';
    const { result, rerender } = renderHookWithProviders(() => useDriveFileComments({
      entry, scope: { kind: 'head', sha: null }, versions: undefined, ask, onAsked, onSeeVersion: vi.fn(),
    }));
    await waitFor(() => expect(onAsked).toHaveBeenCalledTimes(1));
    await waitFor(() => expect(result.current.target!.ui.listTab).toBe(DRIVE_FILE_TAB));
    const ui = result.current.target!.ui;
    expect(ui.focus).toMatchObject({ kind: 'drive', path: 'e1', commentId: 'c2' });
    // The address drops the ask; nothing opens again.
    ask = null;
    rerender();
    expect(onAsked).toHaveBeenCalledTimes(1);
  });
});

describe('which of a Drive file\'s comments its pane shows', () => {
  const v2 = { seq: 2, sha256: 'b'.repeat(64), created_at: '2026-11-03T00:00:00Z' } as DriveVersion;
  const base = { space: 'personal', entry, view: 'document' as const, shown: { docVersion: null, viewed: null, sha: 'h' }, editing: false };

  it('the current file\'s, outside the editor and a memo\'s other views', () => {
    expect(commentScope(base)).toEqual({ kind: 'head', sha: 'h' });
    expect(commentScope({ ...base, editing: true })).toBeNull();
    expect(commentScope({ ...base, view: 'changes' })).toBeNull();
    expect(commentScope({ ...base, view: 'who' })).toBeNull();
    expect(commentScope({ ...base, entry: { ...entry, deleted_at: '2026-11-01T00:00:00Z' } })).toBeNull();
    expect(commentScope({ ...base, space: 'team' })).toBeNull();
  });

  it('a past version\'s, read only, once its versions are read', () => {
    expect(commentScope({ ...base, shown: { docVersion: 2, viewed: v2, sha: v2.sha256 } })).toEqual({ kind: 'version', seq: 2, sha: v2.sha256 });
    expect(commentScope({ ...base, shown: { docVersion: 2, viewed: null, sha: null } })).toBeNull();
  });

  it('a past version\'s target is read only and shows only its own comments; the head\'s links an Outdated one back', () => {
    const versions = [
      { seq: 3, sha256: 'c'.repeat(64), created_at: '2026-11-05T00:00:00Z' },
      { seq: 2, sha256: 'b'.repeat(64), created_at: '2026-11-03T00:00:00Z' },
    ] as DriveVersion[];
    const onV2 = { comment_id: 'x', file_sha256: 'b'.repeat(64), created_at: '2026-11-04T00:00:00Z' } as ArtifactComment;
    const onV3 = { comment_id: 'y', file_sha256: 'c'.repeat(64), created_at: '2026-11-06T00:00:00Z' } as ArtifactComment;
    const onSeeVersion = vi.fn();
    const hook = (scope: CommentScope) => renderHookWithProviders(() => useDriveFileComments({
      entry, scope, versions, ask: null, onAsked: vi.fn(), onSeeVersion,
    })).result.current;

    const past = hook({ kind: 'version', seq: 2, sha: 'b'.repeat(64) });
    expect(past.surface.readOnly).toBe(true);
    expect(past.target!.fileSha).toBe('b'.repeat(64));
    expect([onV2, onV3].filter(past.surface.shows!)).toEqual([onV2]);

    const current = hook({ kind: 'head', sha: 'c'.repeat(64) });
    expect(current.target!.fileSha).toBe('c'.repeat(64));
    const head = current.surface;
    expect(head.readOnly).toBeFalsy();
    expect(head.shows).toBeUndefined();
    const link = head.outdatedLink!(onV2)!;
    expect(link.label).toBe('See it on v2');
    link.open();
    expect(onSeeVersion).toHaveBeenCalledWith(2, 'x');
    // Made on the head itself: nothing older to show it on.
    expect(head.outdatedLink!(onV3)).toBeNull();
  });
});
