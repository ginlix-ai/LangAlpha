import { act, screen, waitFor } from '@testing-library/react';
import { QueryClient, type UseQueryResult } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import type { ViewerCommentsTarget } from '@/pages/ChatAgent/components/viewers/comments/ViewerComments';
import { LIVE } from '@/pages/ChatAgent/utils/api/readFrom';
import { renderWithProviders } from '@/test/utils';
import type { DriveEntry, DriveVersions } from '@/types/drive';
import { DriveFilePane } from '../components/DriveFilePane';
import { DriveHtml } from '../components/DriveHtml';
import { DriveContext, type DriveContextValue } from '../driveContext';
import type { DriveActions } from '../useDriveActions';
import type { DriveEditor } from '../useDriveEditor';

const api = vi.hoisted(() => ({ readPage: vi.fn(), getEntry: vi.fn(), getHomeWorkspace: vi.fn() }));
vi.mock('@/api/drive', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  readPage: api.readPage,
  getEntry: api.getEntry,
}));
vi.mock('@/pages/ChatAgent/utils/api/workspaces', async (importOriginal) => ({
  ...(await importOriginal<Record<string, unknown>>()),
  getHomeWorkspace: api.getHomeWorkspace,
}));

const layer = vi.hoisted(() => ({ fileSha: undefined as string | null | undefined, key: '' }));
vi.mock('@/pages/ChatAgent/components/viewers/html/comments/lazyCommentLayer', () => ({
  LazyCommentLayer: Object.assign(
    (props: { fileSha?: string | null; view: { key: string } }) => {
      layer.fileSha = props.fileSha;
      layer.key = props.view.key;
      return <div data-testid="comment-layer" />;
    },
    { preload: () => Promise.resolve() },
  ),
}));
// The head's comments, as the hook gives them: made against the entry's hash.
vi.mock('../useDriveFileComments', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../useDriveFileComments')>()),
  useDriveFileComments: ({ entry }: { entry: DriveEntry | undefined }) => ({
    target: entry
      ? ({
        workspaceId: 'home', kind: 'drive', path: entry.id, tabId: 'drive-file', active: true,
        ui: {} as ViewerCommentsTarget['ui'], fileSha: entry.sha256,
      } satisfies ViewerCommentsTarget)
      : null,
    tools: null,
  }),
}));

const A = 'a'.repeat(64);
const B = 'b'.repeat(64);
const report = (sha: string, version: number): DriveEntry => ({
  id: 'e1', path: 'report.html', name: 'report.html', kind: 'file', size: 8, sha256: sha, content_type: 'text/html', is_text: true, version,
  created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-01T00:00:00Z', modified_by: null, deleted_at: null, memo: null, undo_until: null,
});
const body = (content: string, sha: string) => ({
  data: { mode: 'text', mime: 'text/html', content, buffer: null, truncated: false, from: LIVE, version: sha } satisfies FileBody,
  isPending: false, isPlaceholderData: false, error: null, refetch: vi.fn(),
}) as unknown as UseQueryResult<FileBody>;

const viewing = { session: null, dirty: false, theirs: null, behind: false, editorRef: { current: null }, start: vi.fn() } as unknown as DriveEditor;
const noVersions = { data: undefined, isPending: false, error: null } as unknown as UseQueryResult<DriveVersions>;
const ctx = {
  space: 'personal', actions: { pending: {} } as unknown as DriveActions, go: vi.fn(), open: vi.fn(), trash: vi.fn(),
  leaveEditor: vi.fn(), back: vi.fn(), backTo: 'folder' as const, host: 'page' as const, fileCrumbs: false, ask: { rename: vi.fn(), move: vi.fn(), newFolder: vi.fn(), memo: vi.fn() },
} as DriveContextValue;

const pane = (entry: DriveEntry, head: UseQueryResult<FileBody>) => (
  <DriveContext value={ctx}>
    <DriveFilePane
      path={entry.path} entry={entry} entryError={null} onRetry={vi.fn()}
      fileView={{ view: 'document', version: null, against: null }} versions={noVersions}
      mobile={false} editor={viewing} head={head} imageLoader={() => Promise.resolve('')} onOpenLink={vi.fn()}
    />
  </DriveContext>
);

describe('an HTML file saved again while its page is framed with comments', () => {
  it('comments on the bytes the frame shows until the new page swaps in, not on the new head\'s', async () => {
    api.getHomeWorkspace.mockResolvedValue({ workspace_id: 'home' });
    api.getEntry.mockImplementation(() => new Promise(() => {}));
    let arrive: () => void = () => {};
    api.readPage
      .mockResolvedValueOnce('<p>a</p>')
      .mockImplementationOnce(() => new Promise((resolve) => { arrive = () => resolve('<p>b</p>'); }));
    const qc = new QueryClient({ defaultOptions: { queries: { retry: false } } });

    const { rerender } = renderWithProviders(pane(report(A, 1), body('<p>a</p>', A)), { queryClient: qc });
    await screen.findByTestId('comment-layer');
    expect(layer.fileSha).toBe(A);

    // The head's body arrives before its page: the frame still shows A, so a comment made now is on A.
    rerender(pane(report(B, 2), body('<p>b</p>', B)));
    expect(layer.key).toBe(`e1:${A}`);
    expect(layer.fileSha).toBe(A);

    await act(async () => arrive());
    await waitFor(() => expect(layer.key).toBe(`e1:${B}`));
    expect(layer.fileSha).toBe(B);
  });
});

describe('an HTML file whose page could not be read with its bridge', () => {
  const page = { entryId: 'e1', sha: A, seq: null, at: 1 };
  const html = (onUnbridged: (unbridged: boolean) => void) => (
    <DriveContext value={ctx}>
      <DriveHtml content="<p>a</p>" name="report.html" source={false} page={page} comments={null} onUnbridged={onUnbridged} />
    </DriveContext>
  );
  const fresh = () => new QueryClient({ defaultOptions: { queries: { retry: false } } });

  it('says it is framed with no bridge, so nothing offers a comment it cannot take', async () => {
    api.getHomeWorkspace.mockResolvedValue({ workspace_id: 'home' });
    api.readPage.mockReset().mockRejectedValue(new Error('too large'));
    const onUnbridged = vi.fn();
    renderWithProviders(html(onUnbridged), { queryClient: fresh() });
    await waitFor(() => expect(onUnbridged).toHaveBeenLastCalledWith(true));
    expect(screen.queryByTestId('comment-layer')).toBeNull();
  });

  it('says nothing of the kind for a page read with its bridge', async () => {
    api.getHomeWorkspace.mockResolvedValue({ workspace_id: 'home' });
    api.readPage.mockReset().mockResolvedValue('<p>a</p>');
    const onUnbridged = vi.fn();
    renderWithProviders(html(onUnbridged), { queryClient: fresh() });
    await waitFor(() => expect(api.readPage).toHaveBeenCalled());
    await act(async () => {});
    expect(onUnbridged).toHaveBeenCalledWith(false);
    expect(onUnbridged).not.toHaveBeenCalledWith(true);
  });
});
