import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { screen } from '@testing-library/react';
import { QueryClient, type UseQueryResult } from '@tanstack/react-query';
import { describe, expect, it, vi } from 'vitest';
import type { FileBody } from '@/pages/ChatAgent/components/filePanel/fileBody';
import { LIVE } from '@/pages/ChatAgent/utils/api/readFrom';
import { renderWithProviders } from '@/test/utils';
import type { DriveEntry, DriveVersions } from '@/types/drive';
import { DriveFilePane } from '../components/DriveFilePane';
import { DriveContext, type DriveContextValue } from '../driveContext';
import type { DriveActions } from '../useDriveActions';
import type { DriveEditor } from '../useDriveEditor';

vi.mock('@/pages/ChatAgent/components/filePanel/FileViewer', () => ({
  FileViewer: () => <div data-testid="viewer" />,
}));
// The file's comment controls stand in as a mark: what is pinned here is
// whether the header shows them, which the body decides.
vi.mock('../useDriveFileComments', async (importOriginal) => ({
  ...(await importOriginal<typeof import('../useDriveFileComments')>()),
  useDriveFileComments: () => ({ target: null, tools: <span data-testid="comment-tools" /> }),
}));

const SHA = 'a'.repeat(64);
const file = (name: string, over: Partial<DriveEntry> = {}): DriveEntry => ({
  id: 'e1', path: name, name, kind: 'file', size: 8, sha256: SHA, content_type: null, is_text: true, version: 1,
  created_at: '2026-11-01T00:00:00Z', modified_at: '2026-11-01T00:00:00Z', modified_by: null, deleted_at: null, memo: null, undo_until: null,
  ...over,
});

const viewing = { session: null, dirty: false, theirs: null, behind: false, editorRef: { current: null }, start: vi.fn() } as unknown as DriveEditor;
const noVersions = { data: undefined, isPending: false, error: null } as unknown as UseQueryResult<DriveVersions>;

function head(data: FileBody | undefined, isPending = false): UseQueryResult<FileBody> {
  return { data, isPending, error: null, refetch: vi.fn() } as unknown as UseQueryResult<FileBody>;
}

function renderPane(entry: DriveEntry, body: UseQueryResult<FileBody>) {
  const ctx = {
    space: 'personal', actions: { pending: {} } as unknown as DriveActions, go: vi.fn(), open: vi.fn(), trash: vi.fn(),
    leaveEditor: vi.fn(), back: vi.fn(), backTo: 'folder' as const, host: 'page' as const, fileCrumbs: false, ask: { rename: vi.fn(), move: vi.fn(), newFolder: vi.fn(), memo: vi.fn() },
  } as DriveContextValue;
  renderWithProviders(
    <DriveContext value={ctx}>
      <DriveFilePane
        path={entry.path}
        entry={entry}
        entryError={null}
        onRetry={vi.fn()}
        fileView={{ view: 'document', version: null, against: null }}
        versions={noVersions}
        mobile={false}
        editor={viewing}
        head={body}
        imageLoader={() => Promise.resolve('')}
        onOpenLink={vi.fn()}
      />
    </DriveContext>,
    { queryClient: new QueryClient({ defaultOptions: { queries: { retry: false } } }) },
  );
}

describe('the comment tools in a Drive file\'s header', () => {
  it('show over a document the viewer draws', () => {
    renderPane(file('q3.md'), head({ mode: 'text', mime: 'text/markdown', content: '# Q3', buffer: null, truncated: false, from: LIVE }));
    expect(screen.getByTestId('comment-tools')).toBeInTheDocument();
  });

  it('stay out beside "Cannot preview this file"', () => {
    renderPane(file('holdings.bin', { is_text: false, content_type: 'application/octet-stream' }), head(undefined));
    expect(screen.getByTestId('viewer')).toBeInTheDocument();
    expect(screen.queryByTestId('comment-tools')).toBeNull();
  });

  it('stay out while the body is first read', () => {
    renderPane(file('q3.md'), head(undefined, true));
    expect(screen.queryByTestId('comment-tools')).toBeNull();
  });
});

describe('the header on a narrow pane', () => {
  it('lets the actions wrap under the name, so the file name keeps its room beside the comment tools', () => {
    const css = readFileSync(resolve(__dirname, '../Drive.css'), 'utf8');
    const narrow = css.slice(css.indexOf('@container drive-file-top'));
    expect(narrow).toMatch(/\.drive-file-head \{ flex-wrap: wrap;/);
    expect(narrow).toMatch(/\.drive-file-title \{ flex: 1 1 10rem; \}/);
    expect(narrow).toMatch(/\.drive-file-actions \{ margin-left: auto; \}/);
    expect(css).toMatch(/container: drive-file-top \/ inline-size;/);
  });
});
