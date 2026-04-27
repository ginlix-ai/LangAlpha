import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// These tests exercise the cache-refetch race + body-link rewrite + unsaved-
// edit confirmation behaviour. We stub the data hooks directly (rather than
// the api layer) so we can deterministically control `isLoading` /
// `isFetching` transitions without spinning a real react-query lifecycle.
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => {
      // Mirror the i18next defaultValue fallback for any keys still using it.
      if (opts && typeof opts === 'object' && 'defaultValue' in opts) {
        return String((opts as { defaultValue: unknown }).defaultValue);
      }
      if (opts && typeof opts === 'object') {
        let out = key;
        for (const [k, v] of Object.entries(opts)) {
          out = out.replace(new RegExp(`{{\\s*${k}\\s*}}`, 'g'), String(v));
        }
        return out;
      }
      return key;
    },
  }),
}));

vi.mock('../Markdown', () => ({
  default: ({
    content,
    onOpenFile,
  }: {
    content: string;
    onOpenFile?: (href: string, wsId?: string) => void;
  }) => (
    <div data-testid="markdown-content">
      {content}
      {/* Expose the link-router so tests can assert how hrefs are rewritten. */}
      <button
        type="button"
        data-testid="md-link-md"
        onClick={() => onOpenFile?.('siblings.md')}
      >
        siblings.md
      </button>
      <button
        type="button"
        data-testid="md-link-md-dotslash"
        onClick={() => onOpenFile?.('./other.md')}
      >
        ./other.md
      </button>
      <button
        type="button"
        data-testid="md-link-pdf"
        onClick={() => onOpenFile?.('reports/q1.pdf')}
      >
        reports/q1.pdf
      </button>
      <button
        type="button"
        data-testid="md-link-bare-pdf"
        onClick={() => onOpenFile?.('attached.pdf')}
      >
        attached.pdf
      </button>
    </div>
  ),
}));

vi.mock('@/components/ui/dialog', () => ({
  Dialog: ({ children, open }: { children: React.ReactNode; open: boolean }) =>
    open ? <div data-testid="dialog">{children}</div> : null,
  DialogContent: ({ children }: { children: React.ReactNode }) => (
    <div data-testid="dialog-content">{children}</div>
  ),
  DialogTitle: ({ children }: { children: React.ReactNode }) => <h2>{children}</h2>,
  DialogDescription: ({ children }: { children: React.ReactNode }) => (
    <div>{children}</div>
  ),
}));

vi.mock('@/components/ui/hover-card', () => ({
  HoverCard: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  HoverCardTrigger: ({ children }: { children: React.ReactNode }) => <>{children}</>,
  HoverCardContent: ({ children }: { children: React.ReactNode }) => <div>{children}</div>,
}));

vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: [] } }),
}));

// Stub the api module wholesale — the hooks below are mocked but the panel
// still imports a couple of helpers (download, deleteUserMemo for bulk).
vi.mock('@/pages/ChatAgent/utils/api', () => ({
  listUserMemos: vi.fn(),
  readUserMemo: vi.fn(),
  uploadUserMemo: vi.fn(),
  writeUserMemo: vi.fn(),
  deleteUserMemo: vi.fn(),
  regenerateUserMemo: vi.fn(),
  triggerUserMemoDownload: vi.fn(),
  downloadUserMemoBlobUrl: vi.fn(),
}));

// ---------------------------------------------------------------------------
// Hook stubs — we drive the list/read state directly so each test can
// control the loading / refetching / data transitions without timing.
// ---------------------------------------------------------------------------

interface ListState {
  data: { entries: Array<{ key: string; original_filename: string | null; mime_type: string | null; size_bytes: number; description: string | null; metadata_status: 'ready' | 'pending' | 'failed' | null; created_at: string | null; modified_at: string | null; source_kind: string | null; source_workspace_id: string | null; source_path: string | null; sha256: string | null }>; truncated: boolean } | undefined;
  isLoading: boolean;
  isFetching: boolean;
  error: unknown;
  refetch: () => void;
}

interface ReadState {
  data:
    | {
        key: string;
        content: string;
        encoding: string;
        mime_type: string | null;
        size_bytes: number;
        original_filename: string | null;
        description: string | null;
        summary: string | null;
        metadata_status: 'ready' | 'pending' | 'failed' | null;
        metadata_error: string | null;
        created_at: string | null;
        modified_at: string | null;
        source_kind: string | null;
        source_workspace_id: string | null;
        source_path: string | null;
      }
    | undefined;
  isLoading: boolean;
  error: unknown;
}

let listState: ListState;
let readState: ReadState;

vi.mock('../../hooks/useMemo', () => ({
  useUserMemoList: () => listState,
  useReadUserMemo: () => readState,
  useUploadUserMemo: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useDeleteUserMemo: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useWriteUserMemo: () => ({ mutateAsync: vi.fn(), isPending: false }),
  useRegenerateUserMemo: () => ({ mutateAsync: vi.fn(), isPending: false }),
}));

import MemoPanel from '../MemoPanel';

const baseEntry = {
  key: 'note.md',
  original_filename: 'note.md',
  mime_type: 'text/markdown',
  size_bytes: 12,
  description: null,
  metadata_status: 'ready' as const,
  created_at: '2026-01-01T00:00:00Z',
  modified_at: null,
  source_kind: null,
  source_workspace_id: null,
  source_path: null,
  sha256: null,
};

beforeEach(() => {
  vi.clearAllMocks();
  listState = {
    data: { entries: [], truncated: false },
    isLoading: false,
    isFetching: false,
    error: null,
    refetch: vi.fn(),
  };
  readState = {
    data: undefined,
    isLoading: false,
    error: null,
  };
});

// ---------------------------------------------------------------------------
// Fix #5 — race: stale list + targetKey arrives while refetch is in flight
// ---------------------------------------------------------------------------

describe('MemoPanel — cache-refetch race (Fix #5)', () => {
  it('does not flash the not-found banner while a background refetch is in flight, and opens the entry once it lands', async () => {
    // Stale list (no target) + refetch in flight.
    listState = {
      data: { entries: [], truncated: false },
      isLoading: false,
      isFetching: true,
      error: null,
      refetch: vi.fn(),
    };

    const { rerender } = renderWithProviders(
      <MemoPanel targetKey="fresh-memo.md" onTargetHandled={() => {}} />,
    );

    // Banner must NOT appear while we're still fetching the fresh list.
    await waitFor(() => {
      expect(
        screen.queryByText('memoPanel.notFound'),
      ).not.toBeInTheDocument();
    });

    // Refetch lands — fresh list now contains the target.
    listState = {
      data: {
        entries: [
          { ...baseEntry, key: 'fresh-memo.md', original_filename: 'fresh-memo.md' },
        ],
        truncated: false,
      },
      isLoading: false,
      isFetching: false,
      error: null,
      refetch: vi.fn(),
    };
    readState = {
      data: {
        key: 'fresh-memo.md',
        content: '# fresh',
        encoding: 'utf-8',
        mime_type: 'text/markdown',
        size_bytes: 7,
        original_filename: 'fresh-memo.md',
        description: null,
        summary: null,
        metadata_status: 'ready',
        metadata_error: null,
        created_at: null,
        modified_at: null,
        source_kind: null,
        source_workspace_id: null,
        source_path: null,
      },
      isLoading: false,
      error: null,
    };

    rerender(<MemoPanel targetKey="fresh-memo.md" onTargetHandled={() => {}} />);

    // Viewer mounted with the fresh content; no not-found banner ever rendered.
    await waitFor(() =>
      expect(screen.getByTestId('markdown-content')).toBeInTheDocument(),
    );
    expect(
      screen.queryByText(/memoPanel\.notFound/),
    ).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Fix #6 — body link rewrite heuristic
// ---------------------------------------------------------------------------

describe('MemoPanel — body link routing (Fix #6)', () => {
  it('rewrites bare slug hrefs into the memo dir, but passes hrefs containing a "/" through unchanged', async () => {
    const onOpenFile = vi.fn();

    listState = {
      data: { entries: [{ ...baseEntry }], truncated: false },
      isLoading: false,
      isFetching: false,
      error: null,
      refetch: vi.fn(),
    };
    readState = {
      data: {
        key: 'note.md',
        content: '# n',
        encoding: 'utf-8',
        mime_type: 'text/markdown',
        size_bytes: 3,
        original_filename: 'note.md',
        description: null,
        summary: null,
        metadata_status: 'ready',
        metadata_error: null,
        created_at: null,
        modified_at: null,
        source_kind: null,
        source_workspace_id: null,
        source_path: null,
      },
      isLoading: false,
      error: null,
    };

    renderWithProviders(<MemoPanel onOpenFile={onOpenFile} />);
    const user = userEvent.setup();

    await user.click(await screen.findByText('note.md'));
    await screen.findByTestId('markdown-content');

    // Bare slug `siblings.md` → resolves against the memo dir.
    await user.click(screen.getByTestId('md-link-md'));
    expect(onOpenFile).toHaveBeenLastCalledWith(
      '.agents/user/memo/siblings.md',
      undefined,
    );

    // `./other.md` → leading `./` stripped, resolved against memo dir.
    await user.click(screen.getByTestId('md-link-md-dotslash'));
    expect(onOpenFile).toHaveBeenLastCalledWith(
      '.agents/user/memo/other.md',
      undefined,
    );

    // `reports/q1.pdf` → has a subdir → passes through verbatim.
    await user.click(screen.getByTestId('md-link-pdf'));
    expect(onOpenFile).toHaveBeenLastCalledWith('reports/q1.pdf', undefined);

    // Bare `attached.pdf` (no subdir) → still resolved against memo dir
    // because PDFs are valid memo entries.
    await user.click(screen.getByTestId('md-link-bare-pdf'));
    expect(onOpenFile).toHaveBeenLastCalledWith(
      '.agents/user/memo/attached.pdf',
      undefined,
    );
  });
});

// ---------------------------------------------------------------------------
// Fix #8 — unsaved edit + empty-string targetKey sentinel
// ---------------------------------------------------------------------------

describe('MemoPanel — unsaved edit guard on empty targetKey (Fix #8)', () => {
  it('prompts before discarding when the user cancels, and keeps the editor mounted', async () => {
    listState = {
      data: { entries: [{ ...baseEntry }], truncated: false },
      isLoading: false,
      isFetching: false,
      error: null,
      refetch: vi.fn(),
    };
    readState = {
      data: {
        key: 'note.md',
        content: 'old content',
        encoding: 'utf-8',
        mime_type: 'text/markdown',
        size_bytes: 11,
        original_filename: 'note.md',
        description: null,
        summary: null,
        metadata_status: 'ready',
        metadata_error: null,
        created_at: null,
        modified_at: null,
        source_kind: null,
        source_workspace_id: null,
        source_path: null,
      },
      isLoading: false,
      error: null,
    };

    const onTargetHandled = vi.fn();

    const { rerender } = renderWithProviders(
      <MemoPanel onTargetHandled={onTargetHandled} />,
    );
    const user = userEvent.setup();

    // Open the entry, enter edit mode, dirty the buffer.
    await user.click(await screen.findByText('note.md'));
    await screen.findByTestId('markdown-content');
    await user.click(screen.getByTitle('memoPanel.actions.edit'));
    const textarea = await screen.findByDisplayValue('old content');
    await user.type(textarea, ' + my unsaved work');

    // Empty-string sentinel arrives (e.g. agent wrote memo.md index).
    // The non-blocking discard-confirm dialog should open instead of the
    // synchronous window.confirm() that would freeze the SSE event loop.
    rerender(
      <MemoPanel targetKey="" onTargetHandled={onTargetHandled} />,
    );

    // Dialog title appears (translation mock returns the key verbatim).
    const cancelBtn = await screen.findByRole('button', { name: 'common.cancel' });
    await user.click(cancelBtn);

    // Editor still mounted with the dirty buffer intact.
    expect(
      screen.getByDisplayValue('old content + my unsaved work'),
    ).toBeInTheDocument();

    // Sentinel was acked so we don't loop on every render.
    expect(onTargetHandled).toHaveBeenCalled();
  });

  it('clears the editor when the user confirms the discard', async () => {
    listState = {
      data: { entries: [{ ...baseEntry }], truncated: false },
      isLoading: false,
      isFetching: false,
      error: null,
      refetch: vi.fn(),
    };
    readState = {
      data: {
        key: 'note.md',
        content: 'old content',
        encoding: 'utf-8',
        mime_type: 'text/markdown',
        size_bytes: 11,
        original_filename: 'note.md',
        description: null,
        summary: null,
        metadata_status: 'ready',
        metadata_error: null,
        created_at: null,
        modified_at: null,
        source_kind: null,
        source_workspace_id: null,
        source_path: null,
      },
      isLoading: false,
      error: null,
    };

    const { rerender } = renderWithProviders(
      <MemoPanel onTargetHandled={() => {}} />,
    );
    const user = userEvent.setup();

    await user.click(await screen.findByText('note.md'));
    await screen.findByTestId('markdown-content');
    await user.click(screen.getByTitle('memoPanel.actions.edit'));
    const textarea = await screen.findByDisplayValue('old content');
    await user.type(textarea, ' dirty');

    rerender(<MemoPanel targetKey="" onTargetHandled={() => {}} />);

    // The discard-confirm dialog opens; click the destructive Discard button.
    const discardBtn = await screen.findByRole('button', { name: 'memoPanel.discardEdits' });
    await user.click(discardBtn);

    // Back on the list view (no editor mounted).
    await waitFor(() =>
      expect(
        screen.queryByDisplayValue('old content dirty'),
      ).not.toBeInTheDocument(),
    );
  });
});

// ---------------------------------------------------------------------------
// Fix #9 — banner clears on equal-length membership change
// ---------------------------------------------------------------------------

describe('MemoPanel — not-found banner clears on key-set change (Fix #9)', () => {
  it('clears the banner when the missing key replaces another entry (length unchanged)', async () => {
    // Initial: list has [a.md], target b.md → banner.
    listState = {
      data: {
        entries: [
          { ...baseEntry, key: 'a.md', original_filename: 'a.md' },
        ],
        truncated: false,
      },
      isLoading: false,
      isFetching: false,
      error: null,
      refetch: vi.fn(),
    };

    const { rerender } = renderWithProviders(
      <MemoPanel targetKey="b.md" onTargetHandled={() => {}} />,
    );

    await waitFor(() =>
      expect(screen.getByText(/memoPanel\.notFound/)).toBeInTheDocument(),
    );

    // Same length, different membership: drop a.md, add b.md.
    await act(async () => {
      listState = {
        data: {
          entries: [
            { ...baseEntry, key: 'b.md', original_filename: 'b.md' },
          ],
          truncated: false,
        },
        isLoading: false,
        isFetching: false,
        error: null,
        refetch: vi.fn(),
      };
      rerender(<MemoPanel targetKey={null} onTargetHandled={() => {}} />);
    });

    await waitFor(() =>
      expect(
        screen.queryByText(/memoPanel\.notFound/),
      ).not.toBeInTheDocument(),
    );
  });
});
