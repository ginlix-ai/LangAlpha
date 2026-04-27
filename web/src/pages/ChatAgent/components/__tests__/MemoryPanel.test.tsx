import { describe, it, expect, vi, beforeEach } from 'vitest';
import { screen, waitFor, act } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// MemoryPanel tests for fixes #5, #6, #9. We stub the memory hooks directly
// so each test deterministically controls loading / refetching transitions.
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({
    t: (key: string, opts?: Record<string, unknown>) => {
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
      <button
        type="button"
        data-testid="md-link-md"
        onClick={() => onOpenFile?.('feedback_visualization_preference.md')}
      >
        feedback.md
      </button>
      <button
        type="button"
        data-testid="md-link-md-dotslash"
        onClick={() => onOpenFile?.('./other-note.md')}
      >
        ./other-note.md
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
      <button
        type="button"
        data-testid="md-link-qualified"
        onClick={() => onOpenFile?.('.agents/skills/foo/skill.md')}
      >
        skill ref
      </button>
    </div>
  ),
}));

// ---------------------------------------------------------------------------
// Hook stubs
// ---------------------------------------------------------------------------

interface ListState {
  entries: Array<{
    key: string;
    size: number;
    modified_at: string | null;
  }>;
  loading: boolean;
  isFetching: boolean;
  error: string | null;
  refresh: () => void;
}

interface ReadState {
  data: { content: string; key: string; encoding?: string; size?: number } | undefined;
  loading: boolean;
  error: string | null;
}

let userListState: ListState;
let wsListState: ListState;
let userReadState: ReadState;
let wsReadState: ReadState;

vi.mock('../../hooks/useMemory', () => ({
  useUserMemory: () => userListState,
  useWorkspaceMemory: () => wsListState,
  useReadUserMemory: () => userReadState,
  useReadWorkspaceMemory: () => wsReadState,
}));

import MemoryPanel from '../MemoryPanel';

const baseEntry = (key: string) => ({
  key,
  size: 100,
  modified_at: '2026-01-01T00:00:00Z',
});

beforeEach(() => {
  vi.clearAllMocks();
  userListState = {
    entries: [],
    loading: false,
    isFetching: false,
    error: null,
    refresh: vi.fn(),
  };
  wsListState = {
    entries: [],
    loading: false,
    isFetching: false,
    error: null,
    refresh: vi.fn(),
  };
  userReadState = { data: undefined, loading: false, error: null };
  wsReadState = { data: undefined, loading: false, error: null };
});

// ---------------------------------------------------------------------------
// Fix #5 — race
// ---------------------------------------------------------------------------

describe('MemoryPanel — cache-refetch race (Fix #5)', () => {
  it('does not flash not-found while a background refetch is in flight; opens entry once it lands', async () => {
    // Stale list with no target, refetch in flight.
    userListState = {
      entries: [],
      loading: false,
      isFetching: true,
      error: null,
      refresh: vi.fn(),
    };

    const { rerender } = renderWithProviders(
      <MemoryPanel
        workspaceId={null}
        targetKey="just-written.md"
        targetTier="user"
      />,
    );

    // No banner while refetching — give react a tick to settle.
    await waitFor(() => {
      expect(
        screen.queryByText(/memoryPanel\.notFound/),
      ).not.toBeInTheDocument();
    });

    // Refresh lands with the target entry.
    userListState = {
      entries: [baseEntry('just-written.md')],
      loading: false,
      isFetching: false,
      error: null,
      refresh: vi.fn(),
    };
    userReadState = {
      data: { key: 'just-written.md', content: '# fresh', size: 7, encoding: 'utf-8' },
      loading: false,
      error: null,
    };

    rerender(
      <MemoryPanel
        workspaceId={null}
        targetKey="just-written.md"
        targetTier="user"
      />,
    );

    // Viewer mounted with content; banner never appeared.
    await waitFor(() =>
      expect(screen.getByTestId('markdown-content')).toBeInTheDocument(),
    );
    expect(
      screen.queryByText(/memoryPanel\.notFound/),
    ).not.toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Fix #6 — body link rewrite heuristic
// ---------------------------------------------------------------------------

describe('MemoryPanel — body link routing (Fix #6)', () => {
  it('rewrites `.md` siblings into the memory dir but passes other extensions through unchanged', async () => {
    const onOpenFile = vi.fn();
    userListState = {
      entries: [baseEntry('memory.md')],
      loading: false,
      isFetching: false,
      error: null,
      refresh: vi.fn(),
    };
    userReadState = {
      data: { key: 'memory.md', content: '# memory', size: 8 },
      loading: false,
      error: null,
    };

    renderWithProviders(
      <MemoryPanel
        workspaceId={null}
        onOpenFile={onOpenFile}
      />,
    );
    const user = userEvent.setup();

    await user.click(await screen.findByText('memory.md'));
    await screen.findByTestId('markdown-content');

    // Bare `.md` → resolved against user memory dir.
    await user.click(screen.getByTestId('md-link-md'));
    expect(onOpenFile).toHaveBeenLastCalledWith(
      '.agents/user/memory/feedback_visualization_preference.md',
      undefined,
    );

    // `./other-note.md` → leading `./` stripped, then resolved.
    await user.click(screen.getByTestId('md-link-md-dotslash'));
    expect(onOpenFile).toHaveBeenLastCalledWith(
      '.agents/user/memory/other-note.md',
      undefined,
    );

    // `reports/q1.pdf` (non-md) → passes through verbatim, stripped of any `./`.
    await user.click(screen.getByTestId('md-link-pdf'));
    expect(onOpenFile).toHaveBeenLastCalledWith('reports/q1.pdf', undefined);

    // Bare `attached.pdf` → still passes through (not a memory entry).
    await user.click(screen.getByTestId('md-link-bare-pdf'));
    expect(onOpenFile).toHaveBeenLastCalledWith('attached.pdf', undefined);

    // Already-qualified `.agents/...` → passes through verbatim.
    await user.click(screen.getByTestId('md-link-qualified'));
    expect(onOpenFile).toHaveBeenLastCalledWith(
      '.agents/skills/foo/skill.md',
      undefined,
    );
  });
});

// ---------------------------------------------------------------------------
// Fix #9 — banner clears on equal-length membership change
// ---------------------------------------------------------------------------

describe('MemoryPanel — not-found banner clears on key-set change (Fix #9)', () => {
  it('clears the banner when the missing key replaces another entry (length unchanged)', async () => {
    userListState = {
      entries: [baseEntry('a.md')],
      loading: false,
      isFetching: false,
      error: null,
      refresh: vi.fn(),
    };

    const { rerender } = renderWithProviders(
      <MemoryPanel
        workspaceId={null}
        targetKey="b.md"
        targetTier="user"
      />,
    );

    await waitFor(() =>
      expect(screen.getByText(/memoryPanel\.notFound/)).toBeInTheDocument(),
    );

    // Same length; swap a.md → b.md.
    await act(async () => {
      userListState = {
        entries: [baseEntry('b.md')],
        loading: false,
        isFetching: false,
        error: null,
        refresh: vi.fn(),
      };
      rerender(
        <MemoryPanel
          workspaceId={null}
          targetKey={null}
          targetTier="user"
        />,
      );
    });

    await waitFor(() =>
      expect(
        screen.queryByText(/memoryPanel\.notFound/),
      ).not.toBeInTheDocument(),
    );
  });
});
