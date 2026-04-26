import { describe, it, expect, vi, beforeEach } from 'vitest';
import type { Mock } from 'vitest';
import { screen, waitFor, fireEvent } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import '@testing-library/jest-dom';
import { renderWithProviders } from '@/test/utils';

// ---------------------------------------------------------------------------
// Mocks — keep MemoPanel mountable in jsdom by stubbing heavy deps.
// ---------------------------------------------------------------------------

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string, opts?: Record<string, unknown>) => {
    if (opts && typeof opts === 'object') {
      // Substitute simple {{var}} interpolations so we can find rendered text.
      let out = key;
      for (const [k, v] of Object.entries(opts)) {
        out = out.replace(new RegExp(`{{\\s*${k}\\s*}}`, 'g'), String(v));
      }
      return out;
    }
    return key;
  } }),
}));

// Stub Markdown — full ESM toolchain is too heavy for jsdom and we just need
// to assert that content reaches it.
vi.mock('../Markdown', () => ({
  default: ({ content }: { content: string }) => (
    <div data-testid="markdown-content">{content}</div>
  ),
}));

// Render Radix Dialog inline (no portal) so Testing Library finds its children.
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

// Mock the entire api module so we control list/read/write/delete behavior.
vi.mock('@/pages/ChatAgent/utils/api', () => ({
  listUserMemos: vi.fn(),
  readUserMemo: vi.fn(),
  uploadUserMemo: vi.fn(),
  writeUserMemo: vi.fn(),
  deleteUserMemo: vi.fn(),
  regenerateUserMemo: vi.fn(),
  triggerUserMemoDownload: vi.fn(),
  downloadUserMemoBlobUrl: vi.fn(),
  // workspace listing happens inside MemoPanel via useWorkspaces → axios.
  // We stub the workspaces hook below; this remains for the api path used
  // by useUserMemoList / useReadUserMemo / etc.
  getWorkspaces: vi.fn().mockResolvedValue({ workspaces: [] }),
}));

// useWorkspaces is fired only when there's at least one sandbox-sourced memo,
// but stub it to keep the test deterministic regardless of fixtures.
vi.mock('@/hooks/useWorkspaces', () => ({
  useWorkspaces: () => ({ data: { workspaces: [] } }),
}));

import {
  listUserMemos,
  readUserMemo,
  uploadUserMemo,
  writeUserMemo,
  deleteUserMemo,
  regenerateUserMemo,
  triggerUserMemoDownload,
  downloadUserMemoBlobUrl,
} from '@/pages/ChatAgent/utils/api';
import MemoPanel from '../MemoPanel';

const mockList = listUserMemos as Mock;
const mockRead = readUserMemo as Mock;
const mockUpload = uploadUserMemo as Mock;
const mockWrite = writeUserMemo as Mock;
const mockDelete = deleteUserMemo as Mock;
const mockRegen = regenerateUserMemo as Mock;
const mockDownload = triggerUserMemoDownload as Mock;
const mockBlobUrl = downloadUserMemoBlobUrl as Mock;

beforeEach(() => {
  vi.clearAllMocks();
  // Default: no memos. Individual tests override.
  mockList.mockResolvedValue({ entries: [], truncated: false });
  // jsdom doesn't ship URL.createObjectURL/revokeObjectURL — the PDF viewer
  // path uses them in cleanup.
  if (!URL.createObjectURL) {
    Object.defineProperty(URL, 'createObjectURL', {
      configurable: true,
      writable: true,
      value: vi.fn(() => 'blob:fake-pdf'),
    });
  }
  if (!URL.revokeObjectURL) {
    Object.defineProperty(URL, 'revokeObjectURL', {
      configurable: true,
      writable: true,
      value: vi.fn(),
    });
  }
});

// ---------------------------------------------------------------------------
// Empty state
// ---------------------------------------------------------------------------

describe('MemoPanel — empty state', () => {
  it('renders the empty title when there are no memos', async () => {
    renderWithProviders(<MemoPanel />);

    await waitFor(() =>
      expect(screen.getByText('memoPanel.empty.title')).toBeInTheDocument(),
    );
    expect(screen.getByText('memoPanel.empty.hint')).toBeInTheDocument();
    // Upload button is enabled in empty state
    expect(screen.getByText('memoPanel.uploadButton')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// List rendering with status badges
// ---------------------------------------------------------------------------

describe('MemoPanel — list rendering', () => {
  const baseEntry = {
    key: '',
    original_filename: null,
    mime_type: null,
    size_bytes: 100,
    description: null,
    metadata_status: 'ready' as const,
    created_at: '2026-01-01T00:00:00Z',
    modified_at: null,
    source_kind: null,
    source_workspace_id: null,
    source_path: null,
    sha256: null,
  };

  it('lists memos and shows the "Generating" badge for pending entries', async () => {
    mockList.mockResolvedValue({
      entries: [
        { ...baseEntry, key: 'pending-1', original_filename: 'pending.md', metadata_status: 'pending' },
        { ...baseEntry, key: 'ready-1', original_filename: 'ready.md', metadata_status: 'ready' },
      ],
      truncated: false,
    });

    renderWithProviders(<MemoPanel />);

    await waitFor(() => expect(screen.getByText('pending.md')).toBeInTheDocument());
    expect(screen.getByText('ready.md')).toBeInTheDocument();
    // Both badges visible (translation keys come through unchanged)
    expect(screen.getByText('memoPanel.status.pending')).toBeInTheDocument();
    expect(screen.getByText('memoPanel.status.ready')).toBeInTheDocument();
  });

  it('shows the "Failed" badge when metadata generation failed', async () => {
    mockList.mockResolvedValue({
      entries: [
        { ...baseEntry, key: 'failed-1', original_filename: 'broken.md', metadata_status: 'failed' },
      ],
      truncated: false,
    });

    renderWithProviders(<MemoPanel />);

    await waitFor(() => expect(screen.getByText('broken.md')).toBeInTheDocument());
    expect(screen.getByText('memoPanel.status.failed')).toBeInTheDocument();
  });
});

// ---------------------------------------------------------------------------
// Row click → viewer pane
// ---------------------------------------------------------------------------

describe('MemoPanel — viewer', () => {
  const mdEntry = {
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

  it('switches to the viewer when a row is clicked and renders Markdown content', async () => {
    mockList.mockResolvedValue({ entries: [mdEntry], truncated: false });
    mockRead.mockResolvedValue({
      key: 'note.md',
      content: '# Hello memo',
      encoding: 'utf-8',
      mime_type: 'text/markdown',
      size_bytes: 12,
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
    });

    renderWithProviders(<MemoPanel />);
    const user = userEvent.setup();

    const row = await screen.findByText('note.md');
    await user.click(row);

    // Wait for read query to resolve and content to flow through Markdown stub
    const md = await screen.findByTestId('markdown-content');
    expect(md.textContent).toBe('# Hello memo');

    // Viewer header has a back button + delete button
    expect(screen.getByTitle('memoPanel.backToList')).toBeInTheDocument();
    expect(screen.getByTitle('memoPanel.actions.delete')).toBeInTheDocument();
  });

  it('opens delete confirmation when the trash icon is clicked', async () => {
    mockList.mockResolvedValue({ entries: [mdEntry], truncated: false });
    mockRead.mockResolvedValue({
      key: 'note.md',
      content: 'x',
      encoding: 'utf-8',
      mime_type: 'text/markdown',
      size_bytes: 1,
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
    });

    renderWithProviders(<MemoPanel />);
    const user = userEvent.setup();

    await user.click(await screen.findByText('note.md'));
    await user.click(screen.getByTitle('memoPanel.actions.delete'));

    // Dialog now visible
    expect(await screen.findByText('memoPanel.confirm.deleteTitle')).toBeInTheDocument();
    // Body interpolates the file name
    expect(screen.getByText(/note\.md/)).toBeInTheDocument();
  });

  it('confirming delete calls deleteUserMemo with the key', async () => {
    mockList.mockResolvedValue({ entries: [mdEntry], truncated: false });
    mockRead.mockResolvedValue({
      key: 'note.md',
      content: 'x',
      encoding: 'utf-8',
      mime_type: 'text/markdown',
      size_bytes: 1,
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
    });
    mockDelete.mockResolvedValue(undefined);

    renderWithProviders(<MemoPanel />);
    const user = userEvent.setup();

    await user.click(await screen.findByText('note.md'));
    await user.click(screen.getByTitle('memoPanel.actions.delete'));
    await user.click(await screen.findByText('memoPanel.confirm.deleteConfirm'));

    await waitFor(() => expect(mockDelete).toHaveBeenCalledWith('note.md'));
  });

  it('clicking download invokes triggerUserMemoDownload', async () => {
    mockList.mockResolvedValue({ entries: [mdEntry], truncated: false });
    mockRead.mockResolvedValue({
      key: 'note.md',
      content: 'x',
      encoding: 'utf-8',
      mime_type: 'text/markdown',
      size_bytes: 1,
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
    });
    mockDownload.mockResolvedValue(undefined);

    renderWithProviders(<MemoPanel />);
    const user = userEvent.setup();

    await user.click(await screen.findByText('note.md'));
    await user.click(screen.getByTitle('memoPanel.actions.download'));

    expect(mockDownload).toHaveBeenCalledWith('note.md', 'note.md');
  });
});

// ---------------------------------------------------------------------------
// Failed status → regenerate flow
// ---------------------------------------------------------------------------

describe('MemoPanel — regenerate on failed memo', () => {
  it('shows a regenerate button in viewer header that triggers regenerateUserMemo', async () => {
    const failedEntry = {
      key: 'broken.md',
      original_filename: 'broken.md',
      mime_type: 'text/markdown',
      size_bytes: 12,
      description: null,
      metadata_status: 'failed' as const,
      created_at: '2026-01-01T00:00:00Z',
      modified_at: null,
      source_kind: null,
      source_workspace_id: null,
      source_path: null,
      sha256: null,
    };
    mockList.mockResolvedValue({ entries: [failedEntry], truncated: false });
    mockRead.mockResolvedValue({
      key: 'broken.md',
      content: '',
      encoding: 'utf-8',
      mime_type: 'text/markdown',
      size_bytes: 0,
      original_filename: 'broken.md',
      description: null,
      summary: null,
      metadata_status: 'failed',
      metadata_error: 'boom',
      created_at: null,
      modified_at: null,
      source_kind: null,
      source_workspace_id: null,
      source_path: null,
    });
    mockRegen.mockResolvedValue({
      key: 'broken.md',
      original_filename: 'broken.md',
      metadata_status: 'pending',
    });

    renderWithProviders(<MemoPanel />);
    const user = userEvent.setup();

    await user.click(await screen.findByText('broken.md'));
    const regenBtn = await screen.findByTitle('memoPanel.actions.regenerate');
    await user.click(regenBtn);

    expect(mockRegen).toHaveBeenCalledWith('broken.md');
  });
});

// ---------------------------------------------------------------------------
// Edit → save flow for a markdown memo
// ---------------------------------------------------------------------------

describe('MemoPanel — edit + save', () => {
  it('save calls writeUserMemo with the edited content', async () => {
    const entry = {
      key: 'note.md',
      original_filename: 'note.md',
      mime_type: 'text/markdown',
      size_bytes: 5,
      description: null,
      metadata_status: 'ready' as const,
      created_at: '2026-01-01T00:00:00Z',
      modified_at: null,
      source_kind: null,
      source_workspace_id: null,
      source_path: null,
      sha256: null,
    };
    mockList.mockResolvedValue({ entries: [entry], truncated: false });
    mockRead.mockResolvedValue({
      key: 'note.md',
      content: 'old',
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
    });
    mockWrite.mockResolvedValue({
      key: 'note.md',
      original_filename: 'note.md',
      metadata_status: 'pending',
    });

    renderWithProviders(<MemoPanel />);
    const user = userEvent.setup();

    await user.click(await screen.findByText('note.md'));
    // Wait for read to populate so the Edit button enables
    await screen.findByTestId('markdown-content');

    await user.click(screen.getByTitle('memoPanel.actions.edit'));

    // textarea now visible — replace with new content
    const textarea = await screen.findByDisplayValue('old');
    await user.clear(textarea);
    await user.type(textarea, 'new!');

    await user.click(screen.getByText('memoPanel.actions.save'));

    await waitFor(() =>
      expect(mockWrite).toHaveBeenCalledWith('note.md', 'new!'),
    );
  });
});

// ---------------------------------------------------------------------------
// Drag-and-drop upload
// ---------------------------------------------------------------------------

describe('MemoPanel — drag-and-drop upload', () => {
  it('drops a markdown file → calls uploadUserMemo with that file', async () => {
    mockList.mockResolvedValue({ entries: [], truncated: false });
    mockUpload.mockResolvedValue({
      key: 'dropped.md',
      original_filename: 'dropped.md',
      metadata_status: 'pending',
    });

    const { container } = renderWithProviders(<MemoPanel />);

    await waitFor(() =>
      expect(screen.getByText('memoPanel.empty.title')).toBeInTheDocument(),
    );

    const dropTarget = container.firstChild as HTMLElement;
    expect(dropTarget).toBeTruthy();

    const file = new File(['# hi'], 'dropped.md', { type: 'text/markdown' });

    // The drag handlers gate on dataTransfer.types containing 'Files'.
    fireEvent.dragEnter(dropTarget, {
      dataTransfer: { types: ['Files'], files: [file] },
    });
    fireEvent.drop(dropTarget, {
      dataTransfer: { types: ['Files'], files: [file] },
    });

    await waitFor(() => expect(mockUpload).toHaveBeenCalledTimes(1));
    // doUpload calls uploadMutation.mutateAsync(file). The hook routes a bare
    // File into uploadUserMemo(file) (no extra args).
    expect(mockUpload).toHaveBeenCalledWith(file);
  });

  it('rejects unsupported file types with an inline error', async () => {
    mockList.mockResolvedValue({ entries: [], truncated: false });

    const { container } = renderWithProviders(<MemoPanel />);
    await waitFor(() =>
      expect(screen.getByText('memoPanel.empty.title')).toBeInTheDocument(),
    );

    const dropTarget = container.firstChild as HTMLElement;
    const badFile = new File(['x'], 'image.png', { type: 'image/png' });

    fireEvent.dragEnter(dropTarget, {
      dataTransfer: { types: ['Files'], files: [badFile] },
    });
    fireEvent.drop(dropTarget, {
      dataTransfer: { types: ['Files'], files: [badFile] },
    });

    expect(await screen.findByText('memoPanel.errors.unsupportedType')).toBeInTheDocument();
    expect(mockUpload).not.toHaveBeenCalled();
  });
});

// ---------------------------------------------------------------------------
// PDF viewer — uses object embed via blob URL
// ---------------------------------------------------------------------------

describe('MemoPanel — PDF viewer', () => {
  it('fetches a blob URL and embeds it in an <object> tag', async () => {
    const pdfEntry = {
      key: 'doc.pdf',
      original_filename: 'doc.pdf',
      mime_type: 'application/pdf',
      size_bytes: 999,
      description: null,
      metadata_status: 'ready' as const,
      created_at: '2026-01-01T00:00:00Z',
      modified_at: null,
      source_kind: null,
      source_workspace_id: null,
      source_path: null,
      sha256: null,
    };
    mockList.mockResolvedValue({ entries: [pdfEntry], truncated: false });
    mockRead.mockResolvedValue({
      key: 'doc.pdf',
      content: '',
      encoding: 'binary',
      mime_type: 'application/pdf',
      size_bytes: 999,
      original_filename: 'doc.pdf',
      description: null,
      summary: null,
      metadata_status: 'ready',
      metadata_error: null,
      created_at: null,
      modified_at: null,
      source_kind: null,
      source_workspace_id: null,
      source_path: null,
    });
    mockBlobUrl.mockResolvedValue('blob:fake-pdf');

    const { container } = renderWithProviders(<MemoPanel />);
    const user = userEvent.setup();

    await user.click(await screen.findByText('doc.pdf'));

    await waitFor(() => expect(mockBlobUrl).toHaveBeenCalledWith('doc.pdf'));

    await waitFor(() => {
      const obj = container.querySelector('object');
      expect(obj).toBeTruthy();
      expect(obj?.getAttribute('data')).toBe('blob:fake-pdf');
      expect(obj?.getAttribute('type')).toBe('application/pdf');
    });
  });
});
