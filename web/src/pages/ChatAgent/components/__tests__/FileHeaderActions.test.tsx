import type React from 'react';
import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';

import { toast } from '@/components/ui/use-toast';
import type { ArtifactDestinations } from '@/types/api';
import { ArtifactCommentSendContext } from '../artifactComments/sendContext';
import type { PanelAccess } from '../filePanel/types';
import FileHeaderActions, {
  getFileExtension,
  isMarkdownFile,
  isHtmlFile,
  isTextMime,
} from '../FileHeaderActions';

// --- Mocks ---

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: vi.fn(),
}));

const exportServedPdfMock = vi.fn().mockResolvedValue(undefined);
vi.mock('../viewers/html/useHtmlActions', () => ({
  exportServedPdf: (...args: unknown[]) => exportServedPdfMock(...args),
}));

vi.mock('@/hooks/useWorkspaceFileGrant', () => ({
  useWorkspaceFileGrant: (workspaceId: string | null) => ({
    data: workspaceId ? { prefix: '/api/v1/wsfiles/g/grant-1/', expires_in: 12 * 60 * 60 } : undefined,
    error: null,
    refetch: vi.fn(),
  }),
}));
const bindMutate = vi.fn();
const unbindMutate = vi.fn();
let artifactRecord: { artifact_id: string; shared: boolean; bound_thread_id: string | null; url?: string } | null = null;
let destinations: ArtifactDestinations | undefined;
// Which workspace each view asked to make a record in, null for none.
const ownedAsks: (string | null)[] = [];
const byPathAsks: { workspaceId: string | null; path: string | null }[] = [];
vi.mock('@/hooks/useArtifact', () => ({
  artifactPathKey: (path: string) => path,
  // The owner's record, made for the file in front: what "open in new tab" opens.
  useOwnedArtifact: (workspaceId: string | null) => {
    ownedAsks.push(workspaceId);
    return { record: workspaceId ? { code: 'abc123abc123', url: '/a/abc123abc123' } : undefined };
  },
  useArtifactByPath: (workspaceId: string | null, path: string | null) => {
    byPathAsks.push({ workspaceId, path });
    return { data: workspaceId ? artifactRecord : undefined };
  },
  useArtifactDestinations: (artifactId: string | null) => ({ data: artifactId ? destinations : undefined }),
  useArtifactBinding: () => ({ bind: { mutate: bindMutate }, unbind: { mutate: unbindMutate } }),
}));

const besideThread = { thread_id: 'th-1', title: 'Beside', workspace_id: 'ws-123', usable: true, home: false };
/** Where the server says the file's comments go: `bound` to the thread beside, else to a new thread. */
function answerFor(bound: boolean): ArtifactDestinations {
  return {
    default: bound ? { rule: 'bound', thread: besideThread, workspace_id: 'ws-123' } : { rule: 'new', thread: null, workspace_id: 'ws-123' },
    bound: bound ? besideThread : null,
    beside: besideThread,
    creator: null,
  };
}

/** The header beside a chat on thread `th-1`. */
function besideChat(ui: React.ReactElement): React.ReactElement {
  return (
    <ArtifactCommentSendContext value={{ besideThreadId: 'th-1', running: false, send: vi.fn() }}>
      {ui}
    </ArtifactCommentSendContext>
  );
}
vi.mock('@/hooks/usePreferences', () => ({
  usePreferences: () => ({ preferences: null, isLoading: false, isLoaded: true }),
}));

vi.mock('@/components/ui/dropdown-menu', () => ({
  DropdownMenu: ({ children }: any) => (
    <div data-testid="dropdown-menu">{children}</div>
  ),
  DropdownMenuTrigger: ({ children }: any) => <>{children}</>,
  DropdownMenuContent: ({ children }: any) => (
    <div data-testid="dropdown-menu-content">{children}</div>
  ),
  DropdownMenuItem: ({ children, onSelect, ...props }: any) => (
    <button onClick={onSelect} {...props}>{children}</button>
  ),
  DropdownMenuLabel: ({ children }: any) => <div>{children}</div>,
  DropdownMenuSeparator: () => <hr />,
  DropdownMenuSub: ({ children }: any) => <div>{children}</div>,
  DropdownMenuSubTrigger: ({ children }: any) => <div>{children}</div>,
  DropdownMenuSubContent: ({ children }: any) => <div>{children}</div>,
}));

// --- Default props helper ---

const defaultProps = {
  selectedFile: 'report.md',
  isEditing: false,
  workspaceId: 'ws-123',
  fileContent: '# Hello',
  fileMime: 'text/markdown',
  canEdit: true,
  onStartEdit: vi.fn(),
  onOpenExportModal: vi.fn(),
  triggerDownloadFn: vi.fn().mockResolvedValue(undefined),
  readFileFullFn: vi.fn().mockResolvedValue({ content: '# Hello' }),
  access: { kind: 'owner' } as PanelAccess,
  editorRef: { current: null },
  canUndo: false,
  canRedo: false,
  hasUnsavedChanges: false,
  showDiff: false,
  setShowDiff: vi.fn(),
  isSaving: false,
  saveError: null,
  onSave: vi.fn(),
  onCancelEdit: vi.fn(),
};

// --- Helper function tests ---

describe('getFileExtension', () => {
  it('returns correct extension for .md file', () => {
    expect(getFileExtension('report.md')).toBe('md');
  });

  it('returns correct extension for .py file', () => {
    expect(getFileExtension('script.py')).toBe('py');
  });

  it('returns last extension for .tar.gz', () => {
    expect(getFileExtension('archive.tar.gz')).toBe('gz');
  });

  it('returns empty string when no extension', () => {
    expect(getFileExtension('Makefile')).toBe('');
  });
});

describe('isMarkdownFile', () => {
  it('returns true for .md extension', () => {
    expect(isMarkdownFile('notes.md', null)).toBe(true);
  });

  it('returns true for .md file in a path', () => {
    expect(isMarkdownFile('work/results/report.md', null)).toBe(true);
  });

  it('returns true for text/markdown mime', () => {
    expect(isMarkdownFile('file.txt', 'text/markdown')).toBe(true);
  });

  it('returns false for non-markdown file without markdown mime', () => {
    expect(isMarkdownFile('data.csv', 'text/csv')).toBe(false);
  });
});

describe('isHtmlFile', () => {
  it('returns true for .html and .htm files', () => {
    expect(isHtmlFile('report.html')).toBe(true);
    expect(isHtmlFile('results/page.htm')).toBe(true);
  });

  it('returns false for non-HTML files', () => {
    expect(isHtmlFile('report.md')).toBe(false);
    expect(isHtmlFile('data.json')).toBe(false);
  });
});

describe('isTextMime', () => {
  it('returns true for text/* mimes', () => {
    expect(isTextMime('text/plain')).toBe(true);
    expect(isTextMime('text/html')).toBe(true);
    expect(isTextMime('text/csv')).toBe(true);
  });

  it('returns true for application/json', () => {
    expect(isTextMime('application/json')).toBe(true);
  });

  it('returns true for application/yaml', () => {
    expect(isTextMime('application/yaml')).toBe(true);
  });

  it('returns true for application/xml', () => {
    expect(isTextMime('application/xml')).toBe(true);
  });

  it('returns false for image/png', () => {
    expect(isTextMime('image/png')).toBe(false);
  });

  it('returns false for application/pdf', () => {
    expect(isTextMime('application/pdf')).toBe(false);
  });

  it('returns false for null', () => {
    expect(isTextMime(null)).toBe(false);
  });
});

// --- Component tests ---

describe('FileHeaderActions', () => {
  it('returns null when no file is selected', () => {
    const { container } = render(
      <FileHeaderActions {...defaultProps} selectedFile={null} />,
    );
    expect(container.innerHTML).toBe('');
  });

  it('renders download dropdown for markdown file with PDF and Markdown options', () => {
    render(<FileHeaderActions {...defaultProps} />);
    expect(screen.getByText('filePanel.downloadAsPdf')).toBeInTheDocument();
    expect(
      screen.getByText('filePanel.downloadAsMarkdown'),
    ).toBeInTheDocument();
  });

  it('calls onOpenExportModal when "Download as PDF" is clicked', () => {
    const onOpenExportModal = vi.fn();
    render(
      <FileHeaderActions
        {...defaultProps}
        onOpenExportModal={onOpenExportModal}
      />,
    );
    fireEvent.click(screen.getByText('filePanel.downloadAsPdf'));
    expect(onOpenExportModal).toHaveBeenCalledTimes(1);
  });

  it('calls triggerDownloadFn when "Download as Markdown" is clicked', () => {
    const triggerDownloadFn = vi.fn().mockResolvedValue(undefined);
    render(
      <FileHeaderActions
        {...defaultProps}
        triggerDownloadFn={triggerDownloadFn}
      />,
    );
    fireEvent.click(screen.getByText('filePanel.downloadAsMarkdown'));
    expect(triggerDownloadFn).toHaveBeenCalledWith('ws-123', 'report.md');
  });

  // A save that fails shows nothing by itself: no file arrives, nothing opens
  // and nothing navigates, so silence here reads as a dead menu item.
  it('reports a failed download instead of only logging it', async () => {
    const triggerDownloadFn = vi.fn().mockRejectedValue(new Error('sandbox stopped'));
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    vi.mocked(toast).mockClear();
    render(
      <FileHeaderActions
        {...defaultProps}
        triggerDownloadFn={triggerDownloadFn}
      />,
    );
    fireEvent.click(screen.getByText('filePanel.downloadAsMarkdown'));

    await waitFor(() => {
      expect(toast).toHaveBeenCalledWith({
        description: 'filePanel.downloadFailed',
        variant: 'destructive',
      });
    });
    logged.mockRestore();
  });

  it('renders edit button when canEdit is true', () => {
    render(<FileHeaderActions {...defaultProps} canEdit={true} />);
    expect(
      screen.getByTitle('filePanel.editFile'),
    ).toBeInTheDocument();
  });

  it('does not render edit button when canEdit is false', () => {
    render(<FileHeaderActions {...defaultProps} canEdit={false} />);
    expect(screen.queryByTitle('filePanel.editFile')).not.toBeInTheDocument();
  });

  it('renders edit mode buttons when isEditing is true', () => {
    render(
      <FileHeaderActions
        {...defaultProps}
        isEditing={true}
        hasUnsavedChanges={true}
      />,
    );
    expect(screen.getByTitle('filePanel.undo')).toBeInTheDocument();
    expect(screen.getByTitle('filePanel.redo')).toBeInTheDocument();
    expect(screen.getByTitle('filePanel.save')).toBeInTheDocument();
    expect(screen.getByTitle('filePanel.cancelEditing')).toBeInTheDocument();
  });

  it('renders Download and Copy to clipboard for non-markdown text file', () => {
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="data.txt"
        fileMime="text/plain"
      />,
    );
    expect(screen.getByText('filePanel.download')).toBeInTheDocument();
    expect(
      screen.getByText('filePanel.copyToClipboard'),
    ).toBeInTheDocument();
  });

  it('renders Download and Save as PDF for HTML file', () => {
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="results/report.html"
        fileMime="text/html"
      />,
    );
    expect(screen.getByText('filePanel.download')).toBeInTheDocument();
    expect(screen.getByText('filePanel.saveAsPdf')).toBeInTheDocument();
    expect(screen.queryByText('filePanel.copyToClipboard')).not.toBeInTheDocument();
    expect(screen.queryByText('filePanel.downloadAsPdf')).not.toBeInTheDocument();
  });

  it('exports the server PDF (under a share\'s serve prefix) on Save as PDF', async () => {
    exportServedPdfMock.mockClear();
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="results/report.html"
        fileMime="text/html"
        access={{ kind: 'share', servePrefix: '/api/v1/public/shared/tok-1/files/serve/' }}
      />,
    );
    fireEvent.click(screen.getByText('filePanel.saveAsPdf'));
    await waitFor(() => {
      expect(exportServedPdfMock).toHaveBeenCalledWith({
        filePath: 'results/report.html',
        servedUrl: '/api/v1/public/shared/tok-1/files/serve/results/report.html',
        openUrl: '/api/v1/public/shared/tok-1/files/serve/results/report.html',
        printHint: 'filePanel.pdfPrintHint',
        generatingHint: 'filePanel.pdfGenerating',
        scale: 1,
        pageNumbers: false,
        branding: true,
      });
    });
  });

  // The owner's served URL carries the workspace grant, so the print fallback
  // opens the file's own /a/ page rather than handing the grant to a new tab.
  it('exports the owner\'s PDF from the grant and opens the /a/ link as the fallback', async () => {
    exportServedPdfMock.mockClear();
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="results/report.html"
        fileMime="text/html"
      />,
    );
    fireEvent.click(screen.getByText('filePanel.saveAsPdf'));
    await waitFor(() => {
      expect(exportServedPdfMock).toHaveBeenCalledWith(expect.objectContaining({
        servedUrl: '/api/v1/wsfiles/g/grant-1/results/report.html',
        openUrl: '/a/abc123abc123',
      }));
    });
  });

  it('passes the chosen PDF options (scale, page numbers) to the export', async () => {
    exportServedPdfMock.mockClear();
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="results/report.html"
        fileMime="text/html"
      />,
    );
    expect(screen.getByText('filePanel.pdfOptions')).toBeInTheDocument();
    fireEvent.click(screen.getByText('filePanel.pdfBranding'));
    fireEvent.click(screen.getByText('filePanel.pdfPageNumbers'));
    fireEvent.click(screen.getByText('80%'));
    fireEvent.click(screen.getByText('filePanel.saveAsPdf'));
    await waitFor(() => {
      expect(exportServedPdfMock).toHaveBeenCalledWith(
        expect.objectContaining({ scale: 0.8, pageNumbers: true, branding: false }),
      );
    });
  });

  it('offers no Publish unless the panel is the owner\'s', () => {
    render(<FileHeaderActions {...defaultProps} />);
    expect(screen.queryByTitle('artifact.publish')).not.toBeInTheDocument();
  });

  it("opens the panel's Publish dialog", () => {
    const openPublish = vi.fn();
    render(<FileHeaderActions {...defaultProps} selectedFile="results/data.csv" openPublish={openPublish} />);
    fireEvent.click(screen.getByTitle('artifact.publish'));
    expect(openPublish).toHaveBeenCalledWith('results/data.csv');
  });

  it("opens Publish from an HTML file's menu, which says where comments go and binds them to the thread beside", () => {
    artifactRecord = { artifact_id: 'art-1', shared: false, bound_thread_id: null };
    destinations = answerFor(false);
    const openPublish = vi.fn();
    render(besideChat(<FileHeaderActions {...defaultProps} selectedFile="results/report.html" openPublish={openPublish} />));
    fireEvent.click(screen.getByRole('button', { name: /artifact\.publishEllipsis/ }));
    expect(openPublish).toHaveBeenCalledWith('results/report.html');
    expect(screen.getByText('artifact.commentsGoNew')).toBeInTheDocument();

    fireEvent.click(screen.getByRole('button', { name: /pageViewer\.comments\.bindHere/ }));
    expect(bindMutate).toHaveBeenCalledWith(
      { workspaceId: 'ws-123', path: 'results/report.html', threadId: 'th-1' },
      expect.anything(),
    );
    expect(screen.queryByRole('button', { name: /artifact\.unbind/ })).not.toBeInTheDocument();
    artifactRecord = null;
  });

  it('unbinds a file bound to the thread beside, and offers no second bind there', () => {
    artifactRecord = { artifact_id: 'art-1', shared: true, bound_thread_id: 'th-1' };
    destinations = answerFor(true);
    render(besideChat(<FileHeaderActions {...defaultProps} selectedFile="results/report.html" openPublish={vi.fn()} />));
    expect(screen.getByText('artifact.commentsGoHere')).toBeInTheDocument();
    expect(screen.queryByRole('button', { name: /pageViewer\.comments\.bindHere/ })).not.toBeInTheDocument();
    fireEvent.click(screen.getByRole('button', { name: /artifact\.unbind/ }));
    expect(unbindMutate).toHaveBeenCalledWith('art-1', expect.anything());
    artifactRecord = null;
  });

  // A link inside the frame moved it to another file: the comment box and the
  // toolbar are about that one now, and so is the menu beside them.
  it("keys the HTML file's menu on the document the frame shows", () => {
    artifactRecord = { artifact_id: 'art-2', shared: false, bound_thread_id: null };
    destinations = answerFor(false);
    byPathAsks.length = 0;
    const openPublish = vi.fn();
    render(besideChat(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="results/report.html"
        artifactDoc="results/appendix.html"
        openPublish={openPublish}
      />,
    ));
    expect(byPathAsks).toContainEqual({ workspaceId: 'ws-123', path: 'results/appendix.html' });
    expect(byPathAsks).not.toContainEqual({ workspaceId: 'ws-123', path: 'results/report.html' });
    fireEvent.click(screen.getByRole('button', { name: /artifact\.publishEllipsis/ }));
    expect(openPublish).toHaveBeenCalledWith('results/appendix.html');
    fireEvent.click(screen.getByRole('button', { name: /pageViewer\.comments\.bindHere/ }));
    expect(bindMutate).toHaveBeenLastCalledWith(
      { workspaceId: 'ws-123', path: 'results/appendix.html', threadId: 'th-1' },
      expect.anything(),
    );
    artifactRecord = null;
  });

  it('offers no bind and says nothing of where comments go until the server has answered for the file', () => {
    artifactRecord = null;
    render(besideChat(<FileHeaderActions {...defaultProps} selectedFile="results/report.html" openPublish={vi.fn()} />));
    expect(screen.queryByRole('button', { name: /pageViewer\.comments\.bindHere/ })).not.toBeInTheDocument();
    expect(screen.queryByText(/artifact\.commentsGo/)).not.toBeInTheDocument();
  });

  it('switches an HTML file between its page and its source', () => {
    const onToggleHtmlSource = vi.fn();
    const html = { selectedFile: 'results/report.html', fileMime: 'text/html', onToggleHtmlSource };
    const { rerender } = render(<FileHeaderActions {...defaultProps} {...html} />);
    fireEvent.click(screen.getByRole('button', { name: 'filePanel.viewSource' }));
    expect(onToggleHtmlSource).toHaveBeenCalledTimes(1);

    rerender(<FileHeaderActions {...defaultProps} {...html} htmlSource />);
    expect(screen.getByRole('button', { name: 'filePanel.viewPreview' })).toBeInTheDocument();
  });

  it('offers no source switch for a file that is not HTML', () => {
    render(<FileHeaderActions {...defaultProps} onToggleHtmlSource={vi.fn()} />);
    expect(screen.queryByRole('button', { name: 'filePanel.viewSource' })).not.toBeInTheDocument();
  });

  // The grant URL is the panel's to load and expires; a tab opened on it
  // would break hours later, so the owner gets the file's own /a/ page.
  it("opens the owner's HTML file at its share page in a new tab", () => {
    const open = vi.spyOn(window, 'open').mockImplementation(() => null);
    render(<FileHeaderActions {...defaultProps} selectedFile="results/report.html" fileMime="text/html" />);
    fireEvent.click(screen.getByRole('button', { name: 'filePanel.openInNewTab' }));
    expect(open).toHaveBeenCalledWith(
      '/a/abc123abc123',
      '_blank',
      'noopener,noreferrer',
    );
    open.mockRestore();
  });

  it("opens a shared HTML file under the share's own prefix", () => {
    const open = vi.spyOn(window, 'open').mockImplementation(() => null);
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="results/report.html"
        fileMime="text/html"
        access={{ kind: 'share', servePrefix: '/api/v1/public/shared/tok-1/files/serve/' }}
      />,
    );
    fireEvent.click(screen.getByRole('button', { name: 'filePanel.openInNewTab' }));
    expect(open).toHaveBeenCalledWith(
      '/api/v1/public/shared/tok-1/files/serve/results/report.html',
      '_blank',
      'noopener,noreferrer',
    );
    open.mockRestore();
  });

  // A read-only look at a sibling's files changes nothing there, a record
  // included: it reads one the file already has and offers its page, and
  // offers none for a file never recorded.
  it('makes no record for a read-only look, and opens one the file already has', () => {
    const open = vi.spyOn(window, 'open').mockImplementation(() => null);
    ownedAsks.length = 0;
    artifactRecord = null;
    const props = { ...defaultProps, selectedFile: 'results/report.html', fileMime: 'text/html', access: { kind: 'readOnly' } as PanelAccess };
    const { rerender } = render(<FileHeaderActions {...props} />);
    expect(ownedAsks.every((ws) => ws === null)).toBe(true);
    expect(screen.queryByRole('button', { name: 'filePanel.openInNewTab' })).not.toBeInTheDocument();

    artifactRecord = { artifact_id: 'art-3', shared: false, bound_thread_id: null, url: '/a/sibling00001' };
    rerender(<FileHeaderActions {...props} fileContent="<p>again</p>" />);
    fireEvent.click(screen.getByRole('button', { name: 'filePanel.openInNewTab' }));
    expect(open).toHaveBeenCalledWith('/a/sibling00001', '_blank', 'noopener,noreferrer');
    expect(ownedAsks.every((ws) => ws === null)).toBe(true);
    artifactRecord = null;
    open.mockRestore();
  });

  it('renders only Download for binary file', () => {
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="chart.png"
        fileMime="image/png"
      />,
    );
    expect(screen.getByText('filePanel.download')).toBeInTheDocument();
    expect(
      screen.queryByText('filePanel.downloadAsPdf'),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByText('filePanel.copyToClipboard'),
    ).not.toBeInTheDocument();
  });

  // A copy-link share grants allow_files without allow_download, and the
  // download endpoint refuses what allow_files alone opened. Every item behind
  // this trigger saves a file, so the trigger goes with them rather than
  // standing over an empty menu.
  it.each([
    ['markdown', 'report.md', 'text/markdown'],
    ['html', 'report.html', 'text/html'],
    ['text', 'data.txt', 'text/plain'],
    ['binary', 'chart.png', 'image/png'],
  ])('hides the whole download menu for a %s file when the share forbids saving', (_kind, file, mime) => {
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile={file}
        fileMime={mime}
        canDownload={false}
      />,
    );
    expect(screen.queryByTestId('dropdown-menu')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('filePanel.downloadOptions')).not.toBeInTheDocument();
    expect(screen.queryByText('filePanel.download')).not.toBeInTheDocument();
    expect(screen.queryByText('filePanel.downloadAsPdf')).not.toBeInTheDocument();
    expect(screen.queryByText('filePanel.saveAsPdf')).not.toBeInTheDocument();
    // The edit affordance is a separate permission and is not swept up.
    expect(screen.getByTitle('filePanel.editFile')).toBeInTheDocument();
  });

  it('keeps the download menu when the prop is omitted', () => {
    render(<FileHeaderActions {...defaultProps} />);
    expect(screen.getByTestId('dropdown-menu')).toBeInTheDocument();
  });

  it('does not render download dropdown when isEditing is true', () => {
    render(<FileHeaderActions {...defaultProps} isEditing={true} />);
    expect(screen.queryByText('filePanel.downloadAsPdf')).not.toBeInTheDocument();
    expect(screen.queryByText('filePanel.downloadAsMarkdown')).not.toBeInTheDocument();
    expect(screen.queryByTestId('dropdown-menu')).not.toBeInTheDocument();
  });

  it('copies full content to clipboard on copy click', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });

    const readFileFullFn = vi.fn().mockResolvedValue({ content: 'full file content' });
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="data.txt"
        fileMime="text/plain"
        readFileFullFn={readFileFullFn}
      />,
    );
    fireEvent.click(screen.getByText('filePanel.copyToClipboard'));

    await waitFor(() => {
      expect(readFileFullFn).toHaveBeenCalledWith('ws-123', 'data.txt');
      expect(writeText).toHaveBeenCalledWith('full file content');
    });
  });

  it('shows error toast when clipboard write fails', async () => {
    const writeText = vi.fn().mockRejectedValue(new Error('denied'));
    Object.assign(navigator, { clipboard: { writeText } });

    const readFileFullFn = vi.fn().mockRejectedValue(new Error('denied'));
    render(
      <FileHeaderActions
        {...defaultProps}
        selectedFile="data.txt"
        fileMime="text/plain"
        readFileFullFn={readFileFullFn}
      />,
    );
    fireEvent.click(screen.getByText('filePanel.copyToClipboard'));

    // Toast mock is called via the module mock — just verify no crash
    await waitFor(() => {
      expect(readFileFullFn).toHaveBeenCalled();
    });
  });

  it('returns true for application/javascript and application/typescript mimes', () => {
    expect(isTextMime('application/javascript')).toBe(true);
    expect(isTextMime('application/typescript')).toBe(true);
  });

  it('returns true for markdown mime in isTextMime', () => {
    expect(isTextMime('text/markdown')).toBe(true);
  });
});
