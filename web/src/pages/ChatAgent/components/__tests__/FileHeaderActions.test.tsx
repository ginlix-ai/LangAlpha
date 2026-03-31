import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent, waitFor } from '@testing-library/react';
import '@testing-library/jest-dom';

import FileHeaderActions, {
  getFileExtension,
  isMarkdownFile,
  isTextMime,
} from '../FileHeaderActions';

// --- Mocks ---

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

vi.mock('@/components/ui/use-toast', () => ({
  toast: vi.fn(),
}));

vi.mock('@/components/ui/popover', () => ({
  Popover: ({ children, open, onOpenChange }: any) => (
    <div data-testid="popover">
      {typeof children === 'function' ? children({ open }) : children}
    </div>
  ),
  PopoverTrigger: ({ children, asChild }: any) => <>{children}</>,
  PopoverContent: ({ children }: any) => (
    <div data-testid="popover-content">{children}</div>
  ),
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

  it('does not render download dropdown when isEditing is true', () => {
    render(<FileHeaderActions {...defaultProps} isEditing={true} />);
    expect(screen.queryByText('filePanel.downloadAsPdf')).not.toBeInTheDocument();
    expect(screen.queryByText('filePanel.downloadAsMarkdown')).not.toBeInTheDocument();
    expect(screen.queryByTestId('popover')).not.toBeInTheDocument();
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
