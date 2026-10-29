import { describe, it, expect, vi } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import { FileErrorDisplay } from '../FilePanel';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

describe('FileErrorDisplay', () => {
  it('shows retry button for sandbox_starting category', () => {
    const onRetry = vi.fn();
    render(<FileErrorDisplay error={{ category: 'sandbox_starting' }} onRetry={onRetry} />);
    const btn = screen.getByText('filePanel.error.retry');
    expect(btn).toBeInTheDocument();
    fireEvent.click(btn);
    expect(onRetry).toHaveBeenCalledOnce();
  });

  it('shows retry button for unknown category', () => {
    render(<FileErrorDisplay error={{ category: 'unknown' }} onRetry={vi.fn()} />);
    expect(screen.getByText('filePanel.error.retry')).toBeInTheDocument();
  });

  it('shows retry button for sandbox_unavailable category', () => {
    render(<FileErrorDisplay error={{ category: 'sandbox_unavailable' }} onRetry={vi.fn()} />);
    expect(screen.getByText('filePanel.error.retry')).toBeInTheDocument();
  });

  it('does not show retry button for not_found category', () => {
    render(<FileErrorDisplay error={{ category: 'not_found' }} onRetry={vi.fn()} />);
    expect(screen.queryByText('filePanel.error.retry')).not.toBeInTheDocument();
  });

  it('shows download button for binary_file category', () => {
    const onDownload = vi.fn();
    render(<FileErrorDisplay error={{ category: 'binary_file' }} onDownload={onDownload} />);
    const btn = screen.getByText('filePanel.error.download');
    expect(btn).toBeInTheDocument();
    fireEvent.click(btn);
    expect(onDownload).toHaveBeenCalledOnce();
  });

  it('does not show download button for non-binary categories', () => {
    render(<FileErrorDisplay error={{ category: 'not_found' }} onDownload={vi.fn()} />);
    expect(screen.queryByText('filePanel.error.download')).not.toBeInTheDocument();
  });

  // The binary hint ends "but you can download it". With no handler there is no
  // button to keep that promise, so the sentence loses the clause instead.
  it('drops the download offer, and the copy promising it, with no handler', () => {
    render(<FileErrorDisplay error={{ category: 'binary_file' }} />);
    expect(screen.queryByText('filePanel.error.download')).not.toBeInTheDocument();
    expect(screen.getByText('filePanel.error.binaryFileNoDownloadHint')).toBeInTheDocument();
    expect(screen.queryByText('filePanel.error.binaryFileHint')).not.toBeInTheDocument();
  });

  it('keeps the download copy when a handler is given', () => {
    render(<FileErrorDisplay error={{ category: 'binary_file' }} onDownload={vi.fn()} />);
    expect(screen.getByText('filePanel.error.binaryFileHint')).toBeInTheDocument();
  });

  it('hides retry button when onRetry is not provided', () => {
    render(<FileErrorDisplay error={{ category: 'unknown' }} />);
    expect(screen.queryByText('filePanel.error.retry')).not.toBeInTheDocument();
  });

  it('renders title and hint for each category', () => {
    render(<FileErrorDisplay error={{ category: 'access_denied' }} />);
    expect(screen.getByText('filePanel.error.accessDenied')).toBeInTheDocument();
    expect(screen.getByText('filePanel.error.accessDeniedHint')).toBeInTheDocument();
  });

  it('says the server\'s sentence in place of the hint', () => {
    render(<FileErrorDisplay error={{ category: 'unknown', message: 'The machine did not accept our key.' }} />);
    expect(screen.getByText('filePanel.error.unknown')).toBeInTheDocument();
    expect(screen.getByText('The machine did not accept our key.')).toBeInTheDocument();
    expect(screen.queryByText('filePanel.error.unknownHint')).not.toBeInTheDocument();
  });
});
