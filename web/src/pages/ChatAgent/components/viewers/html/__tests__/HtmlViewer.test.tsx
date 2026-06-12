import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';

import HtmlViewer from '../../HtmlViewer';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const toastMock = vi.fn();
vi.mock('@/components/ui/use-toast', () => ({
  toast: (...args: unknown[]) => toastMock(...args),
}));

// Avoid pulling the heavy prism-async highlighter into jsdom.
vi.mock('../../../SyntaxHighlighter', () => ({
  default: ({ children }: { children: string }) => (
    <pre data-testid="syntax-highlighter">{children}</pre>
  ),
  oneDark: {},
  oneLight: {},
}));

const defaultProps = {
  content: '<!DOCTYPE html><html><body><h1>Report</h1></body></html>',
  fileName: 'report.html',
  workspaceId: 'ws-1',
  filePath: 'results/report.html',
  onTriggerDownload: vi.fn(),
};

function getPreviewIframe(): HTMLIFrameElement {
  // Preview iframe is the served one; fullscreen iframe is portaled separately.
  const frame = document.querySelector('iframe.html-viewer-frame');
  return frame as HTMLIFrameElement;
}

describe('HtmlViewer', () => {
  beforeEach(() => {
    toastMock.mockClear();
  });

  it('renders the Preview iframe pointed at the wsfiles served URL with ?inject=theme', () => {
    render(<HtmlViewer {...defaultProps} />);
    const iframe = getPreviewIframe();
    expect(iframe).toBeTruthy();
    expect(iframe.getAttribute('src')).toBe(
      '/api/v1/wsfiles/ws-1/results/report.html?inject=theme',
    );
  });

  it('sandboxes the preview iframe with allow-scripts only', () => {
    render(<HtmlViewer {...defaultProps} />);
    expect(getPreviewIframe().getAttribute('sandbox')).toBe('allow-scripts');
  });

  it('switches to the Source tab and renders the full content in the highlighter', () => {
    render(<HtmlViewer {...defaultProps} />);
    fireEvent.click(screen.getByText('filePanel.htmlSource'));
    const highlighter = screen.getByTestId('syntax-highlighter');
    expect(highlighter).toBeInTheDocument();
    expect(highlighter).toHaveTextContent('<h1>Report</h1>');
  });

  it('renders the HTML action bar (open-in-new-tab, download, PDF, copy, fullscreen)', () => {
    render(<HtmlViewer {...defaultProps} />);
    expect(screen.getByLabelText('filePanel.fullscreen')).toBeInTheDocument();
    expect(screen.getByLabelText('filePanel.openInNewTab')).toBeInTheDocument();
    expect(screen.getByLabelText('filePanel.downloadAsHtml')).toBeInTheDocument();
    expect(screen.getByLabelText('filePanel.saveAsPdf')).toBeInTheDocument();
    expect(screen.getByLabelText('filePanel.copySource')).toBeInTheDocument();
  });

  it('downloads server original bytes via onTriggerDownload', () => {
    const onTriggerDownload = vi.fn();
    render(<HtmlViewer {...defaultProps} onTriggerDownload={onTriggerDownload} />);
    fireEvent.click(screen.getByLabelText('filePanel.downloadAsHtml'));
    expect(onTriggerDownload).toHaveBeenCalledTimes(1);
  });

  it('opens the fullscreen dialog hosting a served iframe', () => {
    render(<HtmlViewer {...defaultProps} />);
    fireEvent.click(screen.getByLabelText('filePanel.fullscreen'));
    // Dialog portals to body; its iframe also points at the served URL.
    const frames = Array.from(document.querySelectorAll('iframe.html-fullscreen-frame'));
    expect(frames).toHaveLength(1);
    expect((frames[0] as HTMLIFrameElement).getAttribute('src')).toBe(
      '/api/v1/wsfiles/ws-1/results/report.html?inject=theme',
    );
  });

  it('points the preview iframe at the servedUrlOverride on the share page', () => {
    const servedUrlOverride =
      '/api/v1/public/shared/tok-1/files/serve/results/report.html?inject=theme';
    render(<HtmlViewer {...defaultProps} servedUrlOverride={servedUrlOverride} />);
    expect(getPreviewIframe().getAttribute('src')).toBe(servedUrlOverride);
  });

  it('hides the copy-link action by default', () => {
    render(<HtmlViewer {...defaultProps} />);
    expect(screen.queryByLabelText('filePanel.copyShareLink')).not.toBeInTheDocument();
  });

  it('invokes onCopyShareLink with the file path when the link button is clicked', () => {
    const onCopyShareLink = vi.fn();
    render(<HtmlViewer {...defaultProps} onCopyShareLink={onCopyShareLink} />);
    fireEvent.click(screen.getByLabelText('filePanel.copyShareLink'));
    expect(onCopyShareLink).toHaveBeenCalledWith('results/report.html');
  });
});
