import { describe, it, expect, vi, beforeEach } from 'vitest';
import { render, screen, fireEvent } from '@testing-library/react';
import '@testing-library/jest-dom';
import type { ReactElement } from 'react';

import HtmlViewer from '../../HtmlViewer';
import { ThemeProvider, useTheme } from '@/contexts/ThemeContext';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const toastMock = vi.fn();
vi.mock('@/components/ui/use-toast', () => ({
  toast: (...args: unknown[]) => toastMock(...args),
}));

// Avoid pulling the heavy prism-async highlighter into jsdom. The style prop is
// surfaced as data-palette so theme-reactivity can be asserted. (Objects are
// inlined in the factory — vi.mock is hoisted above any top-level consts.)
vi.mock('../../../SyntaxHighlighter', () => ({
  default: ({ children, style }: { children: string; style?: { __palette?: string } }) => (
    <pre data-testid="syntax-highlighter" data-palette={style?.__palette}>
      {children}
    </pre>
  ),
  oneDark: { __palette: 'dark' },
  oneLight: { __palette: 'light' },
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

// HtmlViewer consumes ThemeContext; render under a provider with a control that
// flips the resolved theme so reactivity can be driven the way the app does.
function ThemeToggle() {
  const { setTheme } = useTheme();
  return (
    <>
      <button onClick={() => setTheme('light')}>set-light</button>
      <button onClick={() => setTheme('dark')}>set-dark</button>
    </>
  );
}

function renderViewer(ui: ReactElement) {
  return render(
    <ThemeProvider>
      <ThemeToggle />
      {ui}
    </ThemeProvider>,
  );
}

describe('HtmlViewer', () => {
  beforeEach(() => {
    toastMock.mockClear();
    localStorage.clear();
  });

  it('renders the Preview iframe pointed at the wsfiles served URL with ?inject=theme', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
    const iframe = getPreviewIframe();
    expect(iframe).toBeTruthy();
    expect(iframe.getAttribute('src')).toBe(
      '/api/v1/wsfiles/ws-1/results/report.html?inject=theme',
    );
  });

  it('sandboxes the preview iframe with allow-scripts only', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
    expect(getPreviewIframe().getAttribute('sandbox')).toBe('allow-scripts');
  });

  it('switches to the Source tab and renders the full content in the highlighter', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
    fireEvent.click(screen.getByText('filePanel.htmlSource'));
    const highlighter = screen.getByTestId('syntax-highlighter');
    expect(highlighter).toBeInTheDocument();
    expect(highlighter).toHaveTextContent('<h1>Report</h1>');
  });

  it('re-themes the Source highlighter when the app theme toggles', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
    fireEvent.click(screen.getByText('set-dark'));
    fireEvent.click(screen.getByText('filePanel.htmlSource'));
    expect(screen.getByTestId('syntax-highlighter')).toHaveAttribute('data-palette', 'dark');

    // Switching the app theme must re-theme the highlighter live, with no
    // tab/file remount — the palette is driven by the ThemeContext value, so a
    // context change re-renders the Source tab in place.
    fireEvent.click(screen.getByText('set-light'));
    expect(screen.getByTestId('syntax-highlighter')).toHaveAttribute('data-palette', 'light');

    fireEvent.click(screen.getByText('set-dark'));
    expect(screen.getByTestId('syntax-highlighter')).toHaveAttribute('data-palette', 'dark');
  });

  it('renders only view actions in the toolbar (fullscreen, open-in-new-tab)', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
    expect(screen.getByLabelText('filePanel.fullscreen')).toBeInTheDocument();
    expect(screen.getByLabelText('filePanel.openInNewTab')).toBeInTheDocument();
    // Download/PDF live in the file panel header's download menu, not here.
    expect(screen.queryByLabelText('filePanel.moreActions')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('filePanel.downloadAsHtml')).not.toBeInTheDocument();
    expect(screen.queryByLabelText('filePanel.saveAsPdf')).not.toBeInTheDocument();
  });

  it('opens the fullscreen dialog hosting a served iframe', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
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
    renderViewer(<HtmlViewer {...defaultProps} servedUrlOverride={servedUrlOverride} />);
    expect(getPreviewIframe().getAttribute('src')).toBe(servedUrlOverride);
  });

  it('warns before opening the private wsfiles link, then opens on confirm (owner view)', () => {
    const open = vi.fn();
    vi.stubGlobal('open', open);
    try {
      renderViewer(<HtmlViewer {...defaultProps} />);
      fireEvent.click(screen.getByLabelText('filePanel.openInNewTab'));
      // No tab opened yet — the warning dialog is shown first.
      expect(open).not.toHaveBeenCalled();
      expect(screen.getByText('filePanel.privateLinkWarning')).toBeInTheDocument();
      // Confirm → opens the byte-faithful wsfiles URL (no ?inject=theme).
      fireEvent.click(screen.getByText('filePanel.openInNewTab'));
      expect(open).toHaveBeenCalledWith(
        '/api/v1/wsfiles/ws-1/results/report.html',
        '_blank',
        'noopener,noreferrer',
      );
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('opens directly without a warning on the share page (revocable served URL)', () => {
    const open = vi.fn();
    vi.stubGlobal('open', open);
    try {
      const servedUrlOverride =
        '/api/v1/public/shared/tok-1/files/serve/results/report.html?inject=theme';
      renderViewer(<HtmlViewer {...defaultProps} servedUrlOverride={servedUrlOverride} />);
      fireEvent.click(screen.getByLabelText('filePanel.openInNewTab'));
      expect(open).toHaveBeenCalledWith(
        '/api/v1/public/shared/tok-1/files/serve/results/report.html',
        '_blank',
        'noopener,noreferrer',
      );
      expect(screen.queryByText('filePanel.privateLinkWarning')).not.toBeInTheDocument();
    } finally {
      vi.unstubAllGlobals();
    }
  });

  it('hides the copy-link action by default', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
    expect(screen.queryByLabelText('filePanel.copyShareLink')).not.toBeInTheDocument();
  });

  it('invokes onCopyShareLink with the file path when the link button is clicked', () => {
    const onCopyShareLink = vi.fn();
    renderViewer(<HtmlViewer {...defaultProps} onCopyShareLink={onCopyShareLink} />);
    fireEvent.click(screen.getByLabelText('filePanel.copyShareLink'));
    expect(onCopyShareLink).toHaveBeenCalledWith('results/report.html');
  });
});
