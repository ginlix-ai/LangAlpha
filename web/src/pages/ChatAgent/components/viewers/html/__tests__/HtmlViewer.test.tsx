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

// The owner's served URL rides a grant and "open in new tab" opens the file's
// own /a/ page; both come from hooks the viewer reads, mocked here so the
// test needs no query client. A test can take the grant away or fail it.
const grant = vi.hoisted(() => ({
  state: { minted: true, error: null as Error | null },
  refetch: vi.fn(),
}));
vi.mock('@/hooks/useWorkspaceFileGrant', () => ({
  useWorkspaceFileGrant: (workspaceId: string | null) => ({
    data: workspaceId && grant.state.minted
      ? { prefix: '/api/v1/wsfiles/g/grant-1/', expires_in: 12 * 60 * 60 }
      : undefined,
    error: workspaceId ? grant.state.error : null,
    refetch: grant.refetch,
  }),
}));
vi.mock('@/hooks/useShareLink', () => ({
  useShareLink: (workspaceId: string | null) => ({
    data: workspaceId ? { code: 'abc123abc123' } : undefined,
  }),
}));

const defaultProps = {
  content: '<!DOCTYPE html><html><body><h1>Report</h1></body></html>',
  fileName: 'report.html',
  workspaceId: 'ws-1',
  filePath: 'results/report.html',
};

function getPreviewIframe(): HTMLIFrameElement {
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
    grant.state.minted = true;
    grant.state.error = null;
    grant.refetch.mockClear();
  });

  it('renders the Preview iframe pointed at the wsfiles served URL with ?inject=theme', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
    const iframe = getPreviewIframe();
    expect(iframe).toBeTruthy();
    expect(iframe.getAttribute('src')).toBe(
      '/api/v1/wsfiles/g/grant-1/results/report.html?inject=theme',
    );
  });

  it('sandboxes the preview iframe without allow-same-origin', () => {
    renderViewer(<HtmlViewer {...defaultProps} />);
    expect(getPreviewIframe().getAttribute('sandbox')).toBe(
      'allow-scripts allow-popups allow-popups-to-escape-sandbox',
    );
  });

  it('shows the full content in the highlighter in place of the page', () => {
    renderViewer(<HtmlViewer {...defaultProps} source />);
    expect(getPreviewIframe()).toBeFalsy();
    const highlighter = screen.getByTestId('syntax-highlighter');
    expect(highlighter).toBeInTheDocument();
    expect(highlighter).toHaveTextContent('<h1>Report</h1>');
  });

  describe('an anchored open', () => {
    const rerenderWith = (
      rerender: (ui: ReactElement) => void,
      props: { anchor?: string | null; anchorSeq?: number | null },
    ) => rerender(
      <ThemeProvider>
        <ThemeToggle />
        <HtmlViewer {...defaultProps} {...props} />
      </ThemeProvider>,
    );

    it('asks the document to scroll when the same section is referenced again', () => {
      const { rerender } = renderViewer(
        <HtmlViewer {...defaultProps} anchor="risks" anchorSeq={1} />,
      );
      const iframe = getPreviewIframe();
      const postMessage = vi.fn();
      Object.defineProperty(iframe, 'contentWindow', {
        value: { postMessage },
        configurable: true,
      });

      rerenderWith(rerender, { anchor: 'risks', anchorSeq: 2 });

      // The URL is the one already loaded, so the browser navigates nowhere and
      // the fragment cannot land this open. Without the request the second
      // click moves nothing on screen.
      expect(iframe.getAttribute('src')).toBe(
        '/api/v1/wsfiles/g/grant-1/results/report.html?inject=theme#risks',
      );
      expect(postMessage).toHaveBeenCalledWith({ type: 'widget:scrollTo', id: 'risks' }, '*');
    });
  });

  it('re-themes the Source highlighter when the app theme toggles', () => {
    renderViewer(<HtmlViewer {...defaultProps} source />);
    fireEvent.click(screen.getByText('set-dark'));
    expect(screen.getByTestId('syntax-highlighter')).toHaveAttribute('data-palette', 'dark');

    // Switching the app theme must re-theme the highlighter live, with no
    // tab/file remount — the palette is driven by the ThemeContext value, so a
    // context change re-renders the Source tab in place.
    fireEvent.click(screen.getByText('set-light'));
    expect(screen.getByTestId('syntax-highlighter')).toHaveAttribute('data-palette', 'light');

    fireEvent.click(screen.getByText('set-dark'));
    expect(screen.getByTestId('syntax-highlighter')).toHaveAttribute('data-palette', 'dark');
  });

  it('waits for the served URL before drawing the frame', () => {
    grant.state.minted = false;
    renderViewer(<HtmlViewer {...defaultProps} />);
    expect(getPreviewIframe()).toBeFalsy();
  });

  // A grant that never arrived would otherwise leave a spinner up forever.
  it('shows an error with a retry when the grant cannot be minted', () => {
    grant.state.minted = false;
    grant.state.error = new Error('Request failed with status code 500');
    renderViewer(<HtmlViewer {...defaultProps} />);
    expect(screen.getByRole('alert')).toHaveTextContent('filePanel.htmlPreviewFailed');
    fireEvent.click(screen.getByText('common.retry'));
    expect(grant.refetch).toHaveBeenCalledTimes(1);
  });

  // A failed renewal keeps the grant the frame is already loading under.
  it('keeps the preview when a renewal fails behind a grant it already has', () => {
    grant.state.error = new Error('Request failed with status code 500');
    renderViewer(<HtmlViewer {...defaultProps} />);
    expect(getPreviewIframe()).toBeTruthy();
    expect(screen.queryByRole('alert')).not.toBeInTheDocument();
  });

  it('serves the file under the share page\'s prefix, themed', () => {
    renderViewer(
      <HtmlViewer {...defaultProps} servePrefix="/api/v1/public/shared/tok-1/files/serve/" />,
    );
    expect(getPreviewIframe().getAttribute('src')).toBe(
      '/api/v1/public/shared/tok-1/files/serve/results/report.html?inject=theme',
    );
  });
});
