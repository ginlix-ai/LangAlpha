import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook } from '@testing-library/react';

import { useHtmlActions } from '../useHtmlActions';
import { buildWsfilesUrl, buildSharedServeUrl } from '../wsfilesUrl';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const toastMock = vi.fn();
vi.mock('@/components/ui/use-toast', () => ({
  toast: (...args: unknown[]) => toastMock(...args),
}));

const WIDGET_SRCDOC = '<!DOCTYPE html><html><body>widget</body></html>';

describe('buildWsfilesUrl', () => {
  it('builds a path-style URL with slashes preserved', () => {
    expect(buildWsfilesUrl('ws-1', 'results/report.html')).toBe(
      '/api/v1/wsfiles/ws-1/results/report.html',
    );
  });

  it('encodes path segments but keeps slashes', () => {
    expect(buildWsfilesUrl('ws-1', 'results/my report.html')).toBe(
      '/api/v1/wsfiles/ws-1/results/my%20report.html',
    );
  });

  it('strips a leading slash', () => {
    expect(buildWsfilesUrl('ws-1', '/results/report.html')).toBe(
      '/api/v1/wsfiles/ws-1/results/report.html',
    );
  });

  it('appends ?inject=theme only when requested', () => {
    expect(buildWsfilesUrl('ws-1', 'results/report.html', { injectTheme: true })).toBe(
      '/api/v1/wsfiles/ws-1/results/report.html?inject=theme',
    );
    expect(buildWsfilesUrl('ws-1', 'results/report.html')).not.toContain('inject=theme');
  });
});

describe('buildSharedServeUrl', () => {
  it('builds a token-prefixed serve URL with slashes preserved (no workspace UUID)', () => {
    expect(buildSharedServeUrl('tok-1', 'results/report.html')).toBe(
      '/api/v1/public/shared/tok-1/files/serve/results/report.html',
    );
  });

  it('encodes path segments but keeps slashes', () => {
    expect(buildSharedServeUrl('tok-1', 'results/my report.html')).toBe(
      '/api/v1/public/shared/tok-1/files/serve/results/my%20report.html',
    );
  });

  it('appends ?inject=theme only when requested', () => {
    expect(buildSharedServeUrl('tok-1', 'results/report.html', { injectTheme: true })).toBe(
      '/api/v1/public/shared/tok-1/files/serve/results/report.html?inject=theme',
    );
    expect(buildSharedServeUrl('tok-1', 'results/report.html')).not.toContain('inject=theme');
  });
});

describe('useHtmlActions — widget mode', () => {
  let createObjectURL: ReturnType<typeof vi.fn>;
  let revokeObjectURL: ReturnType<typeof vi.fn>;
  let open: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    createObjectURL = vi.fn(() => 'blob:widget-url');
    revokeObjectURL = vi.fn();
    open = vi.fn(() => ({ print: vi.fn(), addEventListener: vi.fn() }));
    vi.stubGlobal('URL', { ...URL, createObjectURL, revokeObjectURL });
    vi.stubGlobal('open', open);
    toastMock.mockClear();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('opens a blob URL in a new tab', () => {
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC, fileName: 'w.html' }),
    );
    result.current.openInNewTab();
    expect(createObjectURL).toHaveBeenCalledTimes(1);
    expect(open).toHaveBeenCalledWith('blob:widget-url', '_blank', 'noopener,noreferrer');
  });

  it('downloads a blob via an anchor', () => {
    const click = vi.fn();
    const realCreate = document.createElement.bind(document);
    const createSpy = vi.spyOn(document, 'createElement').mockImplementation((tag: string) => {
      const el = realCreate(tag);
      if (tag === 'a') el.click = click;
      return el;
    });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC, fileName: 'w.html' }),
    );
    result.current.downloadHtml();
    expect(createObjectURL).toHaveBeenCalled();
    expect(click).toHaveBeenCalledTimes(1);
    expect(revokeObjectURL).toHaveBeenCalled();
    createSpy.mockRestore();
  });

  it('opens a blob tab and auto-prints for PDF', () => {
    const print = vi.fn();
    open.mockReturnValue({ print, addEventListener: vi.fn() });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC }),
    );
    result.current.exportPdf();
    expect(open).toHaveBeenCalledWith('blob:widget-url', '_blank', 'noopener,noreferrer');
  });

  it('copies the srcDoc source to the clipboard', async () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC }),
    );
    result.current.copySource();
    expect(writeText).toHaveBeenCalledWith(WIDGET_SRCDOC);
  });
});

describe('useHtmlActions — file mode', () => {
  let open: ReturnType<typeof vi.fn>;

  beforeEach(() => {
    open = vi.fn(() => ({ print: vi.fn() }));
    vi.stubGlobal('open', open);
    toastMock.mockClear();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('opens the served wsfiles URL (byte-faithful, no inject=theme)', () => {
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html', content: '<p>x</p>' }),
    );
    result.current.openInNewTab();
    expect(open).toHaveBeenCalledWith(
      '/api/v1/wsfiles/ws-1/results/report.html',
      '_blank',
      'noopener,noreferrer',
    );
  });

  it('downloads server original bytes via triggerDownload', () => {
    const triggerDownload = vi.fn().mockResolvedValue(undefined);
    const { result } = renderHook(() =>
      useHtmlActions({
        mode: 'file',
        workspaceId: 'ws-1',
        filePath: 'results/report.html',
        content: '<p>x</p>',
        triggerDownload,
      }),
    );
    result.current.downloadHtml();
    expect(triggerDownload).toHaveBeenCalledWith('ws-1', 'results/report.html');
  });

  it('opens the served URL and attempts print for PDF', () => {
    const print = vi.fn();
    open.mockReturnValue({ print });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html', content: '<p>x</p>' }),
    );
    result.current.exportPdf();
    expect(open).toHaveBeenCalledWith(
      '/api/v1/wsfiles/ws-1/results/report.html',
      '_blank',
      'noopener,noreferrer',
    );
    expect(print).toHaveBeenCalled();
  });

  it('shows the Cmd/Ctrl-P hint toast when print throws', () => {
    open.mockReturnValue({
      print: () => {
        throw new Error('blocked');
      },
    });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html', content: '<p>x</p>' }),
    );
    result.current.exportPdf();
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfPrintHint' });
  });

  it('shows the hint toast when the popup is blocked (no window)', () => {
    open.mockReturnValue(null);
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html', content: '<p>x</p>' }),
    );
    result.current.exportPdf();
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfPrintHint' });
  });

  it('copies the file content to the clipboard', () => {
    const writeText = vi.fn().mockResolvedValue(undefined);
    Object.assign(navigator, { clipboard: { writeText } });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html', content: '<p>source</p>' }),
    );
    result.current.copySource();
    expect(writeText).toHaveBeenCalledWith('<p>source</p>');
  });

  it('uses the servedUrl override (share page) for open-in-new-tab', () => {
    const served = '/api/v1/public/shared/tok-1/files/serve/results/report.html';
    const { result } = renderHook(() =>
      useHtmlActions({
        mode: 'file',
        workspaceId: '',
        filePath: 'results/report.html',
        content: '<p>x</p>',
        servedUrl: served,
      }),
    );
    result.current.openInNewTab();
    expect(open).toHaveBeenCalledWith(served, '_blank', 'noopener,noreferrer');
  });
});
