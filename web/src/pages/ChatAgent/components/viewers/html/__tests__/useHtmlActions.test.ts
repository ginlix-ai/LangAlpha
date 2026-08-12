import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import { renderHook } from '@testing-library/react';

import { useHtmlActions, exportServedPdf } from '../useHtmlActions';
import { buildWsfilesUrl, buildSharedServeUrl } from '../wsfilesUrl';

vi.mock('react-i18next', () => ({
  useTranslation: () => ({ t: (key: string) => key }),
}));

const toastDismiss = vi.fn();
const toastMock = vi.fn((..._args: unknown[]) => ({
  id: '1',
  dismiss: toastDismiss,
  update: vi.fn(),
}));
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

  it('appends ?format=pdf when format is pdf (takes precedence over inject)', () => {
    expect(buildWsfilesUrl('ws-1', 'results/report.html', { format: 'pdf' })).toBe(
      '/api/v1/wsfiles/ws-1/results/report.html?format=pdf',
    );
    expect(
      buildWsfilesUrl('ws-1', 'results/report.html', { format: 'pdf', injectTheme: true }),
    ).toBe('/api/v1/wsfiles/ws-1/results/report.html?format=pdf');
  });

  it('appends the PDF knobs, omitting scale at the default 1', () => {
    expect(
      buildWsfilesUrl('ws-1', 'results/report.html', {
        format: 'pdf',
        pdfScale: 0.8,
        pdfPageNumbers: true,
      }),
    ).toBe('/api/v1/wsfiles/ws-1/results/report.html?format=pdf&scale=0.8&page_numbers=true');
    expect(
      buildWsfilesUrl('ws-1', 'results/report.html', { format: 'pdf', pdfScale: 1 }),
    ).toBe('/api/v1/wsfiles/ws-1/results/report.html?format=pdf');
  });

  it('appends branding=false only when branding is explicitly off', () => {
    expect(
      buildWsfilesUrl('ws-1', 'results/report.html', { format: 'pdf', pdfBranding: false }),
    ).toBe('/api/v1/wsfiles/ws-1/results/report.html?format=pdf&branding=false');
    expect(
      buildWsfilesUrl('ws-1', 'results/report.html', { format: 'pdf', pdfBranding: true }),
    ).toBe('/api/v1/wsfiles/ws-1/results/report.html?format=pdf');
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

  it('appends ?format=pdf when format is pdf', () => {
    expect(buildSharedServeUrl('tok-1', 'results/report.html', { format: 'pdf' })).toBe(
      '/api/v1/public/shared/tok-1/files/serve/results/report.html?format=pdf',
    );
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

  it('opens a blob tab WITHOUT noopener so auto-print fires for PDF', () => {
    vi.useFakeTimers();
    const print = vi.fn();
    open.mockReturnValue({ print, addEventListener: vi.fn() });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC }),
    );
    result.current.exportPdf();
    // No noopener — we need the window handle to drive print on a same-origin blob.
    expect(open).toHaveBeenCalledWith('blob:widget-url', '_blank');
    vi.advanceTimersByTime(800);
    expect(print).toHaveBeenCalled();
  });

  it('opens in a new tab WITH noopener (no handle needed there)', () => {
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC }),
    );
    result.current.openInNewTab();
    expect(open).toHaveBeenCalledWith('blob:widget-url', '_blank', 'noopener,noreferrer');
  });
});

describe('useHtmlActions — file mode', () => {
  let open: ReturnType<typeof vi.fn>;
  let fetchMock: ReturnType<typeof vi.fn>;
  let createObjectURL: ReturnType<typeof vi.fn>;
  let revokeObjectURL: ReturnType<typeof vi.fn>;
  let anchorClick: ReturnType<typeof vi.fn>;
  let lastAnchor: HTMLAnchorElement | undefined;
  let createSpy: { mockRestore: () => void };

  /** A Response stub whose .blob() resolves so the download path completes. */
  const pdfResponse = (ok: boolean, status = ok ? 200 : 501) => ({
    ok,
    status,
    blob: vi.fn().mockResolvedValue(new Blob(['%PDF'], { type: 'application/pdf' })),
  });

  beforeEach(() => {
    open = vi.fn(() => ({ print: vi.fn() }));
    fetchMock = vi.fn();
    createObjectURL = vi.fn(() => 'blob:pdf-url');
    revokeObjectURL = vi.fn();
    anchorClick = vi.fn();
    lastAnchor = undefined;
    vi.stubGlobal('open', open);
    vi.stubGlobal('fetch', fetchMock);
    vi.stubGlobal('URL', { ...URL, createObjectURL, revokeObjectURL });

    const realCreate = document.createElement.bind(document);
    createSpy = vi.spyOn(document, 'createElement').mockImplementation((tag: string) => {
      const el = realCreate(tag) as HTMLElement;
      if (tag === 'a') {
        (el as HTMLAnchorElement).click = anchorClick;
        lastAnchor = el as HTMLAnchorElement;
      }
      return el;
    });
    toastMock.mockClear();
  });

  afterEach(() => {
    createSpy.mockRestore();
    vi.unstubAllGlobals();
    vi.useRealTimers();
    toastDismiss.mockClear();
  });

  it('opens the served wsfiles URL (byte-faithful, no inject=theme)', () => {
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html' }),
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
        triggerDownload,
      }),
    );
    result.current.downloadHtml();
    expect(triggerDownload).toHaveBeenCalledWith('ws-1', 'results/report.html');
  });

  it('fetches the server PDF and downloads it via an anchor named <stem>.pdf', async () => {
    fetchMock.mockResolvedValue(pdfResponse(true));
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html' }),
    );
    await result.current.exportPdf();
    expect(fetchMock).toHaveBeenCalledWith(
      '/api/v1/wsfiles/ws-1/results/report.html?format=pdf',
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(createObjectURL).toHaveBeenCalled();
    expect(lastAnchor?.download).toBe('report.pdf');
    expect(anchorClick).toHaveBeenCalledTimes(1);
    expect(revokeObjectURL).toHaveBeenCalled();
    // No print fallback on the success path.
    expect(open).not.toHaveBeenCalled();
    // The in-flight "generating" toast shows then clears; no print hint.
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfGenerating' });
    expect(toastMock).not.toHaveBeenCalledWith({ description: 'filePanel.pdfPrintHint' });
    expect(toastDismiss).toHaveBeenCalledTimes(1);
  });

  it('composes ?format=pdf onto the servedUrl override (share page)', async () => {
    fetchMock.mockResolvedValue(pdfResponse(true));
    const served = '/api/v1/public/shared/tok-1/files/serve/results/report.html';
    const { result } = renderHook(() =>
      useHtmlActions({
        mode: 'file',
        workspaceId: '',
        filePath: 'results/report.html',
        servedUrl: served,
      }),
    );
    await result.current.exportPdf();
    expect(fetchMock).toHaveBeenCalledWith(
      `${served}?format=pdf`,
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(lastAnchor?.download).toBe('report.pdf');
  });

  it('exportServedPdf composes the PDF knobs onto the servedUrl override', async () => {
    fetchMock.mockResolvedValue(pdfResponse(true));
    const served = '/api/v1/public/shared/tok-1/files/serve/results/report.html';
    await exportServedPdf({
      workspaceId: '',
      filePath: 'results/report.html',
      servedUrl: served,
      printHint: 'hint',
      generatingHint: 'generating',
      scale: 0.8,
      pageNumbers: true,
      branding: false,
    });
    expect(fetchMock).toHaveBeenCalledWith(
      `${served}?format=pdf&scale=0.8&page_numbers=true&branding=false`,
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
    expect(lastAnchor?.download).toBe('report.pdf');
  });

  it('appends ?format=pdf with & when the servedUrl already has a query', async () => {
    fetchMock.mockResolvedValue(pdfResponse(true));
    const served = '/api/v1/public/shared/tok-1/files/serve/results/report.html?v=2';
    const { result } = renderHook(() =>
      useHtmlActions({
        mode: 'file',
        workspaceId: '',
        filePath: 'results/report.html',
        servedUrl: served,
      }),
    );
    await result.current.exportPdf();
    expect(fetchMock).toHaveBeenCalledWith(
      `${served}&format=pdf`,
      expect.objectContaining({ signal: expect.any(AbortSignal) }),
    );
  });

  it('falls back to print (no noopener) + hint toast on a non-OK response (501)', async () => {
    fetchMock.mockResolvedValue(pdfResponse(false, 501));
    open.mockReturnValue({
      print: () => {
        throw new Error('cross-origin print blocked');
      },
    });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html' }),
    );
    await result.current.exportPdf();
    // Keep the handle: open with no third arg.
    expect(open).toHaveBeenCalledWith('/api/v1/wsfiles/ws-1/results/report.html', '_blank');
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfPrintHint' });
    // No anchor download on the failure path.
    expect(anchorClick).not.toHaveBeenCalled();
  });

  it('attempts print and does not toast when the print call succeeds after a 501', async () => {
    const print = vi.fn();
    fetchMock.mockResolvedValue(pdfResponse(false, 501));
    open.mockReturnValue({ print });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html' }),
    );
    await result.current.exportPdf();
    expect(open).toHaveBeenCalledWith('/api/v1/wsfiles/ws-1/results/report.html', '_blank');
    expect(print).toHaveBeenCalled();
    // Print succeeded → no print-hint toast (the generating toast still fires).
    expect(toastMock).not.toHaveBeenCalledWith({ description: 'filePanel.pdfPrintHint' });
  });

  it('falls back to print + hint when fetch rejects', async () => {
    fetchMock.mockRejectedValue(new Error('network down'));
    open.mockReturnValue({
      print: () => {
        throw new Error('blocked');
      },
    });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html' }),
    );
    await result.current.exportPdf();
    expect(open).toHaveBeenCalledWith('/api/v1/wsfiles/ws-1/results/report.html', '_blank');
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfPrintHint' });
  });

  it('aborts a hung fetch after the timeout and falls back to print', async () => {
    vi.useFakeTimers();
    open.mockReturnValue({ print: vi.fn() });
    // Never resolves on its own; rejects only when its signal aborts (mirrors
    // the browser's AbortError) so the timeout is what drives the fallback.
    fetchMock.mockImplementation(
      (_url: string, init: { signal: AbortSignal }) =>
        new Promise((_resolve, reject) => {
          init.signal.addEventListener('abort', () =>
            reject(new DOMException('Aborted', 'AbortError')),
          );
        }),
    );
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html' }),
    );
    const pending = result.current.exportPdf();
    // 120s client cap — advance past it to trip the AbortController.
    await vi.advanceTimersByTimeAsync(120_000);
    await pending;
    expect(open).toHaveBeenCalledWith('/api/v1/wsfiles/ws-1/results/report.html', '_blank');
  });

  it('shows the hint toast when the print popup is blocked (no window)', async () => {
    fetchMock.mockResolvedValue(pdfResponse(false, 504));
    open.mockReturnValue(null);
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html' }),
    );
    await result.current.exportPdf();
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfPrintHint' });
  });

  it('ignores re-entry while a PDF render is in flight', async () => {
    let resolveFetch: (v: unknown) => void = () => {};
    fetchMock.mockReturnValue(
      new Promise((resolve) => {
        resolveFetch = resolve;
      }),
    );
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', workspaceId: 'ws-1', filePath: 'results/report.html' }),
    );
    const first = result.current.exportPdf();
    result.current.exportPdf(); // re-entry, should be ignored
    result.current.exportPdf();
    expect(fetchMock).toHaveBeenCalledTimes(1);
    resolveFetch(pdfResponse(true));
    await first;
    // After the in-flight render settles, a new request is allowed.
    await result.current.exportPdf();
    expect(fetchMock).toHaveBeenCalledTimes(2);
  });

  it('uses the servedUrl override (share page) for open-in-new-tab with noopener', () => {
    const served = '/api/v1/public/shared/tok-1/files/serve/results/report.html';
    const { result } = renderHook(() =>
      useHtmlActions({
        mode: 'file',
        workspaceId: '',
        filePath: 'results/report.html',
        servedUrl: served,
      }),
    );
    result.current.openInNewTab();
    expect(open).toHaveBeenCalledWith(served, '_blank', 'noopener,noreferrer');
  });
});
