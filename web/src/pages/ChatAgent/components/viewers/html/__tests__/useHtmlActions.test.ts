import { describe, it, expect, vi, beforeEach, afterEach, type Mock } from 'vitest';
import { renderHook } from '@testing-library/react';

import { useHtmlActions, exportServedPdf } from '../useHtmlActions';
import { buildServeUrl, pdfQuery } from '../wsfilesUrl';

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
// The owner's served URL rides a grant prefix; what opens in a tab is the
// item's own page, never a served URL.
const GRANT = '/api/v1/wsfiles/g/grant-1/';
const SERVED = '/api/v1/wsfiles/g/grant-1/results/report.html';
const OPEN = 'http://localhost:3000/a/abc123abc123';

describe('buildServeUrl', () => {
  it('builds a path-style URL with slashes preserved', () => {
    expect(buildServeUrl(GRANT, 'results/report.html')).toBe(
      '/api/v1/wsfiles/g/grant-1/results/report.html',
    );
  });

  it('encodes path segments but keeps slashes', () => {
    expect(buildServeUrl(GRANT, 'results/my report.html')).toBe(
      '/api/v1/wsfiles/g/grant-1/results/my%20report.html',
    );
  });

  it('strips a leading slash', () => {
    expect(buildServeUrl(GRANT, '/results/report.html')).toBe(
      '/api/v1/wsfiles/g/grant-1/results/report.html',
    );
  });

  it('appends ?inject=theme only when requested', () => {
    expect(buildServeUrl(GRANT, 'results/report.html', { injectTheme: true })).toBe(
      '/api/v1/wsfiles/g/grant-1/results/report.html?inject=theme',
    );
    expect(buildServeUrl(GRANT, 'results/report.html')).not.toContain('inject=theme');
  });
});

// The PDF render query rides on the plain served URL, never the themed one.
describe('pdfQuery', () => {
  it('is bare at the defaults', () => {
    expect(pdfQuery()).toBe('format=pdf');
    expect(pdfQuery(1, false, true)).toBe('format=pdf');
  });

  it('appends the PDF knobs, omitting scale at the default 1', () => {
    expect(pdfQuery(0.8, true)).toBe('format=pdf&scale=0.8&page_numbers=true');
  });

  it('appends branding=false only when branding is explicitly off', () => {
    expect(pdfQuery(undefined, undefined, false)).toBe('format=pdf&branding=false');
    expect(pdfQuery(undefined, undefined, undefined)).toBe('format=pdf');
  });
});

describe('buildServeUrl under a share prefix', () => {
  const prefix = '/api/v1/public/shared/tok-1/files/serve/';

  it('builds a token-prefixed serve URL with slashes preserved (no workspace UUID)', () => {
    expect(buildServeUrl(prefix, 'results/report.html')).toBe(
      '/api/v1/public/shared/tok-1/files/serve/results/report.html',
    );
  });

  it('encodes path segments but keeps slashes', () => {
    expect(buildServeUrl(prefix, 'results/my report.html')).toBe(
      '/api/v1/public/shared/tok-1/files/serve/results/my%20report.html',
    );
  });

  it('appends ?inject=theme only when requested', () => {
    expect(buildServeUrl(prefix, 'results/report.html', { injectTheme: true })).toBe(
      '/api/v1/public/shared/tok-1/files/serve/results/report.html?inject=theme',
    );
    expect(buildServeUrl(prefix, 'results/report.html')).not.toContain('inject=theme');
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
    // Here rather than at the end of the test that spies: a failed assertion
    // skips the rest of its body, and the spy would outlive the test.
    vi.restoreAllMocks();
    vi.unstubAllGlobals();
    vi.useRealTimers();
  });

  it('opens a blob URL in a new tab and revokes it once the tab has had time to load', () => {
    vi.useFakeTimers();
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC, fileName: 'w.html' }),
    );
    result.current.openInNewTab!();
    expect(createObjectURL).toHaveBeenCalledTimes(1);
    expect(open).toHaveBeenCalledWith('blob:widget-url', '_blank', 'noopener,noreferrer');
    // Revoked at once, the tab would load nothing; never revoked, each click
    // keeps a copy of the widget alive for the life of the page.
    expect(revokeObjectURL).not.toHaveBeenCalled();
    vi.advanceTimersByTime(60_000);
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:widget-url');
  });

  it('downloads a blob via an anchor', () => {
    const click = vi.fn();
    const realCreate = document.createElement.bind(document);
    vi.spyOn(document, 'createElement').mockImplementation((tag: string) => {
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
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:widget-url');
  });

  it('opens a blob tab WITHOUT noopener so auto-print fires for PDF', async () => {
    vi.useFakeTimers();
    const print = vi.fn();
    open.mockReturnValue({ print, addEventListener: vi.fn() });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC }),
    );
    // Awaited: the shell is asked first now, so the fallback lands a microtask
    // later even when there is no shell to ask.
    await result.current.exportPdf();
    // No noopener — we need the window handle to drive print on a same-origin blob.
    expect(open).toHaveBeenCalledWith('blob:widget-url', '_blank');
    vi.advanceTimersByTime(800);
    expect(print).toHaveBeenCalled();
    expect(revokeObjectURL).not.toHaveBeenCalled();
    vi.advanceTimersByTime(60_000);
    expect(revokeObjectURL).toHaveBeenCalledWith('blob:widget-url');
  });

  it('opens in a new tab WITH noopener (no handle needed there)', () => {
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'widget', srcDoc: WIDGET_SRCDOC }),
    );
    result.current.openInNewTab!();
    expect(open).toHaveBeenCalledWith('blob:widget-url', '_blank', 'noopener,noreferrer');
  });
});

describe('useHtmlActions — file mode', () => {
  let open: ReturnType<typeof vi.fn>;
  let fetchMock: ReturnType<typeof vi.fn>;
  let createObjectURL: ReturnType<typeof vi.fn>;
  let revokeObjectURL: ReturnType<typeof vi.fn>;
  let anchorClick: Mock<() => void>;
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

  it("opens the item's own page, never the served URL", () => {
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
    );
    result.current.openInNewTab!();
    expect(open).toHaveBeenCalledWith(OPEN, '_blank', 'noopener,noreferrer');
  });

  it('downloads server original bytes via triggerDownload', () => {
    const triggerDownload = vi.fn().mockResolvedValue(undefined);
    const { result } = renderHook(() =>
      useHtmlActions({
        mode: 'file',
        filePath: 'results/report.html',
        servedUrl: SERVED,
        openUrl: OPEN,
        triggerDownload,
      }),
    );
    result.current.downloadHtml();
    expect(triggerDownload).toHaveBeenCalledTimes(1);
  });

  it('fetches the server PDF and downloads it via an anchor named <stem>.pdf', async () => {
    fetchMock.mockResolvedValue(pdfResponse(true));
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
    );
    await result.current.exportPdf();
    expect(fetchMock).toHaveBeenCalledWith(
      `${SERVED}?format=pdf`,
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
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
    );
    await result.current.exportPdf();
    // Keep the handle: open with no third arg.
    expect(open).toHaveBeenCalledWith(OPEN, '_blank');
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfPrintHint' });
    // No anchor download on the failure path.
    expect(anchorClick).not.toHaveBeenCalled();
  });

  it('attempts print and does not toast when the print call succeeds after a 501', async () => {
    const print = vi.fn();
    fetchMock.mockResolvedValue(pdfResponse(false, 501));
    open.mockReturnValue({ print });
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
    );
    await result.current.exportPdf();
    expect(open).toHaveBeenCalledWith(OPEN, '_blank');
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
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
    );
    await result.current.exportPdf();
    expect(open).toHaveBeenCalledWith(OPEN, '_blank');
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
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
    );
    const pending = result.current.exportPdf();
    // 120s client cap — advance past it to trip the AbortController.
    await vi.advanceTimersByTimeAsync(120_000);
    await pending;
    expect(open).toHaveBeenCalledWith(OPEN, '_blank');
  });

  it('shows the hint toast when the print popup is blocked (no window)', async () => {
    fetchMock.mockResolvedValue(pdfResponse(false, 504));
    open.mockReturnValue(null);
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
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
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
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

  it('withholds open-in-new-tab until the page to open is known', () => {
    const { result } = renderHook(() =>
      useHtmlActions({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED }),
    );
    expect(result.current.openInNewTab).toBeUndefined();
  });

  it('opens the share page for the print fallback, with the handle kept', async () => {
    fetchMock.mockResolvedValue(pdfResponse(false, 501));
    const print = vi.fn();
    open.mockReturnValue({ print });
    await exportServedPdf({
      filePath: 'results/report.html',
      servedUrl: '/api/v1/public/shared/tok-1/files/serve/results/report.html',
      openUrl: 'http://localhost:3000/s/tok-1',
      printHint: 'hint',
      generatingHint: 'generating',
    });
    expect(open).toHaveBeenCalledWith('http://localhost:3000/s/tok-1', '_blank');
    expect(print).toHaveBeenCalled();
  });
});

// The shell answers every `window.open` by handing the URL to the OS browser,
// which takes http/https/mailto and nothing else. A widget is a `blob:` that
// belongs to the renderer that made it, so there is no fallback to offer and the
// action is withheld rather than left as a button that does nothing. `desktop` is
// read once at module load, so reaching this needs the module graph rebuilt.
describe('useHtmlActions — inside the desktop shell', () => {
  const saveWidgetPdf = vi.fn();
  let open: ReturnType<typeof vi.fn>;

  const install = async () => {
    window.langalphaDesktop = { version: '0.1.2', platform: 'darwin', savePdf: vi.fn() };
    vi.resetModules();
    // `doMock` and not `vi.mock`: the latter is hoisted to the top of the file
    // and would take the real module away from the widget test above, which
    // exercises it for real to prove the no-shell path still reaches the tab.
    vi.doMock('../widgetPdf', () => ({ saveWidgetPdf }));
    return (await import('../useHtmlActions')).useHtmlActions;
  };

  beforeEach(() => {
    toastMock.mockClear();
    toastDismiss.mockClear();
    open = vi.fn().mockReturnValue(null);
    vi.stubGlobal('open', open);
    // A subclass, not a plain object: `install` imports while this stub is in
    // place, and the module runner builds each module's file URL with `new URL`.
    vi.stubGlobal(
      'URL',
      class extends URL {
        static override createObjectURL = vi.fn(() => 'blob:widget-url');
        static override revokeObjectURL = vi.fn();
      },
    );
  });

  afterEach(() => {
    delete window.langalphaDesktop;
    saveWidgetPdf.mockReset();
    vi.doUnmock('../widgetPdf');
    vi.unstubAllGlobals();
    vi.resetModules();
  });

  it('withholds open-in-new-tab for a widget, whose blob the shell cannot open', async () => {
    const hook = await install();
    const { result } = renderHook(() => hook({ mode: 'widget', srcDoc: WIDGET_SRCDOC }));
    expect(result.current.openInNewTab).toBeUndefined();
  });

  it('keeps it for a served file, whose URL opens in the real browser', async () => {
    const hook = await install();
    const { result } = renderHook(() =>
      hook({ mode: 'file', filePath: 'results/report.html', servedUrl: SERVED, openUrl: OPEN }),
    );
    expect(result.current.openInNewTab).toBeDefined();
  });

  // Three answers, three different responses, and only one of them is "print in
  // the browser instead". The distinction is documented at both call sites and
  // was pinned at neither: narrowing the guard to `'saved' in result` — which
  // reads as a tidy-up — puts a print dialog on top of the save dialog the user
  // just dismissed, and inside the shell that print dialog cannot even be
  // reached, because the tab it wants to open is a `blob:` the shell refuses.
  const outcomes: Array<[string, unknown, boolean]> = [
    ['saved', { saved: true }, false],
    ['canceled', { canceled: true }, false],
    ['error', { error: 'no printer' }, false],
    // `null` is covered on its own below: inside the shell it means the shell is
    // too old, which is not the browser's fallback case.
  ];

  for (const [label, answer, fallsBack] of outcomes) {
    it(`${fallsBack ? 'falls back to the browser' : 'stays put'} on ${label}`, async () => {
      saveWidgetPdf.mockResolvedValue(answer);
      const hook = await install();
      const { result } = renderHook(() => hook({ mode: 'widget', srcDoc: WIDGET_SRCDOC }));

      await result.current.exportPdf();

      expect(saveWidgetPdf).toHaveBeenCalledWith(WIDGET_SRCDOC, 'widget');
      expect(open).toHaveBeenCalledTimes(fallsBack ? 1 : 0);
    });
  }

  it('says so on a shell too old to know the channel, rather than nothing at all', async () => {
    // The blob tab is the browser's fallback and the shell refuses to open one,
    // so falling through here is a button that does nothing and says nothing.
    // Every install older than the one that added savePdf lands on this line.
    saveWidgetPdf.mockResolvedValue(null);
    const hook = await install();
    const { result } = renderHook(() => hook({ mode: 'widget', srcDoc: WIDGET_SRCDOC }));

    await result.current.exportPdf();

    expect(open).not.toHaveBeenCalled();
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfFailed' });
  });

  it('shows the same generating toast the served export shows, and clears it', async () => {
    // 700ms to 8s of measuring with no feedback reads as a broken button.
    saveWidgetPdf.mockResolvedValue({ saved: true });
    const hook = await install();
    const { result } = renderHook(() => hook({ mode: 'widget', srcDoc: WIDGET_SRCDOC }));

    await result.current.exportPdf();

    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfGenerating' });
    expect(toastDismiss).toHaveBeenCalledTimes(1);
  });

  it('tells the user when the shell failed, and stays silent when they cancelled', async () => {
    saveWidgetPdf.mockResolvedValue({ error: 'no printer' });
    let hook = await install();
    let view = renderHook(() => hook({ mode: 'widget', srcDoc: WIDGET_SRCDOC }));
    await view.result.current.exportPdf();
    expect(toastMock).toHaveBeenCalledWith({ description: 'filePanel.pdfFailed' });

    toastMock.mockClear();
    saveWidgetPdf.mockResolvedValue({ canceled: true });
    hook = await install();
    view = renderHook(() => hook({ mode: 'widget', srcDoc: WIDGET_SRCDOC }));
    await view.result.current.exportPdf();
    // Dismissing a save dialog is not an error, and reporting it as one is how
    // an app tells the user it was not listening. The generating toast is still
    // raised and dismissed, so this asks about the failure specifically.
    expect(toastMock).not.toHaveBeenCalledWith({ description: 'filePanel.pdfFailed' });
  });
});
