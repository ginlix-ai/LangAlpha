import { afterEach, describe, expect, it, vi } from 'vitest';

/**
 * `desktop` is read once at module load, so every case here rebuilds the module
 * graph with the bridge it wants present or absent.
 */
const load = async (savePdf?: unknown) => {
  if (savePdf) {
    window.langalphaDesktop = {
      version: '0.1.2',
      platform: 'darwin',
      savePdf,
    } as unknown as Window['langalphaDesktop'];
  }
  vi.resetModules();
  return (await import('../shellPdf')).renderToPdf;
};

const root = () => document.getElementById('export-pdf-root');
const pageStyle = () =>
  [...document.head.querySelectorAll('style')].find((s) => s.textContent?.includes('@page'));

afterEach(() => {
  delete window.langalphaDesktop;
  document.body.innerHTML = '';
  document.head.querySelectorAll('style').forEach((s) => s.remove());
  vi.resetModules();
});

describe('renderToPdf', () => {
  it('answers null without a shell, and builds nothing', async () => {
    const renderToPdf = await load();
    const populate = vi.fn();

    expect(await renderToPdf(populate, 'report')).toBeNull();
    // The one case a caller must handle by falling back to browser print, so it
    // has to be reachable without any of the work happening first.
    expect(populate).not.toHaveBeenCalled();
    expect(root()).toBeNull();
  });

  it('gives populate a mounted root and takes it away again', async () => {
    let seen: { mounted: boolean; page: boolean } | null = null;
    const savePdf = vi.fn(async () => ({ saved: true }));
    const renderToPdf = await load(savePdf);

    const result = await renderToPdf((el) => {
      // The node has to be in the document while populate runs: a widget
      // measures itself, and a detached element has no layout to measure.
      seen = { mounted: el.isConnected && el.id === 'export-pdf-root', page: !!pageStyle() };
    }, 'report');

    expect(seen).toEqual({ mounted: true, page: true });
    expect(savePdf).toHaveBeenCalledTimes(1);
    expect(savePdf).toHaveBeenCalledWith({ fileName: 'report' });
    expect(result).toEqual({ saved: true });
    expect(root()).toBeNull();
    expect(pageStyle()).toBeUndefined();
  });

  it('reports a shell that failed inside the channel as an error, not as no shell', async () => {
    // The distinction the caller branches on: `error` may fall back to browser
    // print, `null` means there was never a shell to ask.
    const renderToPdf = await load(async () => {
      throw new Error('no printer');
    });

    const result = await renderToPdf(() => {}, 'report');

    expect(result).toEqual({ error: 'no printer' });
  });

  it('clears the document even when the render throws', async () => {
    // #export-pdf-root left behind is not cosmetic: printExport.css drops every
    // other body child at print media, so a leak turns the user's own Ctrl+P
    // into a blank page for the rest of the session.
    const renderToPdf = await load(async () => {
      throw new Error('boom');
    });

    await renderToPdf(() => {}, 'report');

    expect(root()).toBeNull();
    expect(pageStyle()).toBeUndefined();
  });

  it('clears the document when populate itself throws', async () => {
    const savePdf = vi.fn(async () => ({ saved: true }));
    const renderToPdf = await load(savePdf);

    const result = await renderToPdf(() => {
      throw new Error('bad widget');
    }, 'report');

    expect(result).toEqual({ error: 'bad widget' });
    expect(savePdf).not.toHaveBeenCalled();
    expect(root()).toBeNull();
    expect(pageStyle()).toBeUndefined();
  });

  it('answers a second export canceled rather than null while one is in flight', async () => {
    // Two roots under one id would leave both in the print flow and put the
    // content in the file twice. `canceled` and not `null`, because null sends
    // the caller to browser print on top of the save dialog already open.
    let release: (v: { saved: true }) => void = () => {};
    const savePdf = vi.fn(() => new Promise<{ saved: true }>((r) => { release = r; }));
    const renderToPdf = await load(savePdf);

    const first = renderToPdf(() => {}, 'first');
    const second = await renderToPdf(() => {}, 'second');

    expect(second).toEqual({ canceled: true });
    expect(savePdf).toHaveBeenCalledTimes(1);

    release({ saved: true });
    expect(await first).toEqual({ saved: true });

    // And the guard releases, so the next export is not wedged.
    release = () => {};
    const third = renderToPdf(() => {}, 'third');
    await Promise.resolve(); // past the `await populate`, so savePdf has been reached
    expect(savePdf).toHaveBeenCalledTimes(2);
    release({ saved: true });
    await third;
  });
});
