/**
 * E2E for a large PDF read by range: the file panel opens it from its first
 * slice, and pdf.js asks for the rest as pages need it, through a download
 * route that answers one byte range the way the backend does.
 *
 * The PDFs are built here. Their layout is the costly one for a range reader: one
 * flat page tree whose page dicts sit between ~200 KB images, so opening the
 * file (pdf.js finds the last page by walking every top-level kid) touches every
 * page dict, and only a small chunk size keeps that walk cheap.
 */
import { Buffer } from 'node:buffer';
import crypto from 'node:crypto';
import { configureSSE, resetMockServer, mockAPI, test, expect } from './fixtures.js';
import { sseEvents } from './helpers/mockResponses.js';
import { chatViewOverrides, WS, TH } from './helpers/chatScenario.js';

const FILE = 'big.pdf';
const OPENING_RANGE = 'bytes=0-4194303';
const SERVER_CAP = 8 * 1024 * 1024;

/** A PDF of `pages` pages, each a `side`-pixel square of gray noise. */
function makePdf(pages, side) {
  const parts = [];
  const offsets = [];
  let size = 0;
  const push = (chunk) => {
    parts.push(chunk);
    size += chunk.length;
  };
  const obj = (id, ...body) => {
    offsets[id] = size;
    push(Buffer.from(`${id} 0 obj\n`));
    for (const chunk of body) push(typeof chunk === 'string' ? Buffer.from(chunk) : chunk);
    push(Buffer.from('\nendobj\n'));
  };
  const stream = (dict, data) => [`<< ${dict} /Length ${data.length} >>\nstream\n`, data, '\nendstream'];

  push(Buffer.from('%PDF-1.4\n'));
  const kids = Array.from({ length: pages }, (_, i) => `${4 + 3 * i} 0 R`).join(' ');
  obj(1, '<< /Type /Catalog /Pages 2 0 R >>');
  obj(2, `<< /Type /Pages /Count ${pages} /Kids [${kids}] >>`);
  for (let i = 0; i < pages; i++) {
    const image = 3 + 3 * i;
    obj(image, ...stream(`/Type /XObject /Subtype /Image /Width ${side} /Height ${side} /ColorSpace /DeviceGray /BitsPerComponent 8`, crypto.randomBytes(side * side)));
    obj(image + 1, `<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Resources << /XObject << /Im0 ${image} 0 R >> >> /Contents ${image + 2} 0 R >>`);
    obj(image + 2, ...stream('', Buffer.from('q 512 0 0 512 50 140 cm /Im0 Do Q')));
  }
  const count = 3 + 3 * pages;
  const xrefAt = size;
  let xref = `xref\n0 ${count}\n0000000000 65535 f \n`;
  for (let id = 1; id < count; id++) xref += `${String(offsets[id]).padStart(10, '0')} 00000 n \n`;
  push(Buffer.from(`${xref}trailer\n<< /Size ${count} /Root 1 0 R >>\nstartxref\n${xrefAt}\n%%EOF\n`));
  return Buffer.concat(parts);
}

const LARGE = makePdf(40, 640); // ~15.6 MiB
const SMALL = makePdf(3, 64);

/**
 * The download route, answering one `bytes=a-b` range as the backend does,
 * headers included: the app's API is another origin here, and a script reads
 * Content-Range only once it is exposed.
 */
const EXPOSE = { 'Access-Control-Expose-Headers': 'Content-Range, Accept-Ranges, ETag' };
function rangeRoute(bytes, state) {
  return async (route) => {
    const header = route.request().headers()['range'];
    const match = header && /^bytes=(\d+)-(\d*)$/.exec(header);
    // An outage that outlasts a slice's retries and is over by the reload.
    if (state.outage && header === OPENING_RANGE) state.outage = false;
    if (state.fail || state.outage) {
      state.log.push({ range: header ?? null, status: 500, bytes: 0 });
      return route.fulfill({ status: 500, body: '' });
    }
    if (!state.honor || !match) {
      state.log.push({ range: header ?? null, status: 200, bytes: bytes.length });
      return route.fulfill({ status: 200, contentType: 'application/pdf', body: bytes });
    }
    const first = Number(match[1]);
    if (first >= bytes.length) {
      return route.fulfill({ status: 416, headers: { ...EXPOSE, 'Content-Range': `bytes */${bytes.length}` } });
    }
    const last = Math.min(match[2] ? Number(match[2]) : bytes.length - 1, bytes.length - 1, first + SERVER_CAP - 1);
    const body = bytes.subarray(first, last + 1);
    state.log.push({ range: header, status: 206, bytes: body.length });
    return route.fulfill({
      status: 206,
      contentType: 'application/pdf',
      headers: {
        ...EXPOSE,
        'Content-Range': `bytes ${first}-${last}/${bytes.length}`,
        'Accept-Ranges': 'bytes',
        // A file rewritten between every two requests carries a new tag on each.
        ETag: state.churn ? `"v${state.log.length}"` : state.etag,
      },
      body,
    });
  };
}

async function openPdf(page, bytes, overrides = {}) {
  const state = { etag: '"v1"', honor: true, fail: false, outage: false, churn: false, log: [], ...overrides };
  await mockAPI(page, {
    ...chatViewOverrides(),
    [`GET /workspaces/${WS}/files`]: { files: [FILE] },
  });
  await page.route((url) => url.pathname.endsWith(`/workspaces/${WS}/files/download`), rangeRoute(bytes, state));
  await configureSSE({
    method: 'GET',
    path: `/api/v1/threads/${TH}/messages/replay`,
    events: [sseEvents.replayDone()],
    delay: 10,
  });
  await page.goto(`/chat/t/${TH}`);
  await page.waitForSelector('textarea', { timeout: 10000 });
  await page.locator('button[title="Workspace Files"]').click();
  // Dispatched rather than clicked, as in file-editor.spec.js: the tree
  // column's entry animation can leave the row off screen on a cold load.
  await page.getByRole('treeitem', { name: FILE }).dispatchEvent('click');
  return state;
}

/** Waits for a page's canvas to be drawn. */
async function drawn(page, index) {
  await expect.poll(() => page.evaluate((i) => {
    const canvas = document.querySelector(`[data-pdf-slot="${i}"] canvas`);
    return !!canvas && canvas.width > 0 && canvas.height > 0;
  }, index), { timeout: 30000 }).toBe(true);
}

/**
 * Waits for every page in the render window to be painted. A canvas is sized
 * before its page has the bytes it draws, so a page waiting on a lost slice
 * passes `drawn` with a blank canvas; its noise image never shows.
 */
async function windowPainted(page) {
  await expect.poll(() => page.evaluate(() => {
    const canvases = [...document.querySelectorAll('.pdf-page-surface canvas')];
    return canvases.length > 0 && canvases.every((canvas) => {
      if (!canvas.width) return false;
      const { data } = canvas.getContext('2d').getImageData(canvas.width / 2 - 5, canvas.height / 2 - 5, 10, 10);
      for (let p = 0; p < data.length; p += 4) if (data[p + 3] > 0 && data[p] < 250) return true;
      return false;
    });
  }), { timeout: 30000 }).toBe(true);
}

async function goToPage(page, n) {
  const field = page.getByLabel('Page number');
  await field.click();
  await field.fill(String(n));
  await field.press('Enter');
}

const fetched = (log) => log.reduce((sum, r) => sum + r.bytes, 0);

test.describe('pdf range loading', () => {
  test.beforeEach(async () => {
    await resetMockServer();
  });

  test('a large PDF opens from its first slice and reads the rest as pages need it', async ({ page }) => {
    const state = await openPdf(page, LARGE);
    await drawn(page, 0);
    expect(state.log[0]).toMatchObject({ range: OPENING_RANGE, status: 206 });
    expect(state.log.every((r) => r.status === 206)).toBe(true);
    // The first slice plus one small chunk around each page dict.
    expect(fetched(state.log)).toBeLessThan(LARGE.length / 2);

    const before = fetched(state.log);
    await goToPage(page, 30);
    await drawn(page, 29);
    await expect(page.getByLabel('Page number')).toHaveValue('30');
    expect(fetched(state.log)).toBeGreaterThan(before);
    expect(fetched(state.log)).toBeLessThan(LARGE.length);
  });

  test('a file that changes while it is read reloads in place', async ({ page }) => {
    const state = await openPdf(page, LARGE);
    await drawn(page, 0);
    state.etag = '"v2"';
    const before = state.log.length;
    await goToPage(page, 35);
    // A slice under "v2" does not belong to the open copy, so the file opens
    // again from its first slice and the reader stays on page 35.
    const reopened = () => state.log.slice(before).filter((r) => r.range === OPENING_RANGE).length;
    await expect.poll(reopened, { timeout: 15000 }).toBeGreaterThan(0);
    await drawn(page, 34);
    await expect(page.getByLabel('Page number')).toHaveValue('35');
    await expect(page.getByText('Unable to preview this file')).toHaveCount(0);
    // Every slice that failed asked for the same one re-read.
    expect(reopened()).toBe(1);
  });

  test('a file that never stops changing shows the preview error after one reload, and Try again once it settles', async ({ page }) => {
    const state = await openPdf(page, LARGE);
    await drawn(page, 0);
    state.churn = true;
    const before = state.log.length;
    await goToPage(page, 35);
    // The reloaded copy fails the same way before any slice of it reads, which
    // ends it there rather than reloading for as long as the file changes.
    await expect(page.getByText('Unable to preview this file')).toBeVisible({ timeout: 15000 });
    expect(state.log.slice(before).filter((r) => r.range === OPENING_RANGE).length).toBe(1);

    state.churn = false;
    await page.getByRole('button', { name: 'Try again' }).click();
    await drawn(page, 0);
    await expect(page.getByText('Unable to preview this file')).toHaveCount(0);
  });

  test('a server that ignores the range sends the whole file, which still shows', async ({ page }) => {
    const state = await openPdf(page, LARGE, { honor: false });
    await drawn(page, 0);
    await goToPage(page, 30);
    await drawn(page, 29);
    expect(state.log).toEqual([{ range: OPENING_RANGE, status: 200, bytes: LARGE.length }]);
  });

  test('a small PDF arrives whole in the one request', async ({ page }) => {
    const state = await openPdf(page, SMALL);
    await drawn(page, 0);
    await goToPage(page, 3);
    await drawn(page, 2);
    expect(state.log).toEqual([{ range: OPENING_RANGE, status: 206, bytes: SMALL.length }]);
  });

  test('a slice lost to an outage longer than its retries reloads the file in place', async ({ page }) => {
    const state = await openPdf(page, LARGE);
    await drawn(page, 0);
    state.outage = true;
    const before = state.log.length;
    await goToPage(page, 35);
    // The reload finds the same version, and still replaces the file: the
    // document over the old one waits on the lost slice for good.
    const reopened = () => state.log.slice(before).filter((r) => r.range === OPENING_RANGE).length;
    await expect.poll(reopened, { timeout: 15000 }).toBeGreaterThan(0);
    await drawn(page, 34);
    await windowPainted(page);
    await expect(page.getByLabel('Page number')).toHaveValue('35');
    await expect(page.getByText('Unable to preview this file')).toHaveCount(0);
    await expect(page.getByText('Failed to load file')).toHaveCount(0);
    expect(reopened()).toBe(1);
  });

  test('a slice that keeps failing reloads the file once, then offers a retry', async ({ page }) => {
    const state = await openPdf(page, LARGE);
    await drawn(page, 0);
    state.fail = true;
    const before = state.log.length;
    await goToPage(page, 38);
    // pdf.js never asks again for a slice that failed, so the file reloads
    // from its first slice; that failing too lands on the panel's error, which
    // has the Retry the viewer's own fallback lacks.
    await expect(page.getByText('Failed to load file')).toBeVisible({ timeout: 15000 });
    await expect(page.getByText('Unable to preview this file')).toHaveCount(0);
    // The open, then the one reload, asked three times: the first try and two retries.
    expect(state.log.filter((r) => r.range === OPENING_RANGE).map((r) => r.status)).toEqual([206, 500, 500, 500]);
    const slices = state.log.slice(before).filter((r) => r.range !== OPENING_RANGE);
    expect(slices.length).toBeGreaterThanOrEqual(3);
    expect(slices.every((r) => r.status === 500)).toBe(true);

    state.fail = false;
    await page.getByRole('button', { name: 'Try again' }).click();
    await drawn(page, 0);
  });
});
