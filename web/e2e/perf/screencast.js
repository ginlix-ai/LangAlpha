/**
 * Compositor-frame capture: the ground truth for "was this ever on screen".
 *
 * The in-page probe samples from a task queued off the frame, so a commit that
 * lands between the paint and that task reads as if it had been painted. A
 * screencast frame cannot lie about it. The transcript's first message wears a
 * magenta band, so counting magenta pixels per frame says whether that frame
 * showed the top of the transcript.
 */

/** Paints a band at the top of the transcript, above the fold only while scrolled up. */
export const MAGENTA_MARKER_CSS = 'main [data-message-id]:first-of-type::before{content:"";display:block;height:30px;background:#ff00ff}';

/**
 * Record compositor frames until `until()` resolves, plus a tail. Always stops
 * the screencast and detaches the session, so a failing `until` cannot leave
 * the browser encoding JPEGs for the rest of the run.
 */
export async function recordUntil(page, until, { tailMs = 600, quality = 60, maxWidth = 480, maxHeight = 360 } = {}) {
  const frames = [];
  const cdp = await page.context().newCDPSession(page);
  const onFrame = (f) => {
    frames.push({ ts: f.metadata.timestamp, data: f.data });
    cdp.send('Page.screencastFrameAck', { sessionId: f.sessionId }).catch(() => {});
  };
  cdp.on('Page.screencastFrame', onFrame);
  try {
    await cdp.send('Page.startScreencast', { format: 'jpeg', quality, everyNthFrame: 1, maxWidth, maxHeight });
    await until();
    await page.waitForTimeout(tailMs);
  } finally {
    await cdp.send('Page.stopScreencast').catch(() => {});
    cdp.off('Page.screencastFrame', onFrame);
    await cdp.detach().catch(() => {});
  }
  return frames;
}

/** Magenta pixel count per frame, decoded in the page (Node has no image decoder). */
export async function countMagenta(page, datas) {
  return page.evaluate(async (ds) => {
    const out = [];
    const canvas = document.createElement('canvas');
    const ctx = canvas.getContext('2d', { willReadFrequently: true });
    for (const d of ds) {
      const img = new Image();
      img.src = 'data:image/jpeg;base64,' + d;
      await img.decode();
      canvas.width = img.width; canvas.height = img.height;
      ctx.drawImage(img, 0, 0);
      const px = ctx.getImageData(0, 0, img.width, img.height).data;
      let magenta = 0;
      for (let i = 0; i < px.length; i += 4) if (px[i] > 180 && px[i + 1] < 100 && px[i + 2] > 180) magenta++;
      out.push(magenta);
    }
    return out;
  }, datas);
}
