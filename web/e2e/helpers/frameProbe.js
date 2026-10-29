/**
 * A per-frame sampler for the specs that pin what the reader saw frame by
 * frame (a row that must not move, a fade that must not start early), which an
 * assertion on where things ended up cannot catch.
 */

/**
 * Starts reading `read` once per animation frame, until `stopSampling`. `read`
 * runs in the page: it is serialized with `toString()` and rebuilt there, so it
 * cannot close over anything in Node, and what it needs goes in `arg` (anything
 * Playwright can serialize) and arrives as its one parameter. Each sample is
 * what `read` returned, an object, stamped with `t`, the milliseconds since
 * this call.
 */
export async function sampleFrames(page, read, arg) {
  await page.evaluate(({ source, arg }) => {
    const readFrame = new Function('arg', `return (${source})(arg);`);
    const samples = [];
    const start = performance.now();
    window.__samples = samples;
    window.__sampling = true;
    const tick = () => {
      if (!window.__sampling) return;
      samples.push({ t: performance.now() - start, ...readFrame(arg) });
      requestAnimationFrame(tick);
    };
    requestAnimationFrame(tick);
  }, { source: read.toString(), arg });
}

/** Stops the sampler and returns every sample it took, in frame order. */
export const stopSampling = (page) => page.evaluate(() => {
  window.__sampling = false;
  return window.__samples;
});
