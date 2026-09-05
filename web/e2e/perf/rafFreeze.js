/**
 * Stand-in for a hidden tab: park every rAF callback until thawed, then run
 * the whole queue on one frame. A headless tab never goes hidden, so this is
 * the only way to reproduce "state kept arriving while nothing painted"
 * without a real display.
 *
 * Install it before any other init script that wants to keep ticking through
 * the freeze: `window.__raf.native` is the unpatched requestAnimationFrame.
 */
export function installRafFreeze() {
  const nativeRaf = window.requestAnimationFrame.bind(window);
  const nativeCaf = window.cancelAnimationFrame.bind(window);
  const R = (window.__raf = { frozen: false, queue: [], nextId: -1, native: nativeRaf });
  window.requestAnimationFrame = (cb) => {
    if (!R.frozen) return nativeRaf(cb);
    const id = R.nextId--;
    R.queue.push({ id, cb });
    return id;
  };
  window.cancelAnimationFrame = (id) => {
    if (id < 0) { R.queue = R.queue.filter((q) => q.id !== id); return; }
    nativeCaf(id);
  };
  R.freeze = () => { R.frozen = true; };
  R.thaw = () => {
    R.frozen = false;
    const q = R.queue; R.queue = [];
    nativeRaf((ts) => { for (const { cb } of q) { try { cb(ts); } catch (e) { console.error(e); } } });
  };
}
