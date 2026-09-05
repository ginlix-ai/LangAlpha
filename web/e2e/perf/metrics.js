/**
 * In-page smoothness probe, injected before the app loads.
 *
 * Three independent signals, so a change that helps one cannot hide behind
 * another:
 *  - frame gaps from a requestAnimationFrame loop (what the eye sees)
 *  - Long Animation Frames from PerformanceObserver (why a frame was late)
 *  - DOM mutation churn on the transcript (how much the renderer touches)
 */
export function installSmoothProbe() {
  const S = (window.__smooth = {
    running: false, t0: 0, t1: 0,
    frameGaps: [], loaf: [], longTasks: [],
    mutations: 0, nodesAdded: 0, nodesRemoved: 0, charDataChanges: 0, attrChanges: 0,
  });
  let last = 0;
  function loop(ts) {
    if (S.running) {
      if (last) S.frameGaps.push(ts - last);
      last = ts;
    } else {
      last = 0;
    }
    requestAnimationFrame(loop);
  }
  requestAnimationFrame(loop);

  try {
    new PerformanceObserver((list) => {
      if (!S.running) return;
      for (const e of list.getEntries()) {
        const scripts = (e.scripts || []).map((s) => ({
          src: s.sourceURL || s.invoker || '', fn: s.sourceFunctionName || '', dur: Math.round(s.duration),
        })).sort((a, b) => b.dur - a.dur).slice(0, 3);
        S.loaf.push({ start: e.startTime, dur: e.duration, block: e.blockingDuration, styleLayout: (e.styleAndLayoutStart ? e.startTime + e.duration - e.styleAndLayoutStart : 0), scripts });
      }
    }).observe({ type: 'long-animation-frame', buffered: false });
  } catch { /* entry type unsupported in this browser */ }
  try {
    new PerformanceObserver((list) => {
      if (!S.running) return;
      for (const e of list.getEntries()) S.longTasks.push(e.duration);
    }).observe({ type: 'longtask', buffered: false });
  } catch { /* entry type unsupported in this browser */ }

  let mo = null;
  S.start = (root) => {
    S.frameGaps = []; S.loaf = []; S.longTasks = [];
    S.mutations = 0; S.nodesAdded = 0; S.nodesRemoved = 0; S.charDataChanges = 0; S.attrChanges = 0;
    mo = new MutationObserver((records) => {
      S.mutations += records.length;
      for (const r of records) {
        S.nodesAdded += r.addedNodes.length;
        S.nodesRemoved += r.removedNodes.length;
        if (r.type === 'characterData') S.charDataChanges += 1;
        // Counted apart from the node churn: an attribute write is a class or
        // style flip, which costs style recalc but no reconciliation.
        if (r.type === 'attributes') S.attrChanges += 1;
      }
    });
    mo.observe(root || document.body, { childList: true, subtree: true, characterData: true, attributes: true });
    S.t0 = performance.now(); S.running = true;
  };
  S.stop = () => {
    S.running = false; S.t1 = performance.now();
    if (mo) mo.disconnect();
    const gaps = S.frameGaps.slice().sort((a, b) => a - b);
    const q = (p) => (gaps.length ? gaps[Math.min(gaps.length - 1, Math.floor(p * gaps.length))] : 0);
    const durationMs = S.t1 - S.t0;
    const over = (ms) => S.frameGaps.filter((g) => g > ms).length;
    return {
      durationMs: Math.round(durationMs),
      frames: gaps.length,
      fps: +(gaps.length / (durationMs / 1000)).toFixed(1),
      gapP50: +q(0.5).toFixed(1), gapP95: +q(0.95).toFixed(1), gapP99: +q(0.99).toFixed(1),
      gapMax: +(gaps[gaps.length - 1] || 0).toFixed(1),
      framesOver33: over(33), framesOver50: over(50), framesOver100: over(100),
      // Time the user spent looking at a frozen frame: sum of gap beyond 16.7ms.
      frozenMs: Math.round(S.frameGaps.reduce((s, g) => s + Math.max(0, g - 16.7), 0)),
      loafCount: S.loaf.length,
      loafTotalMs: Math.round(S.loaf.reduce((s, e) => s + e.dur, 0)),
      loafBlockingMs: Math.round(S.loaf.reduce((s, e) => s + e.block, 0)),
      loafMaxMs: Math.round(S.loaf.reduce((m, e) => Math.max(m, e.dur), 0)),
      loafTop: S.loaf.slice().sort((a, b) => b.dur - a.dur).slice(0, 5).map((e) => ({ at: Math.round(e.start - S.t0), dur: Math.round(e.dur), styleLayout: Math.round(e.styleLayout), scripts: e.scripts })),
      longTasks: S.longTasks.length,
      longTaskMaxMs: Math.round(S.longTasks.reduce((m, d) => Math.max(m, d), 0)),
      mutations: S.mutations, nodesAdded: S.nodesAdded, nodesRemoved: S.nodesRemoved,
      charDataChanges: S.charDataChanges, attrChanges: S.attrChanges,
    };
  };
}
