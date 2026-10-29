/**
 * In-page smoothness probe, injected before the app loads.
 *
 * Three independent signals, so a change that helps one cannot hide behind
 * another:
 *  - frame gaps from a requestAnimationFrame loop (what the eye sees)
 *  - Long Animation Frames from PerformanceObserver (why a frame was late)
 *  - DOM mutation churn on the transcript (how much the renderer touches)
 *
 * Optionally, per start: component renders by name, and the DOM churn of one
 * region of the page (the file panel), counted apart from the transcript's.
 */
export function installSmoothProbe() {
  // Declared in here: the function is serialised into the page, so a helper
  // outside it would not exist there.
  const zeroChurn = () => ({ mutations: 0, nodesAdded: 0, nodesRemoved: 0, charDataChanges: 0, attrChanges: 0 });
  const S = (window.__smooth = {
    running: false, t0: 0, t1: 0,
    frameGaps: [], loaf: [], longTasks: [],
    ...zeroChurn(),
    commits: 0,
    countRenders: false, scope: null, renders: {}, scopeRenders: {},
    region: zeroChurn(),
  });

  // Which components a commit rendered, read off its fiber tree the way React
  // DevTools reads it: a component rendered if it mounted or carries
  // PerformedWork, and a child list the commit left as it was is the previous
  // commit's, so nothing under it rendered and the walk stops there. Names are
  // the source names in a dev or unminified build and mangled in a minified one.
  const PERFORMED_WORK = 1;
  // Function, class, forwardRef, memo and simple memo components.
  const COMPONENT_TAGS = new Set([0, 1, 11, 14, 15]);
  const HOST_COMPONENT = 5;
  const nameOf = (fiber) => {
    let type = fiber.type;
    if (type && typeof type === 'object') type = type.type || type.render;
    return (type && (type.displayName || type.name)) || '(anonymous)';
  };
  const rendered = (fiber, prev) => COMPONENT_TAGS.has(fiber.tag) && (!prev || (fiber.flags & PERFORMED_WORK) !== 0);
  const visit = (next, prev, inScope) => {
    const scopeRoot = !inScope && next.tag === HOST_COMPONENT && !!S.scope && !!next.stateNode?.matches?.(S.scope);
    const scoped = inScope || scopeRoot;
    if (rendered(next, prev)) {
      const name = nameOf(next);
      S.renders[name] = (S.renders[name] || 0) + 1;
      if (scoped) S.scopeRenders[name] = (S.scopeRenders[name] || 0) + 1;
    }
    // The component that renders the scope's element (FilePanel for
    // .file-panel) counts as part of it: it was visited on the way down.
    if (scopeRoot) {
      let owner = next.return;
      while (owner && !COMPONENT_TAGS.has(owner.tag)) owner = owner.return;
      if (owner && rendered(owner, owner.alternate)) {
        const name = nameOf(owner);
        S.scopeRenders[name] = (S.scopeRenders[name] || 0) + 1;
      }
    }
    if (prev && next.child === prev.child) return;
    for (let child = next.child; child; child = child.sibling) visit(child, child.alternate, scoped);
  };

  // React commits, through the DevTools hook React calls on every commit in
  // production builds too (without a priority there, so no lane split).
  if (!window.__REACT_DEVTOOLS_GLOBAL_HOOK__) {
    window.__REACT_DEVTOOLS_GLOBAL_HOOK__ = {
      supportsFiber: true, renderers: new Map(),
      inject() { return 1; },
      checkDCE() {},
      onCommitFiberRoot(_id, root) {
        if (!S.running) return;
        S.commits += 1;
        if (S.countRenders) visit(root.current, root.current.alternate, false);
      },
      onCommitFiberUnmount() {},
      onPostCommitFiberRoot() {},
      setStrictMode() {},
    };
  }
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

  const churn = (into) => (records) => {
    into.mutations += records.length;
    for (const r of records) {
      into.nodesAdded += r.addedNodes.length;
      into.nodesRemoved += r.removedNodes.length;
      if (r.type === 'characterData') into.charDataChanges += 1;
      // Counted apart from the node churn: an attribute write is a class or
      // style flip, which costs style recalc but no reconciliation.
      if (r.type === 'attributes') into.attrChanges += 1;
    }
  };
  const OBSERVE = { childList: true, subtree: true, characterData: true, attributes: true };

  let mo = null;
  let regionMo = null;
  /**
   * `scope` is a selector for one region of the page: its DOM churn is
   * counted on its own, and with `renders` so are the components under it.
   * `renders` walks every commit's tree, which costs main-thread time, so a
   * run that counts renders is not a run to read frame timings from.
   */
  S.start = (root, { scope = null, renders = false } = {}) => {
    // PERF_PANEL=tool starts the probe again once the tab is open, and an
    // observer left connected would count every mutation a second time.
    mo?.disconnect();
    regionMo?.disconnect();
    S.frameGaps = []; S.loaf = []; S.longTasks = [];
    S.commits = 0;
    Object.assign(S, zeroChurn());
    mo = new MutationObserver(churn(S));
    mo.observe(root || document.body, OBSERVE);
    S.scope = scope; S.countRenders = renders; S.renders = {}; S.scopeRenders = {};
    S.region = zeroChurn();
    const regionRoot = scope && document.querySelector(scope);
    if (regionRoot) {
      regionMo = new MutationObserver(churn(S.region));
      regionMo.observe(regionRoot, OBSERVE);
    }
    S.t0 = performance.now(); S.running = true;
  };
  S.stop = () => {
    S.running = false; S.t1 = performance.now();
    if (mo) mo.disconnect();
    if (regionMo) regionMo.disconnect();
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
      commits: S.commits,
      ...(S.scope ? { region: { scope: S.scope, ...S.region } } : {}),
      ...(S.countRenders ? { renders: S.renders, scopeRenders: S.scopeRenders } : {}),
    };
  };
}
