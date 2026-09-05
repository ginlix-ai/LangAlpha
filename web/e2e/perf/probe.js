/**
 * In-page transcript sampler, injected before the app loads.
 *
 * Per frame it records the transcript's text length and the scroller's
 * position, which is enough to name what the eye would have caught: a "burst
 * frame" gains far more text than the cadence or the typewriter can produce, a
 * "drop frame" loses text (content replaced, or duplicated and collapsed), a
 * "scroll jump" moves more than 300 px at once.
 *
 * Options, all off by default because each costs per-frame work the benchmark
 * would otherwise be measuring:
 *  - `live`: whether the composer still shows the stop control, which stamps
 *    when the model finished without asking the client.
 *  - `painted`: a second read from a task queued off the frame, so a layout or
 *    resize-observer scroll that lands after the commit is included.
 *  - `scrollLog`: every programmatic scroll with its caller.
 *  - `reply`: the text length of the message bubbles alone, so a drop can be
 *    told apart from page chrome (a route unmounting, a status label) that
 *    shares the transcript's container.
 */
export function installProbe(options) {
  const opts = options || {};
  const P = (window.__probe = {
    painted: [], samples: [], shifts: [], marks: {}, scrolls: [],
    // An epoch time base, not a performance.now() mark: the screencast frames
    // it is compared against are timestamped on the same clock.
    catchUpWall: 0,
  });
  const nativeRaf = (window.__raf && window.__raf.native) || window.requestAnimationFrame.bind(window);
  const ids = new WeakMap(); let nid = 0;
  const idOf = (el) => { if (!ids.has(el)) ids.set(el, ++nid); return ids.get(el); };
  P.idOf = idOf;

  if (opts.scrollLog) try {
    const caller = () => String(new Error().stack).split('\n').slice(2, 12).map((l) => l.trim().slice(-48));
    const d = Object.getOwnPropertyDescriptor(Element.prototype, 'scrollTop');
    Object.defineProperty(Element.prototype, 'scrollTop', {
      get: d.get,
      set(v) { P.scrolls.push({ t: performance.now(), el: idOf(this), top: v, via: 'set', st: caller() }); d.set.call(this, v); },
      configurable: true,
    });
    const orig = Element.prototype.scrollTo;
    Element.prototype.scrollTo = function (...a) {
      const o = a[0];
      P.scrolls.push({ t: performance.now(), el: idOf(this), top: o && typeof o === 'object' ? o.top : a[1], beh: o && typeof o === 'object' ? o.behavior : 'auto', via: 'scrollTo', st: caller() });
      return orig.apply(this, a);
    };
  } catch { /* entry type unsupported in this browser */ }

  let scroller = null;
  function findScroller() {
    if (scroller && scroller.isConnected) return scroller;
    const main = document.querySelector('main') || document.body;
    for (const el of main.querySelectorAll('*')) {
      const oy = getComputedStyle(el).overflowY;
      if ((oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight + 50) { scroller = el; return el; }
    }
    return null;
  }

  /** Elements holding an implausible amount of text, to name a block that appeared or vanished. */
  function bigTextNodes() {
    const main = document.querySelector('main') || document.body;
    const big = [];
    for (const el of main.querySelectorAll('*')) {
      if (el.children.length === 0 && el.textContent.length > 5000) {
        big.push(el.tagName + '.' + String(el.className).slice(0, 60) + ':' + el.textContent.length + ':' + el.textContent.slice(0, 80));
      }
    }
    return big.slice(0, 5).join(' | ');
  }

  function sample(ts) {
    const main = document.querySelector('main') || document.body;
    const s = findScroller();
    const len = main.textContent.length;
    const prev = P.samples[P.samples.length - 1];
    if (prev && len < 50000 && prev.len < 50000 && len - prev.len > 1000 && !P.catchUpWall) P.catchUpWall = Date.now();
    const rec = { t: ts, len, top: s ? s.scrollTop : -1, h: s ? s.scrollHeight - s.clientHeight : -1 };
    if (opts.live) rec.live = !!document.querySelector('button[aria-label="Stop"]');
    if (opts.reply) { let n = 0; for (const b of document.querySelectorAll('[data-message-id]')) n += b.textContent.length; rec.reply = n; }
    const jumped = !!prev && Math.abs(len - prev.len) > 5000;
    P.samples.push(rec);
    if (opts.painted || jumped) {
      // A task queued from rAF runs after this frame is painted, and a layout or
      // resize-observer scroll lands between the two, so this is what the screen
      // showed. The whole-DOM scan for a jump rides the same task: in the frame
      // callback it would be work the frame gaps then blame on the app.
      setTimeout(() => {
        if (jumped) rec.note = bigTextNodes();
        if (!opts.painted) return;
        const sc = findScroller();
        const m = document.querySelector('main') || document.body;
        P.painted.push({ t: performance.now(), len: m.textContent.length, el: sc ? idOf(sc) : 0, cr: sc ? idOf(sc.querySelector('.max-w-3xl') || sc.firstElementChild || sc) : 0, top: sc ? sc.scrollTop : -1, h: sc ? sc.scrollHeight - sc.clientHeight : -1 });
      }, 0);
    }
    nativeRaf(sample);
  }
  nativeRaf(sample);

  try {
    new PerformanceObserver((list) => {
      for (const e of list.getEntries()) if (!e.hadRecentInput) P.shifts.push({ t: e.startTime, v: e.value, src: (e.sources || []).slice(0, 4).map((s) => {
        const n = s.node; const tag = n ? (n.tagName || '').toLowerCase() + (n.className && typeof n.className === 'string' ? '.' + n.className.split(' ').slice(0, 3).join('.') : '') : '?';
        const r = (q) => q ? [Math.round(q.x), Math.round(q.y), Math.round(q.width), Math.round(q.height)].join(',') : '-';
        return tag.slice(0, 60) + ' ' + r(s.previousRect) + ' -> ' + r(s.currentRect);
      }) });
    }).observe({ type: 'layout-shift', buffered: true });
  } catch { /* entry type unsupported in this browser */ }

  P.mark = (name) => { P.marks[name] = performance.now(); };
  /** Frames since a mark, as the tuples the typewriter analysis reads. */
  P.framesSince = (since) => {
    const t0 = P.marks[since] ?? 0;
    return P.samples.filter((x) => x.t >= t0).map((x) => [x.t, x.len, x.live ? 1 : 0, x.top, x.h, x.reply === undefined ? -1 : x.reply]);
  };
  // Poll the transcript's visible text and keep the snapshot around the
  // largest drop, so a vanished block can be named rather than sized.
  P.watchDrops = (ms) => {
    const main = document.querySelector('main') || document.body;
    let prev = main.innerText; let worst = 0; P.drop = null;
    const iv = setInterval(() => {
      const cur = main.innerText;
      if (prev.length - cur.length > worst) {
        worst = prev.length - cur.length;
        const before = new Set(cur.split('\n'));
        P.drop = { t: Math.round(performance.now()), size: worst, removed: prev.split('\n').filter((l) => l.trim() && !before.has(l)).slice(0, 12) };
      }
      prev = cur;
    }, 100);
    setTimeout(() => clearInterval(iv), ms);
  };
  P.report = (since) => {
    const t0 = P.marks[since] ?? 0;
    const xs = P.samples.filter((x) => x.t >= t0);
    let bursts = 0, burstChars = 0, maxGain = 0, drops = 0, maxDrop = 0, replyDrops = 0, maxReplyDrop = 0, jumps = 0, maxJump = 0, lastBurstAt = t0, awayFromBottomFrames = 0;
    for (let i = 1; i < xs.length; i++) {
      const d = xs[i].len - xs[i - 1].len;
      if (d > 20) { bursts++; burstChars += d; lastBurstAt = xs[i].t; }
      if (d > maxGain) maxGain = d;
      if (d < 0) { drops++; if (-d > maxDrop) maxDrop = -d; }
      const rd = xs[i].reply === undefined ? 0 : xs[i].reply - xs[i - 1].reply;
      if (rd < 0) { replyDrops++; if (-rd > maxReplyDrop) maxReplyDrop = -rd; }
      const j = Math.abs(xs[i].top - xs[i - 1].top);
      if (xs[i].top >= 0 && j > 300) { jumps++; if (j > maxJump) maxJump = j; }
      if (xs[i].h > 0 && xs[i].h - xs[i].top > 400) awayFromBottomFrames++;
    }
    const cls = P.shifts.filter((s) => s.t >= t0).reduce((a, s) => a + s.v, 0);
    const first = xs[0];
    const gapAtResume = first && first.h > 0 ? Math.round(first.h - first.top) : -1;
    let settleAt = -1, scrollFrames = 0;
    for (let i = 1; i < xs.length; i++) {
      if (xs[i].top !== xs[i - 1].top) scrollFrames++;
      if (settleAt < 0 && xs[i].h > 0 && xs[i].h - xs[i].top < 50) settleAt = xs[i].t;
    }
    // Text growth per 500 ms bucket for the first 12 s: the cadence delivers
    // ~100 chars per bucket, so a bucket far above that is catch-up.
    const series = [];
    for (let b = 0; b < 24; b++) {
      const a = xs.filter((x) => x.t >= t0 + b * 500 && x.t < t0 + (b + 1) * 500);
      series.push(a.length ? a[a.length - 1].len - a[0].len : 0);
    }
    const notable = [];
    for (let i = 1; i < xs.length; i++) {
      const d = xs[i].len - xs[i - 1].len; const j = xs[i].top - xs[i - 1].top;
      if (Math.abs(d) > 60 || Math.abs(j) > 300 || xs[i].note) notable.push({ t: Math.round(xs[i].t - t0), d, top: xs[i].top, j: Math.round(j), note: xs[i].note });
    }
    const bigShifts = P.shifts.filter((s) => s.t >= t0 && s.v > 0.02).map((s) => ({ t: Math.round(s.t - t0), v: +s.v.toFixed(3), src: s.src }));
    // Painted frames, after the transcript first appears, that showed the
    // scroller more than 400 px short of the bottom: what the eye saw at the top.
    const ps = P.painted.filter((x) => x.t >= t0);
    const firstShown = ps.findIndex((x, i) => i > 0 && x.len - ps[i - 1].len > 500);
    // A hint, not ground truth: the post-paint sample runs in a task, and a
    // commit that lands between the paint and that task is read here as if it
    // had been painted at scrollTop 0. PERF_CAST=1 records compositor frames
    // and is what decides whether a frame was really shown away from the bottom.
    const paintedAwayFrames = firstShown < 0 ? -1 : ps.slice(firstShown).filter((x) => x.h > 0 && x.h - x.top > 400).length;
    return {
      scrolls: P.scrolls.filter((x) => x.t >= t0).map((x) => ({ ...x, t: Math.round(x.t - t0) })).slice(0, 16), paintedPrev: (() => { const i = ps.findIndex((x) => x.h > 0 && x.h - x.top > 400); return i > 0 ? ps.slice(Math.max(0, i - 3), i + 2).map((x) => [Math.round(x.t - t0), x.len, x.el, x.cr, x.top, x.h]) : []; })(),
      paintedAway: firstShown < 0 ? [] : ps.slice(firstShown).filter((x) => x.h > 0 && x.h - x.top > 400).slice(0, 5).map((x) => [Math.round(x.t - t0), x.len, x.el, x.cr, x.top, x.h]),
      paintedAwayFrames, paintedFirstTop: firstShown < 0 ? -1 : ps[firstShown].top, paintedFirstGap: firstShown < 0 ? -1 : ps[firstShown].h - ps[firstShown].top,
      frames: xs.length, burstFrames: bursts, burstChars, maxGainPerFrame: maxGain,
      catchUpMs: Math.round(lastBurstAt - t0), dropFrames: drops, maxDrop, replyDropFrames: replyDrops, maxReplyDrop,
      scrollJumps: jumps, maxScrollJumpPx: Math.round(maxJump), awayFromBottomFrames,
      cls: +cls.toFixed(3), shifts: P.shifts.filter((s) => s.t >= t0).length,
      textLen: xs.length ? xs[xs.length - 1].len : 0,
      growthPer500ms: series, gapAtResumePx: gapAtResume, scrollSettleMs: settleAt < 0 ? -1 : Math.round(settleAt - t0), scrollFrames,
      notable: notable.slice(0, 40), bigShifts: bigShifts.slice(0, 20),
    };
  };
}
