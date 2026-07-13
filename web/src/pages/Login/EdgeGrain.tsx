import { useEffect, useRef } from 'react';
import { derivePalette, hash } from './loginPaper';

/** One grain dot seeded by an ember-ball flight (see MarketScanlines). */
interface Seed {
  fu: number; // position along the density ramp: 0 = ramp start, 1 = right edge
  fy: number; // vertical position as a fraction of the frame height
  r: number; // per-seed random driving its final size and alpha
  born: number; // performance.now() at landing
}

const SEED_FADE_MS = 2600; // ember -> plain ink transition length
const MAX_SEEDS = 200; // plenty for a whole session of play

/**
 * EdgeGrain - the page's only background texture: a dot field spanning the
 * whole frame, densest against the frame's right border and thinning right
 * to left until it dies just past the pane seam. Printed in the page's ink,
 * derived from the page at runtime exactly like the chart palette (see
 * MarketScanlines.setColors). The static field is baked to an offscreen
 * layer, rebuilt on resize and theme flips.
 *
 * The one live behavior: every ember-ball burst in the chart dispatches
 * 'login:ember-seed' (it bubbles up to the frame), and each event seeds one
 * permanent dot at a random spot in the field - born in the ball's ember
 * color, cooling into ordinary ink over a few seconds. The canvas animates
 * only while a seed is still cooling; otherwise it stays still. Takes no
 * pointer events.
 */
function EdgeGrain() {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;
    // Grain is stochastic noise — rendering it at device resolution doubles
    // the backing-store memory for no visible gain. Always bake at 1x.
    const dpr = 1;

    let width = 1;
    let height = 1;
    let x0 = 0; // ramp zero point, set from the auth pane in rebuild()
    let inkRgb: [number, number, number] = [255, 255, 255];
    let emberRgb: [number, number, number]; // set from --login-ember in rebuild()
    let aBoost = 1;
    let base: HTMLCanvasElement | null = null;
    const seeds: Seed[] = [];
    let raf = 0;
    let rebuildRaf = 0; // pending coalesced bake (separate from the seed-cooling raf)
    // Last size + derived palette, cached so a bake with neither changed (a
    // resize event mid-drag, or an app-state class churn on <html>) is skipped.
    let lastSizeKey = '';
    let lastPaletteKey = '';
    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;

    // One grain dot: a size-bucketed square in `color` at (x, y), its alpha the
    // field's density curve - fainter far from the right edge, denser near it.
    // Shared by the baked field and the cooling seeds so both read identically;
    // the settle `e` is 1 for the settled field and < 1 while a seed still burns
    // (0.9 -> finalA blend, so e = 1 collapses to plain finalA). Size buckets
    // differ between the two call sites, so `sz` is passed in.
    const drawGrainDot = (
      g: CanvasRenderingContext2D,
      x: number,
      y: number,
      sz: number,
      r: number,
      d: number,
      color: string,
      e = 1
    ) => {
      const finalA = (0.04 + 0.06 * r) * (1 + 1.6 * d) * aBoost;
      g.globalAlpha = 0.9 + (finalA - 0.9) * e;
      g.fillStyle = color;
      g.fillRect(x, y, sz, sz);
    };

    // Paint the visible canvas: the baked field plus every seed. Returns
    // whether any seed is still cooling and needs another frame.
    const composite = (): boolean => {
      ctx.clearRect(0, 0, width, height);
      if (base) ctx.drawImage(base, 0, 0, width, height);
      const now = performance.now();
      let hot = false;
      for (const s of seeds) {
        const t = Math.min(1, (now - s.born) / SEED_FADE_MS);
        if (t < 1) hot = true;
        const e = t * t * (3 - 2 * t); // hold the ember early, settle late
        const d = s.fu ** 1.8; // same density read as the field's ramp
        const x = Math.round(x0 + s.fu * (width - x0));
        const y = Math.round(s.fy * height);
        const sz = s.r < 0.12 ? 3 : s.r < 0.4 ? 2 : 1;
        if (t < 1) {
          // Landing bloom: an ember-colored halo that burns off as it cools.
          ctx.globalAlpha = 0.3 * (1 - e);
          ctx.fillStyle = `rgb(${emberRgb[0]}, ${emberRgb[1]}, ${emberRgb[2]})`;
          ctx.fillRect(x - 2, y - 2, sz + 4, sz + 4);
        }
        // Color eases from the ember toward plain ink as the seed settles.
        const cr = Math.round(emberRgb[0] + (inkRgb[0] - emberRgb[0]) * e);
        const cg = Math.round(emberRgb[1] + (inkRgb[1] - emberRgb[1]) * e);
        const cb = Math.round(emberRgb[2] + (inkRgb[2] - emberRgb[2]) * e);
        drawGrainDot(ctx, x, y, sz, s.r, d, `rgb(${cr}, ${cg}, ${cb})`, e);
      }
      ctx.globalAlpha = 1;
      return hot;
    };

    const step = () => {
      raf = 0;
      if (composite()) raf = requestAnimationFrame(step);
    };
    const kick = () => {
      if (raf) cancelAnimationFrame(raf);
      raf = requestAnimationFrame(step);
    };

    // Bake the static field to the offscreen layer and repaint — the heavy
    // per-pixel loop lives here, scheduled through rebuild() below.
    const bake = () => {
      rebuildRaf = 0;
      const rect = canvas.getBoundingClientRect();
      const nextW = Math.max(1, Math.round(rect.width));
      const nextH = Math.max(1, Math.round(rect.height));

      // Palette from the page (see loginPaper.derivePalette): the field prints
      // in ink, seeds land in --login-ember and cool to ink.
      const pal = derivePalette(canvas);
      const sizeKey = `${nextW}x${nextH}`;
      const paletteKey = `${pal.ink.join(', ')}|${pal.ember.join(', ')}|${pal.aBoost}`;
      // Neither the size nor the palette moved (e.g. an app-state class churn on
      // <html>) — skip the expensive re-bake entirely.
      if (sizeKey === lastSizeKey && paletteKey === lastPaletteKey) return;
      lastSizeKey = sizeKey;
      lastPaletteKey = paletteKey;

      width = nextW;
      height = nextH;
      canvas.width = width * dpr;
      canvas.height = height * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

      // Re-read here so a theme flip repaints the whole field in the new palette.
      inkRgb = pal.ink;
      emberRgb = pal.ember;
      aBoost = pal.aBoost;

      // The ramp's zero point sits a little left of the auth column, so the
      // fade bleeds over the chart's edge instead of stopping at the seam.
      // When the visual pane is hidden (narrow layouts) the auth pane starts
      // at the frame's left edge and the ramp simply spans the whole frame.
      const pane = canvas.parentElement?.querySelector('.login-page__auth-pane');
      const paneLeft = pane ? pane.getBoundingClientRect().left - rect.left : width * 0.5;
      // Clamped inside the canvas so a degenerate layout (styles not applied
      // yet) can never flip the ramp's direction.
      x0 = Math.min(Math.max(0, paneLeft - 120), width - 8);

      base = base ?? document.createElement('canvas');
      base.width = width * dpr;
      base.height = height * dpr;
      const bctx = base.getContext('2d');
      if (!bctx) return;
      bctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      bctx.clearRect(0, 0, width, height);
      const fieldColor = `rgb(${inkRgb[0]}, ${inkRgb[1]}, ${inkRgb[2]})`;
      for (let cy = 0; cy < height; cy += 4) {
        const wob = 26 * Math.sin(cy * 0.011) + 14 * Math.sin(cy * 0.037 + 2);
        for (let cx = 0; cx < width; cx += 3) {
          const u = (cx - x0 - wob) / (width - x0);
          if (u <= 0) continue;
          const d = Math.min(1, u) ** 1.8; // read right to left: many -> few
          const h = hash(cx * 0.37, cy * 0.61);
          if (h < 0.38 * d) {
            const sz = h < 0.02 * d ? 3 : h < 0.09 * d ? 2 : 1;
            drawGrainDot(bctx, cx, cy, sz, hash(cx * 0.53, cy * 0.29), d, fieldColor);
          }
        }
      }
      bctx.globalAlpha = 1;
      if (composite()) kick();
    };

    // Coalesce rebuilds: a resize drag emits many events per frame, so schedule
    // the bake for the next frame and cancel any already-pending one — at most
    // one bake per frame. Runs on mount, resize, and theme flips.
    const rebuild = () => {
      if (rebuildRaf) cancelAnimationFrame(rebuildRaf);
      rebuildRaf = requestAnimationFrame(bake);
    };

    // One burst, one seed. Position is rejection-sampled against the ramp's
    // own density curve, so seeds land where grain plausibly lives - mostly
    // near the right edge, rarely out by the seam.
    const onSeed = () => {
      let fu = Math.random();
      for (let i = 0; i < 12 && Math.random() > fu ** 1.8; i++) fu = Math.random();
      if (seeds.length >= MAX_SEEDS) seeds.shift();
      // Under reduced motion the seed lands already settled: backdating `born`
      // past SEED_FADE_MS makes composite() paint it in final ink with no ember
      // bloom, and one synchronous pass replaces the 2600ms cooling rAF.
      const born = reduceMotion ? performance.now() - SEED_FADE_MS : performance.now();
      seeds.push({ fu, fy: 0.03 + 0.94 * Math.random(), r: Math.random(), born });
      if (reduceMotion) composite();
      else kick();
    };
    const frame = canvas.closest('.login-page__frame') ?? canvas.parentElement ?? canvas;
    frame.addEventListener('login:ember-seed', onSeed);

    const observer = new ResizeObserver(rebuild);
    observer.observe(canvas);
    // Re-derive the palette if the OS or in-app theme flips while mounted.
    const scheme = window.matchMedia('(prefers-color-scheme: dark)');
    scheme.addEventListener('change', rebuild);
    const themeObserver = new MutationObserver(rebuild);
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme', 'class'],
    });
    rebuild();

    return () => {
      observer.disconnect();
      themeObserver.disconnect();
      scheme.removeEventListener('change', rebuild);
      frame.removeEventListener('login:ember-seed', onSeed);
      cancelAnimationFrame(raf);
      cancelAnimationFrame(rebuildRaf);
    };
  }, []);

  return <canvas ref={canvasRef} className="login-page__edge-grain" aria-hidden="true" />;
}

export default EdgeGrain;
