import { useEffect, useRef } from 'react';
import { SPX_CLOSES } from './spxSeries';
import { derivePalette, hash } from './loginPaper';
import { createEmberBall } from './emberBall';

/**
 * MarketScanlines - the visual pane of the split login layout: the S&P 500's
 * trailing two years rendered in halftone. Real daily closes drive a
 * dot-matrix price line over a scanline-dash area fill that dissolves toward
 * the baseline and into the past; a pulsing dot pinned beside the auth pane
 * continues from the last close with a gentle synthetic drift so the tape
 * stays alive without any network call. Hovering rolls an ember ball along
 * the curve under the cursor (the price label rides it): swept left to right
 * it heats with velocity and sheds sparks, and carried hot into the right
 * end of the line - or swept hot right off the pane - it breaks through
 * the seam and bursts over the auth column (the canvas spans the frame and
 * paints above the form, so the flight never clips at the seam).
 * Horizontal scrolling over the pane rolls the ball too. Each burst also
 * dispatches a 'login:ember-seed' event, and EdgeGrain answers by seeding
 * one permanent dot in the field - play slowly populates the page.
 *
 * The art prints on the page's own paper: the palette derives from the page
 * background at runtime, and the canvas paints NO field of its own - the
 * page's vertical background wash (see .login-page__frame) shows through
 * everywhere, so both panes share one continuous ground with no left-right
 * step. Ink intensity runs strong on the left to soft on the right, so the
 * composition fades out into the auth pane rather than competing with it.
 * The canvas paints the art alone on clean wash - the page's background
 * texture is EdgeGrain (a sibling element), a static dot field anchored to
 * the frame's right border. Static under reduced motion (the crosshair
 * still redraws on demand).
 */

const STEP_X = 9; // column pitch
const BAR_W = 3; // dash width
const STEP_Y = 7; // dash pitch
const DASH_H = 4; // dash height
const SCROLL = 5; // px/second drift into the past - slow, so the real history stays on screen for minutes
const CURVE = 0.7; // fraction of pane height the price line may reach
const BASELINE_PAD = 32; // px from the pane's bottom edge to the chart baseline - room for the caption
const LEAD_GAP = 84; // px between the live dot and the pane seam
const EASE = 7; // 1/s - how fast the live dot chases a new tick
const FRAME_MS = 30; // ~33fps cap; plenty for halftone drift, halves the cost
const MONO = 'ui-monospace, SFMono-Regular, Menlo, monospace';
// Hoisted so the per-frame price readouts reuse one formatter instead of
// building a fresh Intl.NumberFormat on every draw.
const PRICE_FMT = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
const TINTS = 6; // ink -> ember tint buckets for sparks and the hot ball
// Wash re-print: the tape never replays with a visible cut. Once synthetic
// filler holds WASH_FRAC of the pane, a churning glyph front sweeps across
// and re-prints the real two years from the beginning behind it.
const WASH_MS = 2600; // front sweep duration
const WASH_FRAC = 0.3; // synthetic share of the tape that triggers the re-print
const WASH_GLYPHS = '0123456789+-%$·:|/\\';

// Chart scale from the data itself, padded so the synthetic tail has headroom.
const RAW_LO = Math.min(...SPX_CLOSES);
const RAW_HI = Math.max(...SPX_CLOSES);
const LO = RAW_LO - (RAW_HI - RAW_LO) * 0.1;
const HI = RAW_HI + (RAW_HI - RAW_LO) * 0.14;
const LAST_CLOSE = SPX_CLOSES[SPX_CLOSES.length - 1];
const REAL_LEN = SPX_CLOSES.length; // column ids at or past this are synthetic
const GRID_STEP = [100, 200, 250, 500, 1000, 2000].find((s) => (HI - LO) / s <= 7) ?? 2000;
const GRID_PRICES: number[] = [];
for (let p = Math.ceil(LO / GRID_STEP) * GRID_STEP; p < HI; p += GRID_STEP) GRID_PRICES.push(p);

// Alpha quantized to a few buckets so we build fillStyle strings once, not per rect.
const ALPHA_STEPS = 16;

interface TapeColumn {
  p: number;
  id: number;
}

type TapePoint = { x: number; y: number };

type TapeSnapshot = { cols: TapeColumn[]; offset: number };

function MarketScanlines() {
  const canvasRef = useRef<HTMLCanvasElement | null>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext('2d');
    if (!ctx) return;

    const reduceMotion = window.matchMedia('(prefers-reduced-motion: reduce)').matches;
    const dpr = Math.min(window.devicePixelRatio || 1, 2);

    let width = 0;
    let height = 0;
    let lead = 0; // live-dot x, anchored to the pane seam in resize()
    let base = 0; // chart baseline: BASELINE_PAD above the pane's bottom edge
    let chartH = 0; // vertical span the price line may fill (height * CURVE)
    let liveY = 0; // eased live-dot y, set each frame in draw()
    // Real closes first; synthetic continuation appends as the tape advances.
    const hist: number[] = SPX_CLOSES.slice();
    let stride = 1; // sessions per column, chosen so the pane spans the series
    let cols: TapeColumn[] = [];
    // Reused draw buffers, one per tape role (live tape / outgoing wash
    // snapshot), so drawTape allocates nothing at the ~33fps cadence.
    const livePts: TapePoint[] = [];
    const washPts: TapePoint[] = [];
    let seed = hist.length;
    let offset = 0;
    // Active re-print: the outgoing tape holds the right of the sweeping
    // front while the fresh series prints in behind it.
    let wash: { start: number; old: TapeSnapshot } | null = null;
    let dotY: number | null = null;
    let tNow = 0;
    let raf = 0;
    let last = performance.now();
    let rect = canvas.getBoundingClientRect();
    let mx = -1e9;
    let my = -1e9;
    let tintCache: string[] = [];
    // The canvas paints no background of its own - the page's wash and the
    // EdgeGrain dot field (a sibling element) show through everywhere the
    // art doesn't print.
    let glow: HTMLCanvasElement | null = null;
    // Palette, resolved from the page in setColors().
    let fieldRgb = '10, 10, 10';
    let ink = '235, 235, 235';
    let aBoost = 1; // dark ink at low alpha carries less weight; light mode compensates
    let fillCache: string[] = [];
    // Horizontal fade gradients (rebuilt with the palette on resize/theme).
    let gridGrad: CanvasGradient | null = null;
    let dashGrad: CanvasGradient | null = null;
    // Last size + derived palette, cached so a documentElement attribute
    // mutation that changes neither (app-state class churn) can skip the full
    // setColors/rebuild/buildGlow/draw path.
    let lastSizeKey = '';
    let lastPaletteKey = '';

    // Palette from the page (see loginPaper.derivePalette): ink stored as an
    // "r, g, b" string for the rgba() templates, field likewise for the pillow.
    // The ember is read from CSS --login-ember, so it re-derives here on every
    // theme flip alongside ink/field.
    const setColors = () => {
      const pal = derivePalette(canvas);
      aBoost = pal.aBoost;
      ink = pal.ink.join(', ');
      fieldRgb = pal.field.join(', ');
      fillCache = Array.from(
        { length: ALPHA_STEPS + 1 },
        (_, i) => `rgba(${ink}, ${(i / ALPHA_STEPS).toFixed(3)})`
      );
      // Heat ramp: plain ink through burnt ember, the only warmth on the page.
      const inkArr = pal.ink;
      const emberArr = pal.ember;
      tintCache = Array.from({ length: TINTS }, (_, i) => {
        const m = i / (TINTS - 1);
        return `rgb(${inkArr.map((v, k) => Math.round(v + (emberArr[k] - v) * m)).join(', ')})`;
      });
      lastPaletteKey = `${ink}|${fieldRgb}|${emberArr.join(', ')}`;
    };

    const buildGlow = () => {
      // Soft pillow in the lifted page tone the live dot + readout sit on,
      // so the focal cluster stays legible where the art fades out.
      const gw = 180;
      const gh = 110;
      const g = document.createElement('canvas');
      g.width = gw * dpr;
      g.height = gh * dpr;
      const gctx = g.getContext('2d');
      if (gctx) {
        gctx.setTransform(dpr, 0, 0, dpr, 0, 0);
        gctx.translate(gw / 2, gh / 2);
        gctx.scale(1, gh / gw);
        const rg = gctx.createRadialGradient(0, 0, 0, 0, 0, gw / 2);
        rg.addColorStop(0, `rgba(${fieldRgb}, 0.9)`);
        rg.addColorStop(0.55, `rgba(${fieldRgb}, 0.6)`);
        rg.addColorStop(1, `rgba(${fieldRgb}, 0)`);
        gctx.fillStyle = rg;
        gctx.fillRect(-gw / 2, -gw / 2, gw, gw);
        glow = g;
      } else {
        glow = null;
      }
    };

    // Synthetic tick: a mean-reverting wobble around the last real close (with
    // occasional shocks), so the live readout stays honest to the actual index.
    let drift = 0;
    const makeNext = (): TapeColumn => {
      const shock = hash(seed, 9.7) < 0.04 ? (hash(seed, 3.3) - 0.5) * 0.02 : 0;
      drift = drift * 0.9 + (hash(seed, 1.1) - 0.5) * 0.01 + shock;
      drift = Math.min(0.025, Math.max(-0.025, drift));
      seed++;
      const p = LAST_CLOSE * (1 + drift);
      hist.push(p);
      return { p, id: hist.length - 1 };
    };

    // Sample the series so its full span fits the pane, newest at the end.
    const rebuild = () => {
      const n = Math.max(2, Math.ceil(Math.max(0, lead) / STEP_X) + 2);
      stride = Math.max(1, Math.floor((hist.length - 1) / (n - 1)));
      cols = new Array(n);
      for (let j = 0; j < n; j++) {
        const idx = Math.max(0, hist.length - 1 - (n - 1 - j) * stride);
        cols[j] = { p: hist[idx], id: idx };
      }
    };

    // Price -> canvas y at the current layout. base and chartH follow the pane
    // height, recomputed in resize() like lead/width, so this one closure backs
    // every price->y call site (tick, draw, valAt, drawTape).
    const yOf = (p: number) => base - ((p - LO) / (HI - LO)) * chartH;

    // Price and curve-y at an arbitrary canvas x, interpolated between columns.
    const valAt = (x: number) => {
      const fj = cols.length - 1 - (lead - x - offset) / STEP_X;
      const j0 = Math.max(0, Math.min(cols.length - 1, Math.floor(fj)));
      const j1 = Math.min(cols.length - 1, j0 + 1);
      const ft = Math.min(1, Math.max(0, fj - j0));
      const p = cols[j0].p * (1 - ft) + cols[j1].p * ft;
      return { p, y: yOf(p) };
    };

    // The hover flourish (ember ball, sparks, breakout) lives in its own
    // module; the host feeds it the tape's curve, layout, pointer, palette, and
    // a burst callback, and forwards pointer wheel/leave into it below. It is
    // created even under reduced motion but never driven there (see draw/tick).
    const ember = createEmberBall({
      sampleCurve: valAt,
      getBounds: () => ({ width, height, lead }),
      getPointer: () => ({ x: mx, y: my }),
      getTint: (heat) => tintCache[Math.round(heat * (TINTS - 1))],
      getInk: () => ink,
      mono: MONO,
      onBurst: () => canvas.dispatchEvent(new CustomEvent('login:ember-seed', { bubbles: true })),
    });

    // The tape art (area fill, dot-matrix line, leading segment) for one
    // column set. Parameterized so the wash can print the outgoing and the
    // re-printed tape on either side of its front under clip regions. base,
    // chartH (via yOf), lead, and liveY come from the enclosing scope, like the
    // rest of the layout.
    const drawTape = (tcols: TapeColumn[], toff: number, pts: TapePoint[]): TapePoint[] => {
      // Points land in the caller-owned buffer, grown once and mutated in
      // place - the draw loop must not allocate a fresh array per frame.
      while (pts.length < tcols.length) pts.push({ x: 0, y: 0 });
      pts.length = tcols.length;

      // Area fill: scanline dashes under the price line - densest at the line,
      // dissolving toward the baseline and fading out toward the seam.
      for (let j = 0; j < tcols.length; j++) {
        const x = lead - (tcols.length - 1 - j) * STEP_X - toff;
        const { p, id } = tcols[j];
        const top = yOf(p);
        pts[j].x = x;
        pts[j].y = top;
        if (x < -STEP_X) continue;
        const span = base - top;
        const k = Math.min(1, Math.max(0, 1 - x / lead));
        const fade = (0.3 + 0.7 * k) * Math.min(1, k * 4); // feathers to zero at the seam
        for (let d = 0; d * STEP_Y < span; d++) {
          const y = base - d * STEP_Y;
          const rise = (d * STEP_Y) / span; // 0 at the baseline, 1 at the line
          if (hash(id, d) < (1 - rise) * 0.6) continue;
          const alpha = (0.05 + 0.3 * rise * rise) * fade * (0.6 + 0.4 * hash(id, d + 57)) * aBoost;
          ctx.fillStyle = fillCache[Math.round(Math.min(0.75, alpha) * ALPHA_STEPS)];
          ctx.fillRect(x, y, BAR_W, DASH_H);
        }
      }

      // Dot-matrix price line - solid in the past, thinning toward the seam,
      // then re-inking over the last stretch so the rail visibly docks into
      // the live dot. Without the recovery the fade zone grows with the pane
      // (it is proportional to lead) and on wide viewports the anchor floated
      // disconnected past a long invisible run.
      for (let j = 1; j < tcols.length; j++) {
        const x0 = pts[j - 1].x;
        const y0 = pts[j - 1].y;
        const dx = pts[j].x - x0;
        const dy = pts[j].y - y0;
        const steps = Math.max(1, Math.round(Math.hypot(dx, dy) / 4));
        const sid = tcols[j].id;
        for (let s = 0; s < steps; s++) {
          const t = s / steps;
          const px = x0 + dx * t;
          if (px < -2 || px > lead + 2) continue;
          const fadeT = Math.min(1, Math.max(0, 1 - px / lead));
          const meet = Math.max(0, 1 - (lead - px) / 120);
          if (hash(sid, s) < (1 - fadeT) * 0.5 * (1 - meet)) continue;
          const alpha = Math.min(
            0.9,
            Math.max(
              (0.2 + 0.65 * fadeT * fadeT) * (0.7 + 0.3 * hash(sid, s + 31)),
              0.18 + 0.3 * meet
            ) * aBoost
          );
          ctx.fillStyle = fillCache[Math.round(alpha * ALPHA_STEPS)];
          ctx.fillRect(px - 1, y0 + dy * t - 1, 2, 2);
        }
      }
      // Leading segment chases the eased dot, already faded near the seam.
      {
        const p0 = pts[tcols.length - 1];
        const dx = lead - p0.x;
        const dy = liveY - p0.y;
        const steps = Math.max(1, Math.round(Math.hypot(dx, dy) / 3));
        ctx.fillStyle = fillCache[Math.round(0.4 * ALPHA_STEPS)];
        for (let s = 0; s <= steps; s++) {
          const t = s / steps;
          ctx.fillRect(p0.x + dx * t - 1, p0.y + dy * t - 1, 2, 2);
        }
      }
      return pts;
    };

    // The glyph curtain riding the wash front: a churning teletype band that
    // erases the outgoing tape and reveals the re-printed one behind it.
    const drawWashBand = (frontX: number) => {
      const bucket = Math.floor(tNow / 80); // churn rate of the glyphs
      ctx.font = `10px ${MONO}`;
      ctx.textAlign = 'center';
      for (let gx = -4; gx <= 4; gx++) {
        const x = frontX + gx * 9;
        if (x < 2 || x > width - 2) continue;
        const fall = 1 - Math.abs(gx) / 5; // densest on the front line
        // The band hugs the tape - a little above the curve down to the
        // baseline - so the churn never wanders up into the headline copy.
        const top = Math.max(16, valAt(x).y - 46);
        for (let y = top; y < base; y += 11) {
          if (hash(gx * 13.7 + bucket, y * 0.61) > 0.2 + 0.6 * fall) continue;
          const ch = WASH_GLYPHS[Math.floor(hash(y * 1.3, bucket + gx * 7) * WASH_GLYPHS.length)];
          const alpha = (0.14 + 0.5 * fall * hash(y, bucket * 1.7)) * aBoost;
          if (hash(gx + 31, y + bucket) < 0.08) {
            // A few ember flecks ride the front, like the ball's spark shed.
            ctx.globalAlpha = alpha;
            ctx.fillStyle = tintCache[TINTS - 1];
            ctx.fillText(ch, x, y);
            ctx.globalAlpha = 1;
          } else {
            ctx.fillStyle = fillCache[Math.round(Math.min(0.75, alpha) * ALPHA_STEPS)];
            ctx.fillText(ch, x, y);
          }
        }
      }
    };

    const draw = () => {
      // The canvas is transparent wherever the art doesn't print (the page's
      // wash shows through), so clear each frame or sparks would smear.
      ctx.clearRect(0, 0, width, height);
      const targetY = yOf(cols[cols.length - 1].p);
      if (dotY === null) dotY = targetY;
      liveY = dotY;

      // Price grid: hairlines + labels at real index levels.
      ctx.font = `10px ${MONO}`;
      ctx.textAlign = 'left';
      for (const p of GRID_PRICES) {
        const gy = Math.round(yOf(p));
        if (gy < 16 || gy > base - 4) continue;
        ctx.fillStyle = gridGrad ?? `rgba(${ink}, 0.05)`;
        ctx.fillRect(0, gy, width, 1);
        ctx.fillStyle = `rgba(${ink}, 0.2)`;
        ctx.fillText(p.toLocaleString('en-US'), 16, gy - 5);
      }

      // Tape art - split at the wash front while a re-print sweeps through.
      let pts: TapePoint[];
      if (wash) {
        const t = Math.min(1, (tNow - wash.start) / WASH_MS);
        const e = t * t * (3 - 2 * t);
        const frontX = e * (lead + 80); // covers the art's overhang past lead
        ctx.save();
        ctx.beginPath();
        ctx.rect(0, 0, frontX, height);
        ctx.clip();
        pts = drawTape(cols, offset, livePts);
        ctx.restore();
        ctx.save();
        ctx.beginPath();
        ctx.rect(frontX, 0, width - frontX, height);
        ctx.clip();
        drawTape(wash.old.cols, wash.old.offset, washPts);
        ctx.restore();
        drawWashBand(frontX);
        if (t >= 1) wash = null;
      } else {
        pts = drawTape(cols, offset, livePts);
      }

      // Dashed last-price line trails from the live dot into history.
      ctx.strokeStyle = dashGrad ?? `rgba(${ink}, 0.2)`;
      ctx.lineWidth = 1;
      ctx.setLineDash([2, 6]);
      ctx.beginPath();
      ctx.moveTo(0, liveY + 0.5);
      ctx.lineTo(lead, liveY + 0.5);
      ctx.stroke();
      ctx.setLineDash([]);

      ctx.fillStyle = gridGrad ?? `rgba(${ink}, 0.1)`;
      ctx.fillRect(0, base + STEP_Y, width, 1);

      // Caption anchors the art in the real series.
      ctx.font = `10px ${MONO}`;
      ctx.textAlign = 'left';
      ctx.fillStyle = `rgba(${ink}, 0.3)`;
      ctx.fillText('S&P 500 · trailing two years', 16, height - 12);

      // Field-colored pillow keeps the focal cluster legible over the swell.
      if (glow) ctx.drawImage(glow, lead - 120, liveY - 55, 180, 110);

      // The live dot: sonar pulse + core, pinned beside the auth pane.
      if (reduceMotion) {
        ctx.beginPath();
        ctx.arc(lead, liveY, 6, 0, Math.PI * 2);
        ctx.strokeStyle = `rgba(${ink}, 0.15)`;
        ctx.stroke();
      } else {
        const p = (tNow % 1400) / 1400;
        ctx.beginPath();
        ctx.arc(lead, liveY, 4 + p * 10, 0, Math.PI * 2);
        ctx.strokeStyle = `rgba(${ink}, ${(0.22 * (1 - p)).toFixed(3)})`;
        ctx.stroke();
      }
      ctx.beginPath();
      ctx.arc(lead, liveY, 7, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(${ink}, 0.08)`;
      ctx.fill();
      ctx.beginPath();
      ctx.arc(lead, liveY, 3, 0, Math.PI * 2);
      ctx.fillStyle = `rgba(${ink}, 0.55)`;
      ctx.fill();

      // Readout rides the dot, morphing as it eases between ticks. It sits
      // left of the dot, on quiet ground, clear of the seam dissolve.
      const price = LO + ((base - liveY) / chartH) * (HI - LO);
      ctx.font = `11px ${MONO}`;
      ctx.textAlign = 'right';
      ctx.fillStyle = `rgba(${ink}, 0.5)`;
      ctx.fillText(PRICE_FMT.format(price), lead - 12, liveY - 9);

      // Under reduced motion the hover stays a static crosshair: the actual
      // close under the cursor. No ball, no sparks.
      if (reduceMotion) {
        if (mx >= 0 && mx <= lead && my >= 0 && my <= height) {
          const j = Math.min(
            cols.length - 1,
            Math.max(0, Math.round(cols.length - 1 - (lead - mx - offset) / STEP_X))
          );
          const p = pts[j];
          if (p && p.x >= 0) {
            ctx.fillStyle = `rgba(${ink}, 0.07)`;
            ctx.fillRect(Math.round(p.x), 16, 1, base - 16);
            ctx.beginPath();
            ctx.arc(p.x + 0.5, p.y, 5, 0, Math.PI * 2);
            ctx.strokeStyle = `rgba(${ink}, 0.4)`;
            ctx.stroke();
            ctx.beginPath();
            ctx.arc(p.x + 0.5, p.y, 2, 0, Math.PI * 2);
            ctx.fillStyle = `rgba(${ink}, 0.9)`;
            ctx.fill();
            ctx.font = `10px ${MONO}`;
            ctx.textAlign = 'center';
            ctx.fillStyle = `rgba(${ink}, 0.55)`;
            ctx.fillText(
              PRICE_FMT.format(cols[j].p),
              Math.min(lead - 30, Math.max(30, p.x)),
              p.y - 12
            );
          }
        }
        return;
      }

      // The hover ember plays last, over the finished tape and the form.
      ember.draw(ctx);
    };

    let drawQueued = false;
    const drawOnDemand = () => {
      if (drawQueued) return;
      drawQueued = true;
      requestAnimationFrame(() => {
        drawQueued = false;
        draw();
      });
    };

    const resize = () => {
      rect = canvas.getBoundingClientRect();
      width = Math.max(1, Math.round(rect.width));
      height = Math.max(1, Math.round(rect.height));
      canvas.width = width * dpr;
      canvas.height = height * dpr;
      ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
      base = height - BASELINE_PAD;
      chartH = height * CURVE;
      setColors();
      lastSizeKey = `${width}x${height}`;
      // The live dot anchors just left of the pane seam (the canvas overhangs
      // it), so the chart runs the full pane and its dissolve hands off to
      // the grain ramp, which rises from a little left of the auth column
      // (see EdgeGrain). The pane, not the canvas, gives the seam: the canvas
      // is wider by the overhang.
      const paneW = canvas.parentElement?.getBoundingClientRect().width ?? width;
      lead = Math.max(40, Math.min(width, Math.round(paneW)) - LEAD_GAP);
      // Horizontal fades: grid hairlines and the last-price dash soften as
      // they approach the seam and die before it - nothing covers them
      // from above anymore, so no rule may cross into the auth side.
      gridGrad = ctx.createLinearGradient(0, 0, width, 0);
      gridGrad.addColorStop(0, `rgba(${ink}, 0.065)`);
      gridGrad.addColorStop(Math.min(1, Math.max(0, lead / width)), `rgba(${ink}, 0.012)`);
      gridGrad.addColorStop(Math.min(1, (lead + 56) / width), `rgba(${ink}, 0)`);
      dashGrad = ctx.createLinearGradient(0, 0, Math.max(1, lead), 0);
      dashGrad.addColorStop(0, `rgba(${ink}, 0.3)`);
      dashGrad.addColorStop(1, `rgba(${ink}, 0.18)`);
      // A mid-wash resize can't keep the outgoing snapshot's geometry -
      // just land on the freshly printed tape.
      wash = null;
      rebuild();
      buildGlow();
      dotY = null; // snap to the new geometry
      draw();
    };

    const tick = (now: number) => {
      raf = requestAnimationFrame(tick);
      if (now - last < FRAME_MS) return;
      const dt = Math.min(0.1, (now - last) / 1000);
      last = now;
      tNow = now;
      offset += SCROLL * dt;
      while (offset >= STEP_X) {
        offset -= STEP_X;
        cols.push(makeNext());
        cols.shift();
      }
      // Loop the story: the tape never replays with a cut. Once synthetic
      // filler holds enough of the pane, snapshot the outgoing tape and
      // re-print the real two years behind the sweeping glyph front.
      if (!wash) {
        let synth = 0;
        for (let j = cols.length - 1; j >= 0 && cols[j].id >= REAL_LEN; j--) synth++;
        if (synth >= cols.length * WASH_FRAC) {
          wash = { start: now, old: { cols: cols.slice(), offset } };
          hist.length = 0;
          for (const p of SPX_CLOSES) hist.push(p);
          drift = 0;
          rebuild();
          offset = 0;
        }
      }
      const targetY = yOf(cols[cols.length - 1].p);
      if (dotY !== null) dotY += (targetY - dotY) * Math.min(1, dt * EASE);
      ember.update(dt, now);
      draw();
    };

    const onMove = (e: PointerEvent) => {
      mx = e.clientX - rect.left;
      my = e.clientY - rect.top;
      if (reduceMotion) drawOnDemand();
    };
    const onLeave = () => {
      mx = -1e9;
      my = -1e9;
      // The ember module decides the breakout-vs-dissolve on leave (it owns the
      // ball state); the host only clears the pointer above.
      ember.onLeave();
      if (reduceMotion) drawOnDemand();
    };
    // Horizontal scroll (trackpad swipe) rolls the ball ahead of the cursor
    // and stokes it - the scroll route to the same breakout.
    const onWheel = (e: WheelEvent) => {
      if (reduceMotion || mx < 0) return;
      ember.onWheel(e.deltaX);
    };

    const observer = new ResizeObserver(resize);
    observer.observe(canvas);
    // Interaction listens on the pane, not the canvas: the canvas overhangs
    // the seam (pointer-events off in CSS) and must never intercept events
    // over the auth column. The pane covers exactly the old hit area, and
    // onMove keeps measuring against the canvas rect, so coordinates hold.
    const host = canvas.parentElement ?? canvas;
    host.addEventListener('pointermove', onMove);
    host.addEventListener('pointerleave', onLeave);
    host.addEventListener('wheel', onWheel, { passive: true });
    // Re-derive the palette if the OS or in-app theme flips while mounted
    // (the app stamps `data-theme` on the root element).
    const scheme = window.matchMedia('(prefers-color-scheme: dark)');
    scheme.addEventListener('change', resize);
    // Attribute mutations on <html> fire on every class/data-theme change, most
    // of which touch neither the palette nor the size. Recompute those two
    // cheaply and bail before the full setColors/rebuild/buildGlow/draw path
    // when nothing the art depends on actually changed.
    const onThemeChange = () => {
      const r = canvas.getBoundingClientRect();
      const sizeKey = `${Math.max(1, Math.round(r.width))}x${Math.max(1, Math.round(r.height))}`;
      const pal = derivePalette(canvas);
      const paletteKey = `${pal.ink.join(', ')}|${pal.field.join(', ')}|${pal.ember.join(', ')}`;
      if (sizeKey === lastSizeKey && paletteKey === lastPaletteKey) return;
      resize();
    };
    const themeObserver = new MutationObserver(onThemeChange);
    themeObserver.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme', 'class'],
    });
    resize();
    if (!reduceMotion) raf = requestAnimationFrame(tick);

    return () => {
      observer.disconnect();
      themeObserver.disconnect();
      host.removeEventListener('pointermove', onMove);
      host.removeEventListener('pointerleave', onLeave);
      host.removeEventListener('wheel', onWheel);
      scheme.removeEventListener('change', resize);
      cancelAnimationFrame(raf);
    };
  }, []);

  return <canvas ref={canvasRef} className="login-page__scanlines" aria-hidden="true" />;
}

export default MarketScanlines;
