import { useEffect, useRef, useState } from "react";
import { cn } from "@/lib/utils";

/** The caller's opacity fade, which it sets as its `transitionDuration`. */
export const FADE_MS = 200;
// Outlasts that fade, so the dots never freeze while still seen.
const SETTLE_MS = FADE_MS + 50;

// Ten braille cells of 2 × 4 dots. A braille glyph set in a monospace face puts
// its dots 1.2× as far apart across as down, so the box is 3:1.
const COLS = 20;
const ROWS = 8;
const DOT_ASPECT = 1.2;

const ORBIT_MS = 5000;
const FPS = 30;
const FRAMES = (ORBIT_MS / 1000) * FPS;
const REDUCED_RATE = 0.4;

// The orbit is tabled at 750 points: a frame is every 5th and the trail steps
// back 4 at a time, so every position read is a sample, never an interpolation.
const SAMPLES = 750;
const FRAME_STRIDE = SAMPLES / FRAMES;
const TRAIL = 30;
const TRAIL_STRIDE = 4;

/**
 * The figure-eight choreography (Moore 1993, proved by Chenciner & Montgomery
 * in 2000): three equal masses chasing each other round one loop. Integrated
 * over a single period and replayed, so it never drifts however long it runs.
 */
function integrateOrbit(): { xs: Float64Array; ys: Float64Array } {
  const period = 6.32591398;
  const substeps = 10;
  const dt = period / (SAMPLES * substeps);
  const x = [0.97000436, -0.97000436, 0];
  const y = [-0.24308753, 0.24308753, 0];
  const vx = [0.466203685, 0.466203685, -0.93240737];
  const vy = [0.43236573, 0.43236573, -0.86473146];
  const ax = [0, 0, 0];
  const ay = [0, 0, 0];
  const accel = () => {
    ax.fill(0);
    ay.fill(0);
    for (let i = 0; i < 3; i++) {
      for (let j = i + 1; j < 3; j++) {
        const dx = x[j] - x[i];
        const dy = y[j] - y[i];
        const r2 = dx * dx + dy * dy;
        const f = 1 / (r2 * Math.sqrt(r2));
        ax[i] += dx * f; ay[i] += dy * f;
        ax[j] -= dx * f; ay[j] -= dy * f;
      }
    }
  };
  const xs = new Float64Array(SAMPLES * 3);
  const ys = new Float64Array(SAMPLES * 3);
  accel();
  for (let s = 0; s < SAMPLES; s++) {
    for (let i = 0; i < 3; i++) {
      xs[s * 3 + i] = x[i];
      ys[s * 3 + i] = y[i];
    }
    // Leapfrog: symplectic, so the energy holds and the loop closes on itself.
    for (let k = 0; k < substeps; k++) {
      for (let i = 0; i < 3; i++) {
        vx[i] += (ax[i] * dt) / 2; vy[i] += (ay[i] * dt) / 2;
        x[i] += vx[i] * dt; y[i] += vy[i] * dt;
      }
      accel();
      for (let i = 0; i < 3; i++) {
        vx[i] += (ax[i] * dt) / 2; vy[i] += (ay[i] * dt) / 2;
      }
    }
  }
  return { xs, ys };
}

// Older trail points are kept more sparsely, which is how a dot trail fades.
function keepTrail(k: number): boolean {
  const f = k / TRAIL;
  return f < 0.35 || (f < 0.7 ? k % 2 === 0 : k % 3 === 0);
}

/** Each frame as the indices of its lit dots, row-major. */
function buildFrames(): Uint8Array[] {
  const { xs, ys } = integrateOrbit();
  let minX = Infinity, maxX = -Infinity, minY = Infinity, maxY = -Infinity;
  for (let n = 0; n < xs.length; n++) {
    minX = Math.min(minX, xs[n]); maxX = Math.max(maxX, xs[n]);
    minY = Math.min(minY, ys[n]); maxY = Math.max(maxY, ys[n]);
  }
  const sx = Math.min((COLS - 2) / (maxX - minX), (ROWS - 2) / ((maxY - minY) * DOT_ASPECT));
  const sy = sx * DOT_ASPECT;
  const cx = (minX + maxX) / 2;
  const cy = (minY + maxY) / 2;

  const frames: Uint8Array[] = [];
  const grid = new Uint8Array(COLS * ROWS);
  const plot = (col: number, row: number) => {
    if (col >= 0 && col < COLS && row >= 0 && row < ROWS) grid[row * COLS + col] = 1;
  };
  for (let f = 0; f < FRAMES; f++) {
    grid.fill(0);
    for (let i = 0; i < 3; i++) {
      const at = (s: number) => {
        const n = ((s % SAMPLES) + SAMPLES) % SAMPLES * 3 + i;
        return [(xs[n] - cx) * sx + (COLS - 1) / 2, (ROWS - 1) / 2 - (ys[n] - cy) * sy];
      };
      for (let k = 1; k <= TRAIL; k++) {
        if (!keepTrail(k)) continue;
        const [col, row] = at(f * FRAME_STRIDE - k * TRAIL_STRIDE);
        plot(Math.round(col), Math.round(row));
      }
      // The body itself is a 2 × 2 block, so it stands out from its trail.
      const [col, row] = at(f * FRAME_STRIDE);
      const c0 = Math.round(col - 0.5);
      const r0 = Math.round(row - 0.5);
      plot(c0, r0); plot(c0 + 1, r0); plot(c0, r0 + 1); plot(c0 + 1, r0 + 1);
    }
    const lit: number[] = [];
    grid.forEach((on, n) => { if (on) lit.push(n); });
    frames.push(Uint8Array.from(lit));
  }
  return frames;
}

let frameCache: Uint8Array[] | null = null;

interface FigureEightLoadingProps {
  className?: string;
  /** False while the caller has faded the glyph out: the frame loop stops once
   *  the fade has run, instead of redrawing a figure nobody can see. */
  active?: boolean;
}

/**
 * Drawn on a canvas in braille's dot geometry rather than set as braille text.
 * The web mono face has no braille, so text would take each OS's fallback face
 * with its own width and dot spacing, and a new string every frame costs a
 * layout where a canvas costs only a paint. It keeps `currentColor` semantics
 * by reading the computed color when it starts and on every theme flip.
 */
export default function FigureEightLoading({
  className,
  active = true,
}: FigureEightLoadingProps) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  // Orbit time, kept across a stop so a resumed figure carries on from where
  // it was rather than jumping.
  const clockRef = useRef(0);

  const [running, setRunning] = useState(active);
  if (active && !running) setRunning(true);
  useEffect(() => {
    if (active || !running) return;
    const id = setTimeout(() => setRunning(false), SETTLE_MS);
    return () => clearTimeout(id);
  }, [active, running]);

  // className is a dependency so a caller's new color class is read again.
  useEffect(() => {
    const canvas = canvasRef.current;
    const ctx = running ? canvas?.getContext("2d") : null;
    if (!canvas || !ctx) return;

    const frames = (frameCache ??= buildFrames());
    const reduce = window.matchMedia?.("(prefers-reduced-motion: reduce)");
    let color = getComputedStyle(canvas).color;
    let cssWidth = 0;
    let cssHeight = 0;
    let last = performance.now();
    let drawn = -1;

    function draw(now: number) {
      // A frame's timestamp is when the frame began, which can be earlier than
      // the `performance.now()` taken just before it. Never step back: a
      // negative clock picks frame -1, which does not exist.
      clockRef.current += Math.max(0, now - last) * (reduce?.matches ? REDUCED_RATE : 1);
      last = now;
      // Read per frame, not measured: moving the window to another display
      // changes the ratio without resizing the box.
      const dpr = window.devicePixelRatio || 1;
      const width = Math.round(cssWidth * dpr);
      const height = Math.round(cssHeight * dpr);
      if (!width || !height) return;
      if (canvas!.width !== width || canvas!.height !== height) {
        canvas!.width = width;
        canvas!.height = height;
        drawn = -1;
      }

      // The figure moves a whole dot at a time, so a display frame that lands
      // on the same orbit frame has nothing new to paint.
      const f = Math.floor(clockRef.current / (1000 / FPS)) % FRAMES;
      if (f === drawn) return;
      drawn = f;

      const pitchX = width / COLS;
      const pitchY = height / ROWS;
      const radius = Math.min(pitchX / DOT_ASPECT, pitchY) * 0.34;
      ctx!.clearRect(0, 0, width, height);
      ctx!.fillStyle = color;
      ctx!.beginPath();
      for (const n of frames[f]) {
        const x = ((n % COLS) + 0.5) * pitchX;
        const y = (Math.floor(n / COLS) + 0.5) * pitchY;
        ctx!.moveTo(x + radius, y);
        ctx!.arc(x, y, radius, 0, Math.PI * 2);
      }
      ctx!.fill();
    }

    // A canvas with no size, as in a cached thread view under display:none,
    // stops asking for frames: the indicator stays on through long tool calls,
    // and a hidden one would otherwise run a frame loop for nothing. The size
    // observer starts the loop again when the canvas has a box.
    let raf = 0;
    function frame(now: number) {
      draw(now);
      raf = cssWidth && cssHeight ? requestAnimationFrame(frame) : 0;
    }

    // The first observation lands after layout and before paint, so the
    // figure is drawn before it is seen, never shown blank or stale.
    const sizes = new ResizeObserver(([entry]) => {
      cssWidth = entry.contentRect.width;
      cssHeight = entry.contentRect.height;
      if (!raf && cssWidth && cssHeight) {
        last = performance.now();
        raf = requestAnimationFrame(frame);
      }
      draw(performance.now());
    });
    sizes.observe(canvas);
    const themes = new MutationObserver(() => {
      color = getComputedStyle(canvas).color;
      drawn = -1;
    });
    themes.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["data-theme", "class"],
    });
    raf = requestAnimationFrame(frame);

    return () => {
      cancelAnimationFrame(raf);
      sizes.disconnect();
      themes.disconnect();
    };
  }, [running, className]);

  return (
    <div className={cn("relative aspect-[3/1]", className)}>
      <canvas ref={canvasRef} className="w-full h-full" aria-hidden="true" />
    </div>
  );
}
