/**
 * emberBall - the hover flourish for MarketScanlines' market tape, lifted out
 * so the tape file reads as tape + draw alone. Rolls an ember ball along the
 * price curve under the cursor: swept left to right it heats with velocity and
 * sheds sparks, and carried hot into the right end of the line - or swept hot
 * right off the pane - it breaks through the seam and bursts over the auth
 * column. Horizontal scrolling rolls the ball too. Every burst calls
 * deps.onBurst so the page can answer (EdgeGrain seeds one permanent grain dot).
 *
 * Pure physics + canvas drawing, no React. The host owns the rAF loop, the
 * tape, and the palette, and feeds this module by (a) supplying the deps below
 * and (b) forwarding pointer wheel/leave in. It never runs under reduced
 * motion - the host simply never wires it up there.
 */

// Hoisted so the ball's price label reuses one formatter instead of building a
// fresh Intl.NumberFormat on every draw.
const PRICE_FMT = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

// Ember ball: rolls the curve under the cursor, burning with sweep velocity.
const BALL_CHASE = 10; // 1/s - how hard the ball chases the cursor
const BALL_VMAX = 2200; // px/s cap - a scroll fling rolls the ball, never teleports it
const BALL_SETTLE = 14; // 1/s - vertical easing so steep curve sections read smooth
const HEAT_VREF = 700; // px/s sweep speed that counts as full heating
const HEAT_RISE = 3.2; // 1/s heat gain at full speed
const HEAT_FALL = 0.4; // 1/s cooling when the ball idles
const MAX_SPARKS = 400;

interface Spark {
  x: number;
  y: number;
  vx: number;
  vy: number;
  life: number;
  t: number;
  sz: number;
  hot: number; // 0 = plain ink, 1 = full ember
}

export interface EmberBallDeps {
  /** Price and curve-y at a canvas x (wraps the tape's valAt). */
  sampleCurve: (x: number) => { p: number; y: number };
  /** Live layout: pane width/height and the live-dot x (the seam anchor). */
  getBounds: () => { width: number; height: number; lead: number };
  /** Pointer in canvas space; a coord outside [0, width/height] reads as "away". */
  getPointer: () => { x: number; y: number };
  /** Ember tint for a heat in [0,1] (0 = plain ink, 1 = full ember). */
  getTint: (heat: number) => string;
  /** The page ink as an "r, g, b" string, for the price label riding the ball. */
  getInk: () => string;
  /** Monospace stack for the price label. */
  mono: string;
  /** One burst happened: seed the page (dispatches 'login:ember-seed'). */
  onBurst: () => void;
}

export interface EmberBall {
  /** Ball + spark physics, integrated per tick (never under reduced motion). */
  update: (dt: number, now: number) => void;
  /** Paint sparks, the burst ring, and the live ball or breakout comet. */
  draw: (ctx: CanvasRenderingContext2D) => void;
  /** Trackpad horizontal scroll: rolls the ball ahead of the cursor and stokes it. */
  onWheel: (deltaX: number) => void;
  /** Pointer left the pane: launch on a hot overshoot, else let the ball dissolve. */
  onLeave: () => void;
}

export function createEmberBall(deps: EmberBallDeps): EmberBall {
  const { sampleCurve, getBounds, getPointer, getTint, getInk, mono, onBurst } = deps;

  // Ember ball state: rolls the curve under the cursor, heats with sweep
  // velocity, and breaks out through the seam when carried in hot.
  let mode: 'roll' | 'fly' | 'cool' = 'roll';
  let ballX: number | null = null;
  let ballY: number | null = null; // eased toward the curve, not pinned to it
  let ballVX = 0;
  let heat = 0;
  let wheelOff = 0; // horizontal scroll pushes the ball ahead of the cursor
  let pmx = -1e9; // pointer position at the previous tick...
  let pmy = -1e9;
  let pvX = 0; // ...and its smoothed velocity - the breakout inherits it
  let pvY = 0;
  let embersBudget = 0;
  let flyX = 0;
  let flyY = 0;
  let flyVX = 0;
  let flyVY = 0;
  let flyT = 0;
  let coolUntil = 0;
  let ringX = 0;
  let ringY = 0;
  let ringT = -1; // >= 0 while the burst ring expands
  const sparks: Spark[] = [];

  const spawnSpark = (
    x: number,
    y: number,
    vx: number,
    vy: number,
    life: number,
    sz: number,
    hot: number
  ) => {
    if (sparks.length < MAX_SPARKS) sparks.push({ x, y, vx, vy, life, sz, hot, t: 0 });
  };

  const explode = () => {
    for (let i = 0; i < 46; i++) {
      const a = (i / 46) * Math.PI * 2 + Math.random() * 0.35;
      const sp = 90 + Math.random() * 430;
      spawnSpark(
        flyX,
        flyY,
        Math.cos(a) * sp + flyVX * 0.12,
        Math.sin(a) * sp * 0.8,
        0.45 + Math.random() * 0.7,
        Math.random() < 0.22 ? 3 : Math.random() < 0.6 ? 2 : 1,
        0.55 + Math.random() * 0.45
      );
    }
    ringX = flyX;
    ringY = flyY;
    ringT = 0;
    heat = 0;
    mode = 'cool';
    coolUntil = performance.now() + 950;
    // Every flight leaves a trace: EdgeGrain listens on the frame and seeds
    // one permanent grain dot per burst, ember-hot at first.
    onBurst();
  };

  // Break through the seam: the ball leaves the curve along the cursor's
  // smoothed motion (never back into the chart); with a still cursor - a
  // scroll breakout - the curve's tangent wins.
  const launchBall = () => {
    if (ballX === null) return;
    mode = 'fly';
    flyX = ballX;
    flyY = ballY ?? sampleCurve(ballX).y;
    const sp = Math.max(480, Math.min(1400, ballVX * 1.05));
    const pv = Math.hypot(pvX, pvY);
    if (pv > 160) {
      const nx = Math.max(0.25, pvX / pv);
      const ny = pvY / pv;
      const n = Math.hypot(nx, ny);
      flyVX = (sp * nx) / n;
      flyVY = (sp * ny) / n - 60;
    } else {
      const slope = (flyY - sampleCurve(ballX - 14).y) / 14;
      flyVX = sp;
      flyVY = slope * sp * 0.5 - 140;
    }
    flyT = 0;
  };

  const update = (dt: number, now: number) => {
    for (let i = sparks.length - 1; i >= 0; i--) {
      const s = sparks[i];
      s.t += dt;
      if (s.t >= s.life) {
        sparks.splice(i, 1);
        continue;
      }
      s.x += s.vx * dt;
      s.y += s.vy * dt;
      s.vx -= s.vx * 2.2 * dt;
      s.vy -= (s.vy * 2.2 + 70) * dt; // drag, and embers drift up as they die
    }
    if (ringT >= 0) ringT += dt;
    wheelOff -= wheelOff * 3 * dt;
    if (mode === 'cool') {
      if (now >= coolUntil) {
        mode = 'roll';
        ballX = null;
        ballY = null;
        heat = 0;
      }
      return;
    }
    const { width, height, lead } = getBounds();
    if (mode === 'fly') {
      flyT += dt;
      flyVY += 620 * dt;
      flyX += flyVX * dt;
      flyY += flyVY * dt;
      embersBudget += 170 * dt;
      while (embersBudget >= 1) {
        embersBudget -= 1;
        spawnSpark(
          flyX + (Math.random() - 0.5) * 5,
          flyY + (Math.random() - 0.5) * 5,
          -flyVX * 0.25 + (Math.random() - 0.5) * 90,
          (Math.random() - 0.5) * 90,
          0.4 + Math.random() * 0.5,
          Math.random() < 0.3 ? 2 : 1,
          0.7 + Math.random() * 0.3
        );
      }
      if (flyT > 0.34 || flyX > width - 24 || flyY < 12 || flyY > height - 12) explode();
      return;
    }
    const { x: mx, y: my } = getPointer();
    const hover = mx >= 0 && mx <= width && my >= 0 && my <= height;
    if (!hover) {
      ballX = null;
      ballY = null;
      pmx = -1e9;
      pvX = 0;
      pvY = 0;
      heat = Math.max(0, heat - dt);
      return;
    }
    // Cursor velocity, smoothed - the breakout launches along it.
    if (pmx > -1e8) {
      pvX = pvX * 0.75 + ((mx - pmx) / dt) * 0.25;
      pvY = pvY * 0.75 + ((my - pmy) / dt) * 0.25;
    }
    pmx = mx;
    pmy = my;
    const tx = Math.min(lead, Math.max(8, mx + wheelOff));
    if (ballX === null) {
      ballX = tx;
      ballY = null;
      ballVX = 0;
      heat = 0;
    }
    const prev = ballX;
    let step = (tx - ballX) * Math.min(1, dt * BALL_CHASE);
    const maxStep = BALL_VMAX * dt;
    if (step > maxStep) step = maxStep;
    else if (step < -maxStep) step = -maxStep;
    ballX += step;
    ballVX = ballVX * 0.8 + ((ballX - prev) / dt) * 0.2;
    // The ball eases toward the curve instead of riding it exactly, so a fast
    // roll across steep sections doesn't snap up and down.
    const onCurve = sampleCurve(ballX).y;
    ballY = ballY === null ? onCurve : ballY + (onCurve - ballY) * Math.min(1, dt * BALL_SETTLE);
    // Rightward sweeps heat fastest - the story runs left to right.
    const gain = Math.min(1, Math.abs(ballVX) / HEAT_VREF) * (ballVX > 0 ? 1 : 0.55);
    heat = Math.min(1, Math.max(0, heat + (gain * HEAT_RISE - HEAT_FALL) * dt));
    embersBudget += heat * heat * 90 * dt;
    while (embersBudget >= 1) {
      embersBudget -= 1;
      const y0 = ballY ?? sampleCurve(ballX).y;
      spawnSpark(
        ballX + (Math.random() - 0.5) * 6,
        y0 + (Math.random() - 0.5) * 6,
        -ballVX * 0.22 + (Math.random() - 0.5) * 60,
        -20 - 60 * Math.random(),
        0.45 + Math.random() * 0.55,
        Math.random() < 0.25 ? 2 : 1,
        heat * (0.5 + 0.5 * Math.random())
      );
    }
    if (ballX > lead - 20 && heat > 0.55 && ballVX > 150) launchBall();
  };

  const draw = (ctx: CanvasRenderingContext2D) => {
    // Ember ball. Sparks first so the live actors draw over them.
    for (const s of sparks) {
      const q = 1 - s.t / s.life;
      ctx.globalAlpha = q * q * 0.85;
      ctx.fillStyle = getTint(s.hot);
      ctx.fillRect(s.x, s.y, s.sz, s.sz);
    }
    ctx.globalAlpha = 1;
    if (ringT >= 0 && ringT < 0.5) {
      const q = ringT / 0.5;
      ctx.beginPath();
      ctx.arc(ringX, ringY, 6 + 68 * (1 - (1 - q) * (1 - q)), 0, Math.PI * 2);
      ctx.globalAlpha = 0.4 * (1 - q);
      ctx.strokeStyle = getTint(1);
      ctx.lineWidth = 1.5;
      ctx.stroke();
      ctx.globalAlpha = 1;
      ctx.lineWidth = 1;
    }
    if (mode === 'roll' && ballX !== null) {
      const v = sampleCurve(ballX);
      const by = ballY ?? v.y;
      const tint = getTint(heat);
      ctx.beginPath();
      ctx.arc(ballX, by, 5 + 7 * heat, 0, Math.PI * 2);
      ctx.globalAlpha = 0.06 + 0.2 * heat;
      ctx.fillStyle = tint;
      ctx.fill();
      ctx.beginPath();
      ctx.arc(ballX, by, 2.5 + 2 * heat, 0, Math.PI * 2);
      ctx.globalAlpha = 0.55 + 0.4 * heat;
      ctx.fillStyle = tint;
      ctx.fill();
      ctx.globalAlpha = 1;
      ctx.font = `10px ${mono}`;
      ctx.textAlign = 'center';
      ctx.fillStyle = `rgba(${getInk()}, 0.55)`;
      ctx.fillText(
        PRICE_FMT.format(v.p),
        Math.min(getBounds().lead - 30, Math.max(30, ballX)),
        by - 14 - 4 * heat
      );
    } else if (mode === 'fly') {
      // A streaking comet between the line's end and the burst.
      const tint = getTint(1);
      for (let k = 3; k >= 0; k--) {
        ctx.beginPath();
        ctx.arc(flyX - flyVX * 0.012 * k, flyY - flyVY * 0.012 * k, 4 - k * 0.7, 0, Math.PI * 2);
        ctx.globalAlpha = 0.85 - k * 0.2;
        ctx.fillStyle = tint;
        ctx.fill();
      }
      ctx.globalAlpha = 1;
    }
  };

  const onWheel = (deltaX: number) => {
    if (mode !== 'roll') return;
    // Bounded push: a fast fling rolls the ball across the pane - it never
    // slams to the edge and vanishes in a blink.
    wheelOff = Math.min(420, Math.max(-420, wheelOff + deltaX * 1.8));
    if (deltaX > 0) heat = Math.min(1, heat + Math.min(0.05, deltaX * 0.001));
  };

  const onLeave = () => {
    if (mode === 'roll') {
      // Overshooting the seam is the most natural breakout gesture - the
      // cursor carries the hot ball right off the pane. Launch it instead
      // of dropping it; anything colder just dissolves as before. The x
      // floor is generous: a fast flick leaves the ball trailing well
      // behind the cursor when the leave fires.
      const { lead } = getBounds();
      if (ballX !== null && heat > 0.45 && ballVX > 140 && ballX > lead - 260) {
        launchBall();
      } else {
        ballX = null;
        ballY = null;
        heat = 0;
        wheelOff = 0;
      }
    }
    pmx = -1e9;
    pmy = -1e9;
    pvX = 0;
    pvY = 0;
  };

  return { update, draw, onWheel, onLeave };
}
