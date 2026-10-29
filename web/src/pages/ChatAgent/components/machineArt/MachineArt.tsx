/**
 * Front elevations of the machines a computer runs on, for the Computers
 * dialog. A machine the user brings is drawn in millimetres from its maker's
 * own front shots (Apple's for the Macs), so proportions and the small parts
 * land where the real ones do; a hosted one is a box of our own, sized by its
 * spec. Then scaled to fit: hairlines keep their 0.6px at any size.
 *
 * The drawing carries its own shading (DESIGN.md § Illustrations): flat tones
 * from tokens.css, lit from the upper left, never a gradient. The one colour
 * in it is the machine's light, which says what its status line says.
 */
import { useEffect, useId, useState, type CSSProperties, type ReactNode } from 'react';

import { DURATION } from '@/lib/motion';
import type { ResourceTier } from '@/types/api';

import type { MachineArtKind, MachineState } from './machine';
import './machineArt.css';

type Tone = 'top' | 's3' | 's2' | 's1' | 's0';
type Line = 'mid' | 'lo';

/** A rounded rectangle with its own top and bottom radii. */
function rp(x: number, y: number, w: number, h: number, rt: number, rb = rt): string {
  return (
    `M${x + rt} ${y}H${x + w - rt}Q${x + w} ${y} ${x + w} ${y + rt}V${y + h - rb}` +
    `Q${x + w} ${y + h} ${x + w - rb} ${y + h}H${x + rb}Q${x} ${y + h} ${x} ${y + h - rb}V${y + rt}Q${x} ${y} ${x + rt} ${y}Z`
  );
}

/** useId's colons and guillemets are not valid in a `url(#…)` reference. */
function useClipId(): string {
  return `ma${useId().replace(/[^\w-]/g, '')}`;
}

interface AluProps {
  x: number;
  y: number;
  w: number;
  h: number;
  rt?: number;
  rb?: number;
  /** Width of the curved sides, lit on the left and shaded off on the right. */
  side?: number;
  tone?: Tone;
  line?: Line;
}

/** A body with rounded sides, the way light falls on curved aluminium. */
function Alu({ x, y, w, h, rt = 4, rb = rt, side = 0, tone = 's3', line = 'mid' }: AluProps) {
  const id = useClipId();
  const d = rp(x, y, w, h, rt, rb);
  return (
    <>
      <path className={`ma-${tone}`} d={d} />
      {side > 0 && (
        <>
          <clipPath id={id}>
            <path d={d} />
          </clipPath>
          <g clipPath={`url(#${id})`}>
            <rect x={x} y={y} width={side * 0.12} height={h} className="ma-s2" />
            <rect x={x + side * 0.12} y={y} width={side * 0.5} height={h} className="ma-top" />
            <rect x={x + w - side} y={y} width={side} height={h} className="ma-s2" />
            <rect x={x + w - side * 0.5} y={y} width={side * 0.5} height={h} className="ma-s1" />
            <rect x={x + w - side * 0.16} y={y} width={side * 0.16} height={h} className="ma-s0" />
          </g>
        </>
      )}
      <path className={`ma-edge ma-edge-${line}`} d={d} />
    </>
  );
}

function Halo({ cx, rx, ry }: { cx: number; rx: number; ry: number }) {
  return <ellipse className="ma-halo" cx={cx} cy={0} rx={rx} ry={ry} />;
}

function Dots({ x, y, cols, rows, gap, r, mesh = false }: { x: number; y: number; cols: number; rows: number; gap: number; r: number; mesh?: boolean }) {
  const dots: ReactNode[] = [];
  for (let i = 0; i < cols * rows; i++) {
    dots.push(<circle key={i} cx={x + (i % cols) * gap} cy={y + Math.floor(i / cols) * gap} r={r} />);
  }
  return <g className={mesh ? 'ma-mesh' : 'ma-dots'}>{dots}</g>;
}

/**
 * The machine's light: lit while it runs, the accent while it gets ready or
 * goes down, a ring when off. Getting ready, it flickers like a disk at work.
 */
function Led({ x, y, state, r = 1.8 }: { x: number; y: number; state: MachineState; r?: number }) {
  if (state === 'off') return <circle cx={x} cy={y} r={r * 0.85} className="ma-ln" />;
  const lit = state === 'on' ? 'ma-led-on' : state === 'prep' ? 'ma-led-prep ma-led-busy' : 'ma-led-prep';
  return <circle cx={x} cy={y} r={r} className={lit} />;
}

/** The app, in miniature, in whichever theme the reader is in. */
function AppScreen({ w, h }: { w: number; h: number }) {
  const side = w * 0.2;
  const main = side + w * 0.05;
  const chart = [[0.08, 0.82], [0.2, 0.7], [0.32, 0.75], [0.44, 0.52], [0.56, 0.58], [0.68, 0.34], [0.8, 0.4], [0.92, 0.2]];
  return (
    <>
      <rect width={w} height={h} className="ma-app-page" />
      <rect width={side} height={h} className="ma-app-side" />
      {[0, 1, 2, 3, 4].map((i) => (
        <rect
          key={i}
          x={side * 0.16}
          y={h * 0.12 + i * h * 0.085}
          width={side * (i ? 0.6 : 0.4)}
          height={h * 0.025}
          rx={1}
          className={i ? 'ma-app-nav' : 'ma-app-accent'}
        />
      ))}
      <rect x={main} y={h * 0.1} width={w * 0.3} height={h * 0.04} rx={1.5} className="ma-app-ink" />
      <rect x={main} y={h * 0.2} width={w * 0.7} height={h * 0.5} rx={3} className="ma-app-card" />
      <polyline
        className="ma-app-line"
        points={chart.map(([x, y]) => `${main + w * 0.7 * x},${h * 0.2 + h * 0.5 * y}`).join(' ')}
      />
      {[0, 1, 2].map((i) => (
        <rect key={i} x={main + i * w * 0.24} y={h * 0.76} width={w * 0.22} height={h * 0.14} rx={2} className="ma-app-tile" />
      ))}
    </>
  );
}

function TermScreen({ w, h }: { w: number; h: number }) {
  const fs = h / 12;
  const line = (i: number, prompt: boolean, text: string) => (
    <text key={i} x={w * 0.05} y={h * 0.16 + i * fs * 1.5} fontSize={fs}>
      {prompt && <tspan className="ma-term-prompt">dev@lab</tspan>}
      {prompt ? `:~$ ${text}` : text}
    </text>
  );
  return (
    <>
      <rect width={w} height={h} className="ma-glass" />
      <g className="ma-term">
        {line(0, true, 'langalpha status')}
        {line(1, false, 'runtime ready')}
        {line(2, true, '▍')}
      </g>
    </>
  );
}

/** A machine getting ready: its setup typing in at a corner of the dark screen,
 *  a line a beat, then a waiting cursor. Small and faint, to be glanced at
 *  rather than read; the picture takes its place once the machine runs. */
function SetupLog({ w, h }: { w: number; h: number }) {
  const fs = h / 17;
  const x = w * 0.07;
  const line = (i: number) => ({ x, y: h * 0.17 + i * fs * 1.55, fontSize: fs, style: { '--ma-line': i } as CSSProperties });
  // Each step, and the word it ends on once done, in a column of its own.
  const steps: [string, string][] = [['folder', 'ok'], ['runtime', 'ok'], ['connect', '']];
  return (
    <g className="ma-term ma-setup">
      <text {...line(0)} className="ma-setup-line">
        <tspan className="ma-term-prompt">$</tspan> langalpha setup
      </text>
      {steps.map(([step, done], i) => (
        <text key={step} {...line(i + 1)} className="ma-setup-line">
          {step}
          {done && <tspan x={x + fs * 5.4}>{done}</tspan>}
        </text>
      ))}
      <text {...line(steps.length + 1)} className="ma-setup-line ma-setup-cursor">▍</text>
    </g>
  );
}

/**
 * True while `on`, and for `ms` after, so what leaves can fade out where it
 * stood rather than vanish under what fades in.
 */
function useLinger(on: boolean, ms: number): boolean {
  const [shown, setShown] = useState(on);
  if (on && !shown) setShown(true);
  useEffect(() => {
    if (on || !shown) return;
    const timer = setTimeout(() => setShown(false), ms);
    return () => clearTimeout(timer);
  }, [on, shown, ms]);
  return on || shown;
}

/** A screen: dark glass, the setup log while the machine gets ready, the
 *  picture fading in while it runs, a faint glare. The log fades out as the
 *  picture fades in: cut at once, a start quicker than the log showed an empty
 *  screen between the two. */
function Screen({ x, y, w, h, state, term = false, rt = 0 }: { x: number; y: number; w: number; h: number; state: MachineState; term?: boolean; rt?: number }) {
  const id = useClipId();
  const setup = useLinger(state === 'prep', DURATION.fold * 1000);
  return (
    <>
      <clipPath id={id}>
        <path d={rp(x, y, w, h, rt, 0)} />
      </clipPath>
      <g clipPath={`url(#${id})`}>
        <g transform={`translate(${x} ${y})`}>
          <rect width={w} height={h} className="ma-glass" />
          {setup && (
            <g className="ma-screen" style={{ opacity: state === 'prep' ? 1 : 0 }}>
              <SetupLog w={w} h={h} />
            </g>
          )}
          <g className="ma-screen" style={{ opacity: state === 'on' ? 1 : 0 }}>
            {term ? <TermScreen w={w} h={h} /> : <AppScreen w={w} h={h} />}
          </g>
          <path className="ma-glare" d={`M${w * 0.5} 0H${w * 0.78}L${w * 0.46} ${h}H${w * 0.18}Z`} />
        </g>
      </g>
    </>
  );
}

/** `hosted` marks a box drawn on the shared stage, which a machine the user brings is kept smaller than. */
function Figure({ box, label, hosted = false, children }: { box: [number, number, number, number]; label: string; hosted?: boolean; children: ReactNode }) {
  const pad = 8;
  const [x0, y0, x1, y1] = box;
  const w = x1 - x0 + 2 * pad;
  const h = y1 - y0 + 2 * pad;
  return (
    <svg className={hosted ? 'machine-art' : 'machine-art is-own'} viewBox={`${x0 - pad} ${y0 - pad} ${w} ${h}`} width={w} height={h} role="img" aria-label={label}>
      {children}
    </svg>
  );
}

/** A Mac Studio (197 × 95) or, smaller, a Mac mini (127 × 50), standing at x0. */
function MacBox({ x0, state, mini = false }: { x0: number; state: MachineState; mini?: boolean }) {
  const id = useClipId();
  const W = mini ? 127 : 197;
  const H = mini ? 50 : 95;
  const fh = mini ? 4 : 8.5;
  const bh = H - fh;
  const py = -H + bh * (mini ? 0.62 : 0.73);
  const foot = rp(x0 + (mini ? 10 : 8), -fh - 1, W - (mini ? 20 : 16), fh + 1, 0, mini ? 1.5 : 3);
  return (
    <>
      <Halo cx={x0 + W / 2} rx={W / 2 + 4} ry={2.6} />
      <clipPath id={id}>
        <path d={foot} />
      </clipPath>
      <path className="ma-s0" d={foot} />
      {!mini && (
        <g clipPath={`url(#${id})`}>
          <Dots x={x0 + 9} y={-fh + 0.4} cols={121} rows={6} gap={1.5} r={0.42} mesh />
        </g>
      )}
      <path className="ma-edge ma-edge-lo" d={foot} />
      <Alu x={x0} y={-H} w={W} h={bh} rt={mini ? 5 : 7} rb={mini ? 3 : 4} side={mini ? 16 : 24} />
      {mini ? (
        <>
          <rect x={x0 + 18} y={py - 3.2} width={2.6} height={6.4} rx={1.3} className="ma-port" />
          <rect x={x0 + 27} y={py - 3.2} width={2.6} height={6.4} rx={1.3} className="ma-port" />
          <circle cx={x0 + 38} cy={py} r={1.6} className="ma-port" />
          <Led x={x0 + W - 16} y={py} state={state} r={1.1} />
        </>
      ) : (
        <>
          <rect x={x0 + 29.4} y={py - 5} width={3.6} height={10} rx={1.8} className="ma-port" />
          <rect x={x0 + 44.2} y={py - 5} width={3.6} height={10} rx={1.8} className="ma-port" />
          <rect x={x0 + 60} y={py - 1.4} width={28} height={2.8} rx={1.4} className="ma-port" />
          <Led x={x0 + 165} y={py - 1.2} state={state} r={1.3} />
        </>
      )}
    </>
  );
}

/** Studio Display: a 623 × 362 panel on a 140-wide arm with its cable hole. */
function StudioDisplay({ state }: { state: MachineState }) {
  const pw = 623;
  const ph = 362;
  const pb = -117;
  const px = -pw / 2;
  const b = 13;
  return (
    <>
      <Halo cx={0} rx={86} ry={2.6} />
      <Alu x={-80} y={-5} w={160} h={5} rt={1.4} rb={1} tone="s2" line="lo" />
      <Alu x={-70} y={pb - 10} w={140} h={-pb + 5} rt={0} rb={0} side={20} line="lo" />
      <circle cx={0} cy={pb + 1} r={12.5} className="ma-hole" />
      <circle cx={0} cy={pb + 1} r={12.5} className="ma-edge ma-edge-lo" />
      <Alu x={px} y={pb - ph} w={pw} h={ph} rt={9} rb={9} tone="s2" />
      <path className="ma-glass" d={rp(px + 1.8, pb - ph + 1.8, pw - 3.6, ph - 3.6, 7.5)} />
      <Screen x={px + b} y={pb - ph + b} w={pw - 2 * b} h={ph - 2 * b} state={state} />
      <circle cx={0} cy={pb - ph + 7} r={1.5} className="ma-camera" />
    </>
  );
}

/**
 * 14-inch MacBook Pro, drawn the way Apple draws its own (the `macbook` SF
 * Symbol): the lid narrower than the base it stands on, rather than the true
 * elevation, where the two are one width and read as a box on a box. Feet,
 * the base with its thumb scoop, the hinge, the lid with its notch.
 */
function MacBook({ state, label }: { state: MachineState; label: string }) {
  const W = 312.6;
  const LW = 256;
  const L = 174;
  const lx = (W - LW) / 2;
  const foot = 1.3;
  const by = -(foot + 10);
  const hy = by - 2;
  const cx = W / 2;
  const base = (
    <>
      <Halo cx={cx} rx={W / 2 + 4} ry={3} />
      <rect x={24} y={-foot - 0.5} width={28} height={foot + 0.5} rx={0.7} className="ma-foot" />
      <rect x={W - 52} y={-foot - 0.5} width={28} height={foot + 0.5} rx={0.7} className="ma-foot" />
      <path className="ma-s0" d={rp(lx + 8, hy, LW - 16, 3, 1)} />
      <path className="ma-edge ma-edge-lo" d={rp(lx + 8, hy, LW - 16, 3, 1)} />
      <Alu x={0} y={by} w={W} h={10} rt={2.5} rb={5} side={18} />
      <rect x={2.5} y={by + 0.6} width={W - 5} height={1.6} rx={0.8} className="ma-top" />
      <path className="ma-s0" d={`M${cx - 21} ${by}v1.6q0 2.4 2.4 2.4h37.2q2.4 0 2.4-2.4v-1.6Z`} />
      <path className="ma-edge ma-edge-mid" d={`M${cx - 21} ${by}v1.6q0 2.4 2.4 2.4h37.2q2.4 0 2.4-2.4v-1.6`} />
    </>
  );
  const sx = lx + 5.4;
  const sy = -L + 5.8;
  return (
    <Figure box={[-2, hy - L, W + 2, 4]} label={label}>
      {base}
      <g transform={`translate(0 ${hy})`}>
        <Alu x={lx} y={-L} w={LW} h={L} rt={10} rb={2} tone="s2" />
        <path className="ma-glass" d={rp(lx + 1.4, -L + 1.4, LW - 2.8, L - 2.8, 8.6, 1.2)} />
        <Screen x={sx} y={sy} w={LW - 10.8} h={L - 5.8 - 9} state={state} rt={3.2} />
        <path className="ma-glass" d={`M${cx - 15} ${sy}h30v3.8q0 2.2-2.2 2.2h-25.6q-2.2 0-2.2-2.2Z`} />
        <circle cx={cx} cy={sy + 2.6} r={0.9} className="ma-camera" />
      </g>
    </Figure>
  );
}

/**
 * A fine grille filling a shape: a pattern rather than a dot per hole, since
 * a large box's front has thousands of them.
 */
function Grille({ d, gap, r }: { d: string; gap: number; r: number }) {
  const id = useClipId();
  return (
    <>
      <pattern id={id} width={gap} height={gap} patternUnits="userSpaceOnUse">
        <circle cx={gap / 2} cy={gap / 2} r={r} className="ma-grille" />
      </pattern>
      <path d={d} fill={`url(#${id})`} />
    </>
  );
}

/**
 * A hosted computer, as a box on a desk sized by its spec: a flat mini, a
 * taller box on a vented foot, then boxes with a mesh front. Our own shapes,
 * not any maker's, with the provider engraved on the front. Every size shares
 * one stage, so a bigger spec is a bigger box beside a smaller one.
 */
function CloudBox({ tier, state, brand, label }: { tier: ResourceTier; state: MachineState; brand: string; label: string }) {
  let body: ReactNode;
  if (tier === 'standard') {
    const W = 130;
    const H = 46;
    const fh = 3;
    const x = -W / 2;
    const foot = rp(x + 12, -fh - 1, W - 24, fh + 1, 0, 1.2);
    body = (
      <>
        <Halo cx={0} rx={W / 2 + 2} ry={2.2} />
        <path className="ma-s0" d={foot} />
        <path className="ma-edge ma-edge-lo" d={foot} />
        <Alu x={x} y={-H} w={W} h={H - fh} rt={6} rb={4} side={16} />
        <text className="ma-etch" x={x + 12} y={-fh - 9} fontSize={4.2}>{brand}</text>
        <Led x={W / 2 - 13} y={-fh - 10.4} state={state} r={1.2} />
      </>
    );
  } else if (tier === 'performance') {
    const W = 176;
    const H = 92;
    const fh = 8;
    const x = -W / 2;
    const bh = H - fh;
    const foot = rp(x + 8, -fh - 1, W - 16, fh + 1, 0, 2.6);
    body = (
      <>
        <Halo cx={0} rx={W / 2 + 3} ry={2.6} />
        <path className="ma-s0" d={foot} />
        <Grille d={foot} gap={1.5} r={0.42} />
        <path className="ma-edge ma-edge-lo" d={foot} />
        <Alu x={x} y={-H} w={W} h={bh} rt={8} rb={4} side={22} />
        <path className="ma-ln ma-ln-faint" d={`M${x + 16} ${-H + bh * 0.64}H${W / 2 - 16}`} />
        <text className="ma-etch" x={x + 16} y={-fh - 13} fontSize={5}>{brand}</text>
        <Led x={W / 2 - 18} y={-fh - 14.6} state={state} r={1.4} />
      </>
    );
  } else {
    // Large has one mesh front; Extra Large, taller, has two.
    const xl = tier === 'xlarge';
    const W = xl ? 232 : 212;
    const H = xl ? 152 : 124;
    const fh = 4;
    const x = -W / 2;
    const bh = H - fh;
    const mx = x + 12;
    const mw = W - 24;
    const top = -H + 12;
    const room = bh - 34;
    const panels = xl ? [rp(mx, top, mw, (room - 6) / 2, 4), rp(mx, top + (room + 6) / 2, mw, (room - 6) / 2, 4)] : [rp(mx, top, mw, room, 4)];
    body = (
      <>
        <Halo cx={0} rx={W / 2 + 4} ry={3} />
        <rect x={x + 18} y={-fh} width={30} height={fh} rx={1.2} className="ma-foot" />
        <rect x={W / 2 - 48} y={-fh} width={30} height={fh} rx={1.2} className="ma-foot" />
        <Alu x={x} y={-H} w={W} h={bh} rt={xl ? 10 : 9} rb={6} side={xl ? 26 : 24} />
        {panels.map((d) => (
          <g key={d}>
            <path className="ma-s1" d={d} />
            <Grille d={d} gap={2.2} r={0.55} />
            <path className="ma-edge ma-edge-lo" d={d} />
          </g>
        ))}
        <text className="ma-etch" x={mx} y={-fh - 9} fontSize={5.4}>{brand}</text>
        <Led x={W / 2 - 18} y={-fh - 10.8} state={state} r={1.6} />
      </>
    );
  }
  return (
    <Figure box={[-120, -156, 120, 4]} label={label} hosted>
      {body}
    </Figure>
  );
}

/** A plain monitor on a neck, and a tower beside it with its mesh front. */
function LinuxDesk({ state, label }: { state: MachineState; label: string }) {
  const px = -265;
  const pw = 530;
  const ph = 318;
  const pb = -128;
  const tx = 300;
  const tw = 190;
  const th = 420;
  return (
    <Figure box={[px - 2, -th, tx + tw + 2, 6]} label={label}>
      <Halo cx={0} rx={112} ry={3} />
      <Halo cx={tx + tw / 2} rx={tw / 2 + 6} ry={3.4} />
      <Alu x={-105} y={-8} w={210} h={8} rt={2.5} rb={1.5} tone="s2" line="lo" />
      <Alu x={-24} y={pb} w={48} h={-pb - 8} rt={0} rb={0} side={12} tone="s2" line="lo" />
      <path className="ma-glass" d={rp(px, pb - ph, pw, ph, 6)} />
      <path className="ma-edge ma-edge-mid" d={rp(px, pb - ph, pw, ph, 6)} />
      <Screen x={px + 9} y={pb - ph + 9} w={pw - 18} h={ph - 28} state={state} term />
      <Led x={px + pw - 22} y={pb - 9.5} state={state} r={1.4} />
      <rect x={tx + 18} y={-6} width={34} height={6} rx={1.5} className="ma-foot" />
      <rect x={tx + tw - 52} y={-6} width={34} height={6} rx={1.5} className="ma-foot" />
      <Alu x={tx} y={-th} w={tw} h={th - 4} rt={6} rb={3} side={18} />
      <rect x={tx + 16} y={-th + 52} width={tw - 32} height={th - 82} rx={3} className="ma-s2" />
      <rect x={tx + 16} y={-th + 52} width={tw - 32} height={th - 82} rx={3} className="ma-ln" />
      <Dots x={tx + 24} y={-th + 60} cols={24} rows={52} gap={6} r={0.85} />
      <rect x={tx + 22} y={-th + 22} width={12} height={4.6} rx={1} className="ma-port" />
      <rect x={tx + 40} y={-th + 22} width={12} height={4.6} rx={1} className="ma-port" />
      <circle cx={tx + 64} cy={-th + 24.3} r={2.4} className="ma-port" />
      <circle cx={tx + tw - 34} cy={-th + 24.3} r={8} className="ma-ln ma-ln-mid" />
      <Led x={tx + tw - 34} y={-th + 24.3} state={state} r={2.6} />
    </Figure>
  );
}

interface MachineArtProps {
  art: MachineArtKind;
  state: MachineState;
  /** What the drawing shows, for a screen reader. */
  label: string;
  /** Engraved on a cloud machine's front. */
  brand?: string;
  /** A cloud machine's spec, which sizes its box. */
  tier?: ResourceTier;
}

export function MachineArt({ art, state, label, brand = '', tier = 'standard' }: MachineArtProps) {
  switch (art) {
    case 'macbook':
      return <MacBook state={state} label={label} />;
    case 'cloud':
      return <CloudBox tier={tier} state={state} brand={brand.toUpperCase()} label={label} />;
    case 'linux':
      return <LinuxDesk state={state} label={label} />;
    case 'imac':
      return (
        <Figure box={[-313.5, -479, 313.5, 4]} label={label}>
          <StudioDisplay state={state} />
        </Figure>
      );
    case 'macMini':
    case 'macStudio': {
      // Beside the display, aligned to its right edge.
      const mini = art === 'macMini';
      return (
        <Figure box={[-313.5, -479, 313.5, 4]} label={label}>
          <StudioDisplay state={state} />
          <MacBox x0={311.5 - (mini ? 127 : 197)} state={state} mini={mini} />
        </Figure>
      );
    }
  }
}

/**
 * A machine the user brings, before it is reached: the two it could turn out
 * to be, a laptop and a Linux desk, as dashed outlines on one floor.
 */
export function MachineGhost() {
  const lid = rp(14, -99, 132, 91, 7, 1.5);
  return (
    <svg className="machine-art machine-ghost" viewBox="-6 -130 438 136" width={438} height={136} aria-hidden="true">
      <path d={lid} />
      <path d={rp(22, -90, 116, 78, 3, 0)} />
      <path d={rp(0, -6, 160, 6, 1.5, 3)} />
      <path d={rp(204, -124, 152, 94, 4)} />
      <path d={`M272 -30V-4M290 -30V-4`} />
      <path d={rp(250, -4, 62, 4, 1.5, 1)} />
      <path d={rp(372, -124, 54, 124, 4, 2)} />
      <path d="M382 -112h12" />
      <circle cx={414} cy={-110} r={3.5} />
    </svg>
  );
}

