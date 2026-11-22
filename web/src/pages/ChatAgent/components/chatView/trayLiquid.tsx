/**
 * The tray's fold drawn as liquid: the chips' shapes flow together into one
 * drop that falls into the composer's toolbar and becomes the dock, which
 * pours them back out the same way. `useTrayFold` says when; this draws it.
 */
import { useId, useLayoutEffect, useState, type ReactNode, type Ref, type RefObject } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronUp } from 'lucide-react';
import { motion, type Easing } from '@/lib/framer';
import { DURATION } from '@/lib/motion';
import PulseDot from '@/components/ui/pulse-dot';
import { cn } from '@/lib/utils';
import { RunGlyph } from '../messageList/runCard';
import type { LiquidPhase, TrayFoldPhase } from './useTrayFold';
import { trayWorking, useThreadStatus } from './threadStatus';
import { CHIP_ORDER, type TrayContents } from './useTrayContents';

/** Seconds each shape of a liquid run takes, and between one shape and the next. */
export const SHAPE_RUN = 0.5;
const SHAPE_STAGGER = 0.045;

/** One liquid run of `shapes` shapes, the last one's stagger included. */
function liquidRunMs(shapes: number): number {
  return (SHAPE_RUN + SHAPE_STAGGER * Math.max(0, shapes - 1)) * 1000;
}

/**
 * How long a run may go before the fold moves on without hearing it land:
 * the longest run there can be (every chip and the Status button), and a few
 * frames more. It only ends a run that never drew (nothing to measure) or
 * went before it landed; a run that draws ends on its own last shape.
 */
export const LIQUID_FALLBACK_MS = liquidRunMs(CHIP_ORDER.length + 1) + 250;

// The liquid and the dock take the tag gray, which the page color the chips
// sit on would hide: the chips fade off a gray body, and the drop it becomes
// lands as the dock.
const TRAY_FILL = 'bg-(--color-bg-tag)';

interface Box { x: number; y: number; w: number; h: number }

interface LiquidRun {
  phase: LiquidPhase;
  /** The layer's box in the tray row; everything else is in the layer. */
  box: Box;
  chips: Box[];
  dock: Box;
}

/** Room around every shape for the blur that joins them. */
const BLUR_ROOM = 24;

function measureRun(
  row: HTMLElement | null,
  chips: HTMLElement | null,
  dock: HTMLElement | null,
): Omit<LiquidRun, 'phase'> | null {
  if (!row || !chips || !dock) return null;
  const rects = [...chips.children].map((el) => el.getBoundingClientRect());
  const dockRect = dock.getBoundingClientRect();
  if (rects.length === 0 || dockRect.width === 0) return null;
  const all = [...rects, dockRect];
  const left = Math.min(...all.map((r) => r.left)) - BLUR_ROOM;
  const top = Math.min(...all.map((r) => r.top)) - BLUR_ROOM;
  const right = Math.max(...all.map((r) => r.right)) + BLUR_ROOM;
  const bottom = Math.max(...all.map((r) => r.bottom)) + BLUR_ROOM;
  const origin = row.getBoundingClientRect();
  const local = (r: DOMRect): Box => ({ x: r.left - left, y: r.top - top, w: r.width, h: r.height });
  return {
    box: { x: left - origin.left, y: top - origin.top, w: right - left, h: bottom - top },
    chips: rects.map(local),
    dock: local(dockRect),
  };
}

interface TrayLiquidProps {
  /** Mounted only while the liquid runs. */
  phase: LiquidPhase;
  /** The tray's row, which the layer is placed in. */
  trayRef: RefObject<HTMLElement | null>;
  /** The chips, whose shapes the liquid starts or ends in. */
  chipsRef: RefObject<HTMLElement | null>;
  dockRef: RefObject<HTMLElement | null>;
  /** The run's last shape landed (`useTrayFold`'s `settle`). */
  onSettled: (ran: LiquidPhase) => void;
}

/** The liquid under the chips while the tray folds or unfolds. */
export function TrayLiquid({ phase, trayRef, chipsRef, dockRef, onSettled }: TrayLiquidProps) {
  const [run, setRun] = useState<LiquidRun | null>(null);
  // Measured once the phase is in the DOM and before it paints, so the first
  // frame that drops the chips' fill already has their shapes under them.
  useLayoutEffect(() => {
    const geometry = measureRun(trayRef.current, chipsRef.current, dockRef.current);
    setRun(geometry && { phase, ...geometry });
  }, [phase, trayRef, chipsRef, dockRef]);
  return run?.phase === phase ? <LiquidLayer key={run.phase} run={run} onSettled={onSettled} /> : null;
}

/**
 * The chips' fills, drawn through a goo filter: a blur, then an alpha cut
 * that turns the blur's soft edge back into a hard one. Two shapes whose blurs
 * overlap cross the cut together, so they read as one body of liquid, joined
 * by a neck that thins and snaps as they part. Above the composer, so the drop
 * can fall into its toolbar: it lands in the dock's own shape, and the dock
 * takes its place without a seam.
 */
function LiquidLayer({ run, onSettled }: { run: LiquidRun; onSettled: (ran: LiquidPhase) => void }) {
  // React's ids are word characters only (`_r_1_`), so one is valid in
  // url(#…) as it is.
  const filterId = `tray-goo-${useId()}`;
  const { box, chips, dock, phase } = run;
  // Nearest the dock first: the pills beside it reach it first and the rest
  // run into them, so they merge one by one on the way, and split that way.
  const dockX = dock.x + dock.w / 2;
  const rank: number[] = [];
  chips
    .map((chip, i) => ({ i, distance: Math.abs(chip.x + chip.w / 2 - dockX) }))
    .sort((a, b) => a.distance - b.distance)
    .forEach(({ i }, place) => {
      rank[i] = place;
    });
  // Every shape runs as long, so the one set off last lands last.
  const last = chips.length - 1;

  return (
    <div
      aria-hidden="true"
      className="pointer-events-none absolute z-20"
      style={{ left: box.x, top: box.y, width: box.w, height: box.h }}
    >
      <svg className="absolute h-0 w-0">
        <filter id={filterId} colorInterpolationFilters="sRGB">
          <feGaussianBlur stdDeviation="5" />
          <feColorMatrix values="1 0 0 0 0  0 1 0 0 0  0 0 1 0 0  0 0 0 18 -7" />
        </filter>
      </svg>
      <div className="absolute inset-0" style={{ filter: `url(#${filterId})` }}>
        {chips.map((chip, i) => (
          <LiquidShape
            key={i}
            chip={chip}
            dock={dock}
            unfolding={phase === 'unfolding'}
            delay={rank[i] * SHAPE_STAGGER}
            onLanded={rank[i] === last ? () => onSettled(phase) : undefined}
          />
        ))}
      </div>
    </div>
  );
}

interface ShapeFrame { cx: number; cy: number; w: number; h: number }

/** The goo cut sits outside a shape's edge; this much in, it matches the dock. */
const CUT_SPREAD = 1;

function LiquidShape({ chip, dock, unfolding, delay, onLanded }: {
  chip: Box;
  dock: Box;
  unfolding: boolean;
  delay: number;
  onLanded?: () => void;
}) {
  const cx = chip.x + chip.w / 2;
  const cy = chip.y + chip.h / 2;
  const dx = dock.x + dock.w / 2;
  const dy = dock.y + dock.h / 2;
  const ballY = cy + chip.h * 0.4;
  // The pill, narrowing as it runs toward the dock's column; a ball there,
  // just under the tray; a drop drawn long by its fall; the dock.
  const pill: ShapeFrame = { cx, cy, w: chip.w, h: chip.h };
  const running: ShapeFrame = { cx: cx + (dx - cx) * 0.8, cy: cy + 1, w: chip.h + (chip.w - chip.h) * 0.25, h: chip.h };
  const ball: ShapeFrame = { cx: dx, cy: ballY, w: chip.h * 0.92, h: chip.h * 0.92 };
  const falling: ShapeFrame = { cx: dx, cy: (ballY + dy) / 2, w: chip.h * 0.7, h: chip.h * 1.2 };
  const landed: ShapeFrame = { cx: dx, cy: dy, w: dock.w - CUT_SPREAD * 2, h: dock.h - CUT_SPREAD * 2 };
  const frames = unfolding ? [landed, falling, ball, running, pill] : [pill, running, ball, falling, landed];
  // Folding: slow off the tray, faster as it falls, and a splash as it lands.
  // Unfolding: drawn up out of the dock, and the last stretch overshoots a
  // little and settles.
  const times = unfolding ? [0, 0.24, 0.48, 0.64, 1] : [0, 0.34, 0.5, 0.74, 1];
  const ease: Easing[] = unfolding
    ? ['easeIn', 'easeOut', 'easeIn', [0.3, 1.35, 0.5, 1]]
    : ['easeInOut', 'easeOut', 'easeIn', [0.3, 1.4, 0.5, 1]];
  const at = (frame: ShapeFrame) => ({ left: frame.cx - frame.w / 2, top: frame.cy - frame.h / 2, width: frame.w, height: frame.h });

  return (
    <motion.span
      className={cn('absolute rounded-full', TRAY_FILL)}
      initial={at(frames[0])}
      animate={{
        left: frames.map((f) => at(f).left),
        top: frames.map((f) => at(f).top),
        width: frames.map((f) => f.w),
        height: frames.map((f) => f.h),
      }}
      transition={{ duration: SHAPE_RUN, times, ease, delay }}
      onAnimationComplete={onLanded}
    />
  );
}

/**
 * The tray's row, over the liquid, which starts and ends in its shapes and
 * fill: the chips fade out from over it as a fold sets off, and back in onto
 * it as an unfold's last shape lands, when the liquid under them goes. Their
 * own fills stay put, since a chip's color transition would blink one in late.
 */
export function LiquidRow({ ref, phase, shapes, className, children, ...mark }: {
  ref: Ref<HTMLDivElement>;
  phase: TrayFoldPhase;
  /** The row's children, which the liquid draws a shape each for. */
  shapes: number;
  className: string;
  children: ReactNode;
  'data-tray-layout'?: string;
}) {
  const liquid = phase === 'folding' || phase === 'unfolding';
  const landed = liquidRunMs(shapes) / 1000 - DURATION.quick;
  return (
    <motion.div
      ref={ref}
      {...mark}
      inert={liquid}
      className={cn(className, liquid && 'relative z-30')}
      initial={{ opacity: phase === 'unfolding' ? 0 : 1 }}
      animate={{ opacity: phase === 'folding' ? 0 : 1 }}
      transition={phase === 'unfolding' ? { delay: landed, duration: DURATION.quick } : { duration: DURATION.exit }}
    >
      {children}
    </motion.div>
  );
}

interface TrayDockProps {
  ref?: Ref<HTMLButtonElement>;
  phase: TrayFoldPhase;
  contents: TrayContents;
  onOpen: () => void;
}

/**
 * The folded tray, in the composer's toolbar: the drop the chips ran into,
 * which pours them back out when clicked. In the layout but unseen while the
 * liquid runs, so the drop has a place to land and to rise from; the swap is
 * a single frame, since the drop ends in the dock's shape and fill.
 */
export function TrayDock({ ref, phase, contents, onOpen }: TrayDockProps) {
  const { t } = useTranslation();
  const label = t('chat.tray.show');
  const folded = phase === 'folded';
  const status = useThreadStatus();
  const working = !!status && trayWorking(contents, status.work);
  return (
    <button
      ref={ref}
      type="button"
      aria-label={label}
      title={label}
      inert={!folded}
      onClick={onOpen}
      className={cn(
        phase === 'open' ? 'hidden' : 'inline-flex',
        'tap-target h-6 flex-none items-center gap-1.5 rounded-full px-2 text-(--color-text-tertiary) outline-hidden transition-colors',
        TRAY_FILL,
        'hover:text-(--color-text-secondary) focus-visible:ring-2 focus-visible:ring-ring',
        !folded && 'opacity-0',
      )}
    >
      {/* What it holds surfaces once the drop has settled. */}
      <span
        className={cn(
          'inline-flex items-center gap-1.5',
          folded ? 'opacity-100 transition-opacity duration-quick motion-reduce:transition-none' : 'opacity-0',
        )}
      >
        {contents.watch && <PulseDot />}
        {working && <RunGlyph kind="live" />}
        <ChevronUp aria-hidden="true" className="h-3 w-3 shrink-0" />
      </span>
    </button>
  );
}
