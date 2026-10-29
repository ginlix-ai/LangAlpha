import React, { createContext, useContext, useState } from 'react';
import { AnimatePresence, motion } from '@/lib/framer';
import { DURATION, EASE_OUT, TWEEN } from '@/lib/motion';
import { cn } from '@/lib/utils';

/**
 * One state replacing another in place (DESIGN.md § Motion): both are stacked
 * in one grid cell, so the cell is never empty between them and nothing around
 * it moves for the swap. The incoming state arrives over quick unless told
 * otherwise, and the outgoing one leaves over the exit beat. What is leaving is still on screen
 * but no longer there for anyone: it is hidden from a screen reader, which
 * would otherwise read one name twice, and inert, so a click on the part of it
 * that outreaches the incoming state cannot land on a control already gone.
 */

const GRID_STYLE = { display: 'grid', gridTemplateColumns: 'minmax(0, 1fr)', minWidth: 0 } as const;
/** How far a state rises in, and the one it replaces lifts out. */
const RISE_PX = 5;
const LAYER_STYLE = { gridArea: '1 / 1', minWidth: 0 } as const;
// Where a leaving layer sits once out of flow: where it was, cut to the cell.
// A taller one left hanging out would give a scrolling box around the cell
// more to scroll, and flash its scrollbar until the layer is gone.
const LEAVING_STYLE = { position: 'absolute', inset: 0, overflow: 'clip' } as const;

/** The cell's latest swap: which key is on show, and how the swap travels.
 *  Handed to the layers through context rather than framer's `custom`, which
 *  would also reach motion nested in a layer. A layer that is leaving still
 *  reads it, so it knows it is leaving, and leaves the way the latest swap
 *  travels rather than the way the swap that brought it in did. */
interface Swap {
  current: string | null;
  shift?: { x?: number; y?: number };
  /** Whether the latest swap rises, in a cell that can; `null` in one that
   *  cannot. A cell that can always writes its layers' `y`, since a value
   *  dropped from a layer's targets springs back to its first `initial`. */
  rise: boolean | null;
  scale: boolean;
  enterDelay: number;
  enterDuration: number;
  /** A leaving layer fades out over the cell rather than holding it open. */
  sizeToIncoming: boolean;
}

const SwapContext = createContext<Swap>({ current: null, rise: null, scale: false, enterDelay: 0, enterDuration: DURATION.quick, sizeToIncoming: false });

// Only the keys a swap moves are written, so a plain fade leaves the layer
// without a transform at all.
function placed({ shift, rise, scale }: Swap, at: 0 | 1 | -1) {
  return {
    ...(shift?.x ? { x: at * shift.x } : null),
    ...(shift?.y ? { y: at * shift.y } : null),
    ...(rise !== null ? { y: rise ? at * RISE_PX : 0 } : null),
    ...(scale ? { scale: at === 0 ? 1 : 0.8 } : null),
  };
}

interface SwapCellProps {
  /** What the cell shows: a new key crossfades to the new children, and
   *  `null` lets the last state go with nothing in its place. */
  swapKey: string | null;
  className?: string;
  /** Laid out on each layer, so a layer can be the flex line it replaces. */
  layerClassName?: string;
  /** Spans by default, so a cell can sit inside a button; `div` for a cell of
   *  blocks. */
  as?: 'span' | 'div';
  /** Where a state arrives from, in px. It leaves the opposite way, so the
   *  swap reads as travel: down the page, or toward the step a tour went to. */
  shift?: { x?: number; y?: number };
  /** Grows what arrives and shrinks what leaves, for a glyph. */
  scale?: boolean;
  /** What the shown state is a state of. A swap to another one rises: the new
   *  state rises in a few px as the old one lifts out, so a new step reads as
   *  the next one. A swap that keeps it is the same thing in another form,
   *  and crossfades. */
  riseKey?: string;
  /** Seconds the incoming state waits, for one that should arrive once the
   *  outgoing one has gone rather than over it. */
  enterDelay?: number;
  /** Seconds the incoming state takes to arrive: quick for a state of a
   *  control, the entrance beat for a whole page told one at a time. */
  enterDuration?: number;
  /** Plays the first state's entrance on mount too. By default a cell that
   *  mounts settled (history, a reconnect, a page coming back) just is. */
  appear?: boolean;
  /** Takes the incoming state's size at once, the leaving one fading out over
   *  it, for a cell in a box that resizes to fit (a dialog's body): stacked,
   *  the box would hold the taller state's size until it had gone, and only
   *  then start to resize. By default a cell keeps the larger of the two, so
   *  nothing around it moves for the swap. */
  sizeToIncoming?: boolean;
  children?: React.ReactNode;
}

/**
 * A state drawn in place that crossfades to the next when `swapKey` changes.
 * Never `mode="wait"`, which leaves the cell empty between the two.
 */
export function SwapCell({
  swapKey,
  className,
  layerClassName,
  as = 'span',
  shift,
  scale = false,
  riseKey,
  enterDelay = 0,
  enterDuration = DURATION.quick,
  appear = false,
  sizeToIncoming = false,
  children,
}: SwapCellProps): React.ReactElement {
  const Root = as;
  // Whether the latest swap travels, kept until the next one: the layers
  // still moving read it for as long as they move.
  const [latest, setLatest] = useState({ swapKey, riseKey, rises: false });
  let swap = latest;
  if (swapKey !== latest.swapKey) {
    swap = { swapKey, riseKey, rises: riseKey !== latest.riseKey };
    setLatest(swap);
  }
  const rise = riseKey === undefined ? null : swap.rises;
  return (
    <Root className={cn(className)} style={sizeToIncoming ? { ...GRID_STYLE, position: 'relative' } : GRID_STYLE}>
      <SwapContext value={{ current: swapKey, shift, rise, scale, enterDelay, enterDuration, sizeToIncoming }}>
        <AnimatePresence initial={appear}>
          {swapKey !== null && (
            <SwapLayer key={swapKey} layerKey={swapKey} as={as} className={layerClassName}>
              {children}
            </SwapLayer>
          )}
        </AnimatePresence>
      </SwapContext>
    </Root>
  );
}

function SwapLayer({
  layerKey,
  as,
  className,
  children,
}: {
  layerKey: string;
  as: 'span' | 'div';
  className?: string;
  children: React.ReactNode;
}): React.ReactElement {
  const swap = useContext(SwapContext);
  // A leaving layer keeps the props it last had, its own key among them,
  // while the context moves on to the key that replaced it.
  const present = swap.current === layerKey;
  const Layer = as === 'div' ? motion.div : motion.span;
  return (
    <Layer
      className={className}
      style={!present && swap.sizeToIncoming ? { ...LAYER_STYLE, ...LEAVING_STYLE } : LAYER_STYLE}
      aria-hidden={present ? undefined : true}
      inert={!present}
      initial={{ opacity: 0, ...placed(swap, 1) }}
      animate={{
        opacity: 1,
        ...placed(swap, 0),
        transition: { duration: swap.enterDuration, ease: EASE_OUT, delay: swap.enterDelay },
      }}
      exit={{ opacity: 0, ...placed(swap, -1), transition: TWEEN.exit }}
    >
      {children}
    </Layer>
  );
}

interface InPlaceSwapProps {
  /** Which of the two shows: `to` when true. */
  swapped: boolean;
  from: React.ReactNode;
  to: React.ReactNode;
  /** Shrink what leaves and grow what arrives, for an icon; a label only fades. */
  scale?: boolean;
  className?: string;
}

/**
 * Two states of one control, both kept mounted in one cell, so the cell is
 * always as wide as the wider of them and the control never changes size as it
 * swaps. The one not shown is hidden and inert like a leaving layer, for good.
 * CSS transitions rather than framer, since nothing mounts or unmounts.
 * Nothing animates on mount.
 */
export function InPlaceSwap({ swapped, from, to, scale = false, className }: InPlaceSwapProps): React.ReactElement {
  const layer = (shown: boolean, content: React.ReactNode) => (
    <span
      aria-hidden={shown ? undefined : true}
      inert={!shown}
      className={cn(
        'col-start-1 row-start-1 inline-flex items-center gap-1 transition-[opacity,scale] ease-out',
        shown ? 'duration-quick' : 'duration-exit',
      )}
      style={{ opacity: shown ? 1 : 0, scale: scale && !shown ? 0.8 : 1 }}
    >
      {content}
    </span>
  );
  return (
    <span className={cn('inline-grid items-center', className)}>
      {layer(!swapped, from)}
      {layer(swapped, to)}
    </span>
  );
}
