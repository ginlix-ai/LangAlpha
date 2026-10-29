import React, { useContext } from 'react';
import { AnimatePresence, motion } from '@/lib/framer';
import { X as XIcon } from 'lucide-react';
import { TextShimmer } from '@/components/ui/text-shimmer';
import { Loader } from '@/components/ui/loader';
import { DURATION, TWEEN } from '@/lib/motion';
import { useMotionSafe } from '@/lib/reducedMotion';
import { SwapCell } from '@/components/ui/crossfade';
import { RunningGlyphContext } from './runningGlyph';

export function RunningTitle({ label }: { label: string }): React.ReactElement {
  return (
    <span className="titem-title inline-flex min-w-0" style={{ flexShrink: 1 }}>
      <TextShimmer
        as="span"
        className="truncate [--base-color:var(--Labels-Secondary)] [--base-gradient-color:var(--color-text-primary)]"
        duration={1.5}
      >
        {label}
      </TextShimmer>
    </span>
  );
}

export function RunningIcon({ running, children }: { running: boolean; children: React.ReactNode }): React.ReactElement {
  // An opacity loop, which framer's reduced-motion setting does not reach.
  const motionSafe = useMotionSafe();
  const pulse = running && motionSafe;
  return (
    <motion.span
      animate={pulse ? { opacity: [0.85, 1, 0.85] } : { opacity: 1 }}
      // eslint-disable-next-line no-restricted-syntax -- the pulse's loop period, not a transition between states
      transition={pulse ? { duration: 1.5, repeat: Infinity, ease: 'easeInOut' } : { duration: DURATION.quick }}
      style={{ display: 'inline-flex' }}
    >
      {children}
    </motion.span>
  );
}

const GLYPH_CELL = 'h-5 place-items-center';

/** A row's icon cell: the tool icon, pulsing while the call runs (unless
 *  `pulse` is off), or on the status line the loader glyph crossfading to the
 *  icon as the step lands. */
export function RowIcon({ running, pulse = true, children }: { running: boolean; pulse?: boolean; children: React.ReactNode }): React.ReactElement {
  const glyph = useContext(RunningGlyphContext);
  if (!glyph) return pulse ? <RunningIcon running={running}>{children}</RunningIcon> : <>{children}</>;
  return (
    <SwapCell swapKey={running ? 'running' : 'settled'} className={GLYPH_CELL} layerClassName="inline-flex">
      {running
        // The line's text names the step; the glyph's own "Loading" status
        // would only repeat it on every frame.
        ? <span aria-hidden className="inline-flex"><Loader size={14} className="text-(--color-accent-primary)" /></span>
        : children}
    </SwapCell>
  );
}

const BADGE_IN = TWEEN.quick;

/** The ✕ on a failed call's icon. It scales in when a row on screen fails,
 *  never on a row that arrives failed. */
export function FailedIconBadge({ failed, label }: { failed: boolean; label: string }): React.ReactElement {
  return (
    <AnimatePresence initial={false}>
      {failed && (
        <motion.span
          key="failed"
          className="nrow-badge"
          aria-label={label}
          initial={{ scale: 0.6, opacity: 0 }}
          animate={{ scale: 1, opacity: 1 }}
          transition={BADGE_IN}
        >
          <XIcon className="h-3 w-3" />
        </motion.span>
      )}
    </AnimatePresence>
  );
}
