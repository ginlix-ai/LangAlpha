import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronDown } from 'lucide-react';
import { motion, type Variants } from '@/lib/framer';
import { DURATION, EASE_COLLAPSE, TWEEN } from '@/lib/motion';
import { DockPill, PILL_HEIGHT } from './dockPill';
import type { ComposerDock } from './useComposerDock';
import type { DockFloatState, PillOffset } from './useDockFloat';
import './dockFloat.css';

/** How small the composer gets folding into the pill, and growing back out. */
const FOLDED_SCALE = 0.6;

/** The pill's centre on the composer's box, whose bottom edge the pill shares
 *  at home: the composer shrinks into the pill and grows out of it, wherever
 *  it was dragged. */
const towardPill = ({ x, y }: PillOffset) =>
  `calc(50% + ${x}px) calc(100% - ${PILL_HEIGHT / 2}px + ${y}px)`;

/**
 * Every move the composer makes here, from the phase it is in: it rises in
 * as it arrives from the chat, shrinks into the pill, and grows back out of
 * it. Each comes from where the last one left it.
 */
type Phase = 'away' | 'open' | 'folded';
const PHASES: Variants = {
  away: { opacity: 0, y: 4 },
  open: { opacity: 1, y: 0, scale: 1, transition: TWEEN.enter },
  folded: { opacity: 0, scale: FOLDED_SCALE, transition: { duration: DURATION.fold, ease: EASE_COLLAPSE } },
};

/**
 * The composer floating over a file on a wide screen, as it floats over the
 * transcript: a layer over the whole file that lets the pointer through, with
 * the composer at its foot. It folds into a pill, which can be dragged
 * anywhere over the file, to uncover what is under it.
 *
 * Folded, the composer is unseen, lets the pointer through and is hidden from
 * a screen reader, but it can still take the focus, and the focus arriving
 * opens it. Whatever sends the
 * person to the composer focuses its text box (a quote asked about in the
 * chat, a draft handed in or given back, the pill's own click), so each of
 * them opens it without knowing it was folded.
 */
export function DockFloat({ dock, float }: { dock: ComposerDock; float: DockFloatState }): React.ReactElement {
  const { t } = useTranslation();
  // Held as state, not a ref: a pill mounting with the layer (folded on a
  // tab switch) sets up its observer before the layer's ref would attach.
  const [layer, setLayer] = useState<HTMLDivElement | null>(null);
  const boxRef = useRef<HTMLDivElement>(null);
  const { taken, holdAt } = dock;
  const { setFolded } = float;
  const phase: Phase = !taken ? 'away' : float.folded ? 'folded' : 'open';
  // Folded from here, the pill takes the focus the fold button had, so a
  // keyboard is not dropped to the page; folded on arrival, it takes none.
  const [pillTakesFocus, setPillTakesFocus] = useState(false);
  const fold = () => {
    setPillTakesFocus(true);
    setFolded(true);
  };
  // Opened outright, not through the focus: a composer disabled while the
  // agent waits on an answer cannot take it.
  const open = () => {
    setFolded(false);
    boxRef.current?.querySelector<HTMLElement>('[data-handoff="composer"] textarea')?.focus({ preventScroll: true });
  };

  // Listened for on the element: the composer is a portal from the chat, so
  // React's own focus events bubble up through the chat, never through here.
  useEffect(() => {
    const box = boxRef.current;
    if (!box) return;
    const openOnFocus = () => setFolded(false);
    box.addEventListener('focusin', openOnFocus);
    return () => box.removeEventListener('focusin', openOnFocus);
  }, [setFolded]);

  // The holder is a plain element, so the composer's move out of it runs
  // through React's ref cleanup, which a motion element does not pass on.
  const hold = useCallback((el: HTMLDivElement | null) => {
    const release = el && taken ? holdAt(el) : undefined;
    return () => {
      if (typeof release === 'function') release();
    };
  }, [taken, holdAt]);

  return (
    <div ref={setLayer} className="composer-float">
      <div className="composer-float-frame">
        {phase === 'open' && (
          <button
            type="button"
            className="composer-float-fold tap-target"
            onClick={fold}
            title={t('chat.dockMinimize')}
            aria-label={t('chat.dockMinimize')}
          >
            <ChevronDown className="h-3.5 w-3.5" />
          </button>
        )}
        <motion.div
          ref={boxRef}
          className="composer-float-box"
          variants={PHASES}
          initial="away"
          animate={phase}
          style={{ transformOrigin: towardPill(float.pillOffset) }}
          data-folded={float.folded || undefined}
          aria-hidden={float.folded || undefined}
        >
          <div ref={hold} className="contents" />
        </motion.div>
        {float.folded && (
          <DockPill
            area={layer}
            offset={float.pillOffset}
            takeFocus={pillTakesFocus}
            onMove={float.setPillOffset}
            onOpen={open}
            status={float.status}
            queued={float.queued}
          />
        )}
      </div>
    </div>
  );
}
