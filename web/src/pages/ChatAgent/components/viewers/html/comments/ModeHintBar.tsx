import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion } from '@/lib/framer';
import { TWEEN } from '@/lib/motion';
import { ADD_KEY_LABEL } from '../../comments/partModifier';

const HINT_HIDDEN = { opacity: 0, y: 4 };
const HINT_SHOWN = { opacity: 1, y: 0, transition: TWEEN.quick };
const HINT_GONE = { opacity: 0, y: 4, transition: TWEEN.exit };

/** The box over the page, which the hint gives way to: its element, and where it opened, which says when it moves. */
export interface CoveringBox {
  ref: React.RefObject<HTMLElement | null>;
  /** Changes whenever the box opens or moves; null with none open. */
  at: string | null;
}

/**
 * Whether `box` covers `hint`, measured once the box has been placed (its
 * popper places it a frame after it mounts), again as the pane resizes, and
 * as either changes size (a draft growing with its words), as a scroll bar
 * coming or going moves the hint (`insets`), and each time the hint is shown,
 * since it is a new element then. Kept for where the box was, so it holds
 * until the box moves.
 */
function useGivesWayTo(
  hint: React.RefObject<HTMLElement | null>,
  shown: boolean,
  box: CoveringBox,
  size: { width: number; height: number },
  insets: { bottom: number; right: number } | null | undefined,
): boolean {
  const [covering, setCovering] = useState<string | null>(null);
  const { ref, at } = box;
  useEffect(() => {
    if (!at || !shown) return undefined;
    let frame = 0;
    const measure = () => {
      cancelAnimationFrame(frame);
      frame = requestAnimationFrame(() => {
        const a = hint.current?.getBoundingClientRect();
        const b = ref.current?.getBoundingClientRect();
        const over = !!a && !!b && a.left < b.right && a.right > b.left && a.top < b.bottom && a.bottom > b.top;
        setCovering(over ? at : null);
      });
    };
    measure();
    const sized = new ResizeObserver(measure);
    for (const el of [hint.current, ref.current]) if (el) sized.observe(el);
    return () => {
      cancelAnimationFrame(frame);
      sized.disconnect();
    };
  }, [at, shown, hint, ref, size.width, size.height, insets?.bottom, insets?.right]);
  return at !== null && covering === at;
}

/**
 * Comment mode's hint at the foot of the pane: what a click picks, and the
 * way out. It keeps clear of the viewer's scroll bars (`insets`), wraps on a
 * narrow pane, and steps out while a box is open where it sits.
 */
export function ModeHintBar({ shown, takesParts, adding, hint, touchHint, insets, box, size, onExit }: {
  shown: boolean;
  /** The draft takes more parts, by a modifier click or the box's "Add a part". */
  takesParts: boolean;
  /** The next tap adds a part. */
  adding: boolean;
  hint?: string;
  touchHint?: string;
  insets?: { bottom: number; right: number } | null;
  box: CoveringBox;
  size: { width: number; height: number };
  onExit: () => void;
}): React.ReactElement {
  const { t } = useTranslation();
  const hintRef = useRef<HTMLDivElement>(null);
  const covered = useGivesWayTo(hintRef, shown, box, size, insets);
  const style = insets
    ? ({ '--comments-inset-bottom': `${insets.bottom}px`, '--comments-inset-right': `${insets.right}px` } as React.CSSProperties)
    : undefined;
  return (
    <AnimatePresence>
      {shown && (
        <motion.div key="hint" className="page-comments-hint-row" style={style} initial={HINT_HIDDEN} animate={HINT_SHOWN} exit={HINT_GONE}>
          {/* Inert while covered: unseen, its way out must not take a Tab either. */}
          <div ref={hintRef} className="page-comments-hint" data-covered={covered || undefined} inert={covered}>
            <span className="page-comments-hint-text" role="status">
              <ModeHint takesParts={takesParts} adding={adding} hint={hint} touchHint={touchHint} />
            </span>
            <button
              type="button"
              className="page-comments-hint-exit tap-target"
              onClick={onExit}
              aria-label={t('pageViewer:comments.exitModeLabel')}
              aria-keyshortcuts="Escape"
            >
              {t('pageViewer:comments.exitMode')}
              {/* A key only where there is one to press: a touch screen's button is the way out. */}
              <kbd aria-hidden className="pointer-coarse:hidden">Esc</kbd>
            </button>
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}

/**
 * What a click in comment mode does. While a draft takes parts a modifier
 * click adds one; a touch screen has no modifier, so its box offers "Add a
 * part" instead, and says so once pressed.
 */
function ModeHint({ takesParts, adding, hint, touchHint }: {
  takesParts: boolean;
  adding: boolean;
  hint?: string;
  touchHint?: string;
}): React.ReactElement {
  const { t } = useTranslation();
  const plain = hint ?? t('pageViewer:comments.modeHint');
  // A touch screen is told only what a tap does: a drag there scrolls, and
  // has no modifier to hold.
  const touch = touchHint ?? t('pageViewer:comments.modeHintTouch');
  if (adding) return <>{t('pageViewer:comments.modeHintAdding')}</>;
  return (
    <>
      <span className="pointer-coarse:hidden">
        {takesParts ? t('pageViewer:comments.modeHintParts', { key: ADD_KEY_LABEL }) : plain}
      </span>
      <span className="hidden pointer-coarse:inline">{touch}</span>
    </>
  );
}
