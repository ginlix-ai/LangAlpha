import React, { useEffect, useLayoutEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { animate, motion, useMotionValue, useReducedMotion } from '@/lib/framer';
import { X } from 'lucide-react';
import { useBackdropDismiss, useDialogA11y } from '@/hooks/useDialogA11y';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useSwipeToDismiss } from '@/hooks/useSwipeToDismiss';
import { DURATION, TWEEN } from '@/lib/motion';
import { cn } from '@/lib/utils';

/**
 * The one shell for a form dialog: a centred panel that enters and leaves
 * with motion, whose height follows its content smoothly rather than jumping
 * when a section folds open or a result lands. Header is pinned, the body
 * scrolls, the footer is pinned. On a phone it is a bottom sheet that can be
 * swiped away. Exit animations need an `AnimatePresence` around the call
 * site's conditional render.
 *
 * Height is animated as a real height, not a transform, so text never
 * squashes mid-motion. The panel measures its content and follows it; until
 * the first measurement it is `auto`, which is also what a test renderer
 * with no layout sees.
 *
 * A shell opened over another shows one panel at a time: the one underneath
 * is `covered` and steps out of view, the one on top is `stacked` and leaves
 * the dimming to it, so the page behind is dimmed once rather than twice.
 */

const WIDTH = {
  narrow: 'max-w-md',
  standard: 'max-w-lg',
  wide: 'max-w-2xl',
} as const;

/**
 * Padding is the shell's decision, not the caller's. A caller handing in its
 * own classes could silently delete the footer's safe-area inset, because
 * tailwind-merge resolves a later `py-*` over the `pb-[max(...)]` that keeps a
 * bottom sheet clear of the home indicator, and every detail overlay lost it
 * that way. Header, body and footer all read their horizontal padding here, so
 * the three edges cannot drift apart either.
 */
const DENSITY = {
  /** Forms and wizards: compact rows, buttons close to the fields. */
  form: {
    x: 'px-5',
    body: 'pb-5 gap-4',
    footer: 'pt-3 pb-[max(0.75rem,env(safe-area-inset-bottom))]',
  },
  /** Detail overlays: longer sections, read at a roomier rhythm. */
  reading: {
    x: 'px-6',
    body: 'pt-5 pb-5 gap-5',
    footer: 'pt-4 pb-[max(1rem,env(safe-area-inset-bottom))]',
  },
} as const;

const NOOP = () => {};

/**
 * Page scroll is held while any shell is mounted, and a nested pair releases
 * once. The capture is module-level and happens on the outermost lock only: a
 * nested shell reading the body for itself would read 'hidden', and restore
 * that if the outer shell unmounted first.
 */
let lockCount = 0;
let lockedOverflow = '';
function useScrollLock() {
  useEffect(() => {
    if (typeof document === 'undefined') return;
    if (lockCount === 0) lockedOverflow = document.body.style.overflow;
    lockCount += 1;
    document.body.style.overflow = 'hidden';
    return () => {
      lockCount -= 1;
      if (lockCount === 0) document.body.style.overflow = lockedOverflow;
    };
  }, []);
}

export function ModalShell({
  labelId,
  title,
  subtitle,
  header,
  onClose,
  dismissable = true,
  closeDisabled = false,
  width = 'standard',
  zIndex,
  density = 'form',
  covered = false,
  stacked = false,
  fixedHeight,
  footer,
  children,
}: {
  labelId: string;
  title?: React.ReactNode;
  subtitle?: React.ReactNode;
  /**
   * A whole header of the caller's own (an identity tile with its controls)
   * in place of the title block. It must render the element `labelId` names.
   */
  header?: React.ReactNode;
  onClose: () => void;
  /** False while the dialog owns work the user cannot get back by reopening. */
  dismissable?: boolean;
  /** The close control is shown but inert, for a save in flight. */
  closeDisabled?: boolean;
  width?: keyof typeof WIDTH;
  /** Defaults to the shell layer, one step higher when `stacked`, so a shell
   *  opened over another sits on top of it without saying so. */
  zIndex?: number;
  /** How roomy the panel sits: a form, or a detail overlay's reading rhythm. */
  density?: keyof typeof DENSITY;
  /**
   * Another shell is open over this one. The panel fades out and goes inert
   * but stays mounted, so its state and scroll position are there when it
   * comes back; its backdrop stays, as the only dimming on the page.
   */
  covered?: boolean;
  /**
   * Opened over a shell that is `covered`. The backdrop still takes the click
   * outside, but paints nothing, since the shell underneath already dims.
   */
  stacked?: boolean;
  /**
   * Hold the panel at this many pixels, the viewport permitting, whatever the
   * body holds. For a form whose body changes shape at the user's pick: the
   * panel and the controls above the change stay put, and a longer body
   * scrolls rather than growing the panel. A prop rather than the caller
   * sizing its own children, since the body's padding and scroll are the
   * shell's: a caller's fixed box would scroll inside that padding, a second
   * scroller in the first.
   */
  fixedHeight?: number;
  /** Pinned under the scroll body; lay the buttons out yourself. */
  footer?: React.ReactNode;
  children: React.ReactNode;
}) {
  const { t } = useTranslation();
  const close = dismissable && !closeDisabled ? onClose : NOOP;
  const dialogRef = useDialogA11y<HTMLDivElement>(close);
  const backdrop = useBackdropDismiss<HTMLDivElement>(close);
  const reducedMotion = useReducedMotion();
  const isMobile = useIsMobile();
  const space = DENSITY[density];
  const layer = zIndex ?? (stacked ? 1020 : 1010);
  useScrollLock();

  // The panel follows the measured height of its content.
  const height = useMotionValue<number | 'auto'>('auto');
  const measuredRef = useRef<HTMLDivElement>(null);
  const settledRef = useRef(false);
  useLayoutEffect(() => {
    const el = measuredRef.current;
    if (!el || typeof ResizeObserver === 'undefined') return;
    const ro = new ResizeObserver(() => {
      const next = el.offsetHeight;
      if (!next) return;
      if (!settledRef.current) {
        settledRef.current = true;
        height.set(next);
        return;
      }
      if (height.get() === next) return;
      animate(height, next, reducedMotion ? { duration: 0 } : TWEEN.fold);
    });
    ro.observe(el);
    return () => ro.disconnect();
  }, [height, reducedMotion]);

  const { contentRef, handleRef, dragY } = useSwipeToDismiss({
    onDismiss: close,
    enabled: isMobile && dismissable && !closeDisabled,
  });
  const panelRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!isMobile) return;
    return dragY.on('change', (v) => {
      if (panelRef.current) panelRef.current.style.translate = `0 ${v}px`;
    });
  }, [isMobile, dragY]);

  const setPanelRef = (node: HTMLDivElement | null) => {
    panelRef.current = node;
    (dialogRef as React.MutableRefObject<HTMLDivElement | null>).current = node;
  };

  const panelMotion = reducedMotion
    ? { initial: { opacity: 0 }, animate: { opacity: 1 }, exit: { opacity: 0 } }
    : isMobile
      ? { initial: { opacity: 0, y: 32 }, animate: { opacity: 1, y: 0 }, exit: { opacity: 0, y: 24 } }
      : { initial: { opacity: 0, y: 12, scale: 0.98 }, animate: { opacity: 1, y: 0, scale: 1 }, exit: { opacity: 0, y: 6, scale: 0.98 } };
  // Stepping back, not leaving: no travel, and as quick as an exit so the
  // panel coming in over it is the one the eye follows.
  const coveredMotion = {
    ...(reducedMotion || isMobile ? { opacity: 0 } : { opacity: 0, scale: 0.98 }),
    transition: { duration: DURATION.exit },
  };

  return (
    <motion.div
      className={cn('fixed inset-0 flex justify-center', isMobile ? 'items-end' : 'items-center p-4')}
      style={{ backgroundColor: stacked ? 'transparent' : 'var(--color-bg-overlay-strong)', zIndex: layer }}
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0, transition: { duration: DURATION.exit } }}
      transition={{ duration: DURATION.quick }}
      {...backdrop}
    >
      <motion.div
        ref={setPanelRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={labelId}
        tabIndex={-1}
        inert={covered}
        // Clipped, not hidden: an overflow-hidden box can still be scrolled
        // by focus, which would slide the header out of a pinned panel.
        className={cn(
          'relative w-full flex flex-col overflow-clip',
          WIDTH[width],
          isMobile ? 'rounded-t-3xl' : 'rounded-lg',
        )}
        style={{
          backgroundColor: 'var(--color-bg-elevated)',
          border: '1px solid var(--color-border-muted)',
          boxShadow: 'var(--shadow-card)',
          height,
        }}
        {...panelMotion}
        animate={covered ? coveredMotion : panelMotion.animate}
        exit={{ ...panelMotion.exit, transition: { duration: DURATION.exit } }}
        transition={TWEEN.enter}
      >
        <div
          ref={measuredRef}
          className="flex flex-col"
          style={{
            maxHeight: isMobile ? '90dvh' : '85vh',
            // Not shrunk to the panel, whose height is read off this box:
            // as a flex item it would settle wherever the panel was.
            ...(fixedHeight ? { height: fixedHeight, flexShrink: 0 } : {}),
          }}
        >
          {isMobile && (
            <div
              ref={handleRef}
              className="flex justify-center pt-2.5 pb-0.5 shrink-0 cursor-grab active:cursor-grabbing"
              style={{ touchAction: 'none' }}
            >
              <div className="w-10 h-1 rounded-full" style={{ backgroundColor: 'var(--color-border-default)' }} />
            </div>
          )}
          {dismissable && (
            <button
              type="button"
              onClick={onClose}
              disabled={closeDisabled}
              className="absolute top-3 right-3 p-1.5 rounded transition-colors hover:bg-foreground/10 disabled:opacity-40 disabled:pointer-events-none"
              style={{ color: 'var(--color-text-tertiary)' }}
              aria-label={t('common.close')}
            >
              <X className="h-4 w-4" />
            </button>
          )}
          {header ? (
            <div className={cn('shrink-0 pt-5 pb-4', space.x)} style={{ borderBottom: '1px solid var(--color-border-muted)' }}>
              {header}
            </div>
          ) : (
            <div className={cn('shrink-0 pt-5 pb-3 pr-12', space.x)}>
              <h3 id={labelId} className="text-lg font-semibold" style={{ color: 'var(--color-text-primary)' }}>
                {title}
              </h3>
              {subtitle && (
                <p className="text-xs mt-1" style={{ color: 'var(--color-text-tertiary)' }}>
                  {subtitle}
                </p>
              )}
            </div>
          )}
          <div
            ref={contentRef}
            className={cn('min-h-0 flex-1 overflow-y-auto flex flex-col', space.x, space.body)}
            style={{ overscrollBehaviorY: 'contain' }}
          >
            {children}
          </div>
          {footer && (
            <div
              className={cn('shrink-0', space.x, space.footer)}
              style={{ borderTop: '1px solid var(--color-border-muted)' }}
            >
              {footer}
            </div>
          )}
        </div>
      </motion.div>
    </motion.div>
  );
}
