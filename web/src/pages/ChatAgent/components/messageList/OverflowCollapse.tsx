import React, { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { cssEase, DURATION, EASE_COLLAPSE, EASE_OUT } from '@/lib/motion';
import { isPageUnseen } from '@/lib/pageVisibility';
import { prefersReducedMotion } from '@/lib/reducedMotion';

/** Fades the content itself, so it works on whatever background the host has. */
const FADE = 'linear-gradient(to bottom, black 70%, transparent)';

interface OverflowCollapseProps {
  /** Render children unwrapped when false — no extra DOM, zero behavior change. */
  enabled: boolean;
  /** Collapsed bound in px; content measuring taller gets the toggle. */
  maxHeight: number;
  children: React.ReactNode;
}

/**
 * Bounds tall content behind a Show all / Show less toggle with a mask fade.
 * Measured on the inner, never-clipped element (cf. StructuredResultBlock) so
 * the reading stays valid once expanded — measuring the clipped wrapper would
 * report no overflow and drop the control that collapses it again.
 */
export function OverflowCollapse({ enabled, maxHeight, children }: OverflowCollapseProps): React.ReactElement {
  const { t } = useTranslation();
  const [expanded, setExpanded] = useState(false);
  const [overflows, setOverflows] = useState(false);
  const clipRef = useRef<HTMLDivElement>(null);
  const contentRef = useRef<HTMLDivElement>(null);

  // `children` is a fresh element each parent render, so this re-measures on
  // every content change — the same-value setState bails out cheaply. Before
  // paint, so tall content is bounded from its first frame rather than drawn
  // whole for one.
  useLayoutEffect(() => {
    if (!enabled) return;
    const el = contentRef.current;
    if (!el) return;
    setOverflows(el.scrollHeight > maxHeight);
  }, [enabled, maxHeight, children]);

  // The content also grows with no new children: an image loading, or the
  // column narrowing as a side panel opens and the text rewraps.
  useEffect(() => {
    if (!enabled || typeof ResizeObserver === 'undefined') return;
    const el = contentRef.current;
    if (!el) return;
    const observer = new ResizeObserver(() => setOverflows(el.scrollHeight > maxHeight));
    observer.observe(el);
    return () => observer.disconnect();
  }, [enabled, maxHeight]);

  // The toggle moves the bound between two measured heights rather than
  // snapping it, from wherever the box stands, so a second click mid-way turns
  // it around. Taken at the click, while a running tween still holds the box.
  const fromHeight = useRef<number | null>(null);
  const shownExpanded = useRef(expanded);
  useLayoutEffect(() => {
    if (shownExpanded.current === expanded) return;
    shownExpanded.current = expanded;
    const clip = clipRef.current;
    const content = contentRef.current;
    const from = fromHeight.current;
    fromHeight.current = null;
    if (!clip || !content || from === null || typeof clip.animate !== 'function') return;
    if (prefersReducedMotion() || isPageUnseen()) return;
    const to = expanded ? content.scrollHeight : maxHeight;
    // No fill: once it ends the bound is the style below again, or none.
    const tween = clip.animate(
      [{ maxHeight: `${from}px`, overflow: 'hidden' }, { maxHeight: `${to}px`, overflow: 'hidden' }],
      { duration: DURATION.fold * 1000, easing: cssEase(expanded ? EASE_OUT : EASE_COLLAPSE) },
    );
    return () => tween.cancel();
  }, [expanded, maxHeight]);

  if (!enabled) return <>{children}</>;

  const toggle = () => {
    fromHeight.current = clipRef.current?.getBoundingClientRect().height ?? null;
    setExpanded((open) => !open);
  };

  const bounded = overflows && !expanded;
  return (
    <div className="flex flex-col">
      <div ref={clipRef} style={bounded ? { maxHeight, overflow: 'hidden', maskImage: FADE, WebkitMaskImage: FADE } : undefined}>
        <div ref={contentRef}>{children}</div>
      </div>
      {overflows && (
        <button
          type="button"
          data-testid="overflow-collapse-toggle"
          onClick={toggle}
          className="press self-start mt-2 text-xs text-(--color-text-quaternary) hover:text-(--color-text-tertiary)"
          style={{ background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}
        >
          {expanded ? t('chat.collapseResult') : t('chat.expandResult')}
        </button>
      )}
    </div>
  );
}
