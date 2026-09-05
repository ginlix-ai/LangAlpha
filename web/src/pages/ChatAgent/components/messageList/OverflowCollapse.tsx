import React, { useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';

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
  const contentRef = useRef<HTMLDivElement>(null);

  // `children` is a fresh element each parent render, so this re-measures on
  // every content change — the same-value setState bails out cheaply.
  useEffect(() => {
    if (!enabled) return;
    const el = contentRef.current;
    if (!el) return;
    setOverflows(el.scrollHeight > maxHeight);
  }, [enabled, maxHeight, children]);

  if (!enabled) return <>{children}</>;

  const bounded = overflows && !expanded;
  return (
    <div className="flex flex-col">
      <div style={bounded ? { maxHeight, overflow: 'hidden', maskImage: FADE, WebkitMaskImage: FADE } : undefined}>
        <div ref={contentRef}>{children}</div>
      </div>
      {overflows && (
        <button
          type="button"
          data-testid="overflow-collapse-toggle"
          onClick={() => setExpanded((open) => !open)}
          className="self-start mt-2 text-xs transition-opacity hover:opacity-80"
          style={{ color: 'var(--color-text-quaternary)', background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}
        >
          {expanded ? t('chat.collapseResult') : t('chat.expandResult')}
        </button>
      )}
    </div>
  );
}
