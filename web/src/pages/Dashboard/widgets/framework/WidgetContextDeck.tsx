import React, { useEffect, useMemo, useRef, useState } from 'react';
import { BarChart3, Newspaper, LayoutGrid, ListOrdered, FileText } from 'lucide-react';
import { WidgetContextPreview, type WidgetContextPreviewShape } from './WidgetContextPreview';

/** Pick a thumb icon for a widget snapshot based on its type slug. Shared by
 *  the chat-input live deck and the chat-view inline deck so a given widget
 *  always gets the same glyph in both surfaces. */
export function pickWidgetIcon(widgetType: string): React.ComponentType<{ className?: string }> {
  if (widgetType.startsWith('markets.chart') || widgetType.startsWith('markets.miniChart')) return BarChart3;
  if (widgetType.startsWith('news.')) return Newspaper;
  if (widgetType.startsWith('tv.')) return LayoutGrid;
  if (widgetType.startsWith('watchlist.') || widgetType.startsWith('portfolio') || widgetType.startsWith('personal.')) return ListOrdered;
  if (widgetType.includes('list') || widgetType.includes('feed')) return ListOrdered;
  return FileText;
}

/** Card geometry — must stay in sync with the `.widget-deck-*` CSS so the
 *  computed inline transforms align with the card sizing rules. If these
 *  ever drift, the snapshot tests on the deck DOM will catch it. */
const CARD_HEIGHT = 60;
const CARD_GAP = 6;
const PEEK_STEP = 6;

export interface WidgetContextDeckProps {
  snapshots: WidgetContextPreviewShape[];
  fanned: boolean;
  onToggleFan: () => void;
  onCollapse: () => void;
  /** Optional eyebrow row above the stack (count + hint + clear button on
   *  the live deck; omitted on the read-only inline deck). */
  eyebrow?: React.ReactNode;
  /** Optional render hook for trailing card content (e.g. the remove `×`
   *  button on the live deck). When provided, called per card with the
   *  snapshot — return null to skip the slot for that card. */
  renderCardSlotEnd?: (snapshot: WidgetContextPreviewShape) => React.ReactNode;
  /** Outside-click boundary. Clicks inside this element keep the deck
   *  fanned (used by the live deck so typing in the textarea doesn't
   *  collapse the stack). Defaults to the deck rail itself. */
  boundaryRef?: React.RefObject<HTMLElement | null>;
  /** Extra class names on the rail wrapper. */
  className?: string;
  /** Inline style overrides for the rail wrapper. */
  style?: React.CSSProperties;
  /** When true, override card grid to drop the trailing slot column. The
   *  inline (read-only) variant uses this since it has no remove button. */
  compactCardGrid?: boolean;
  /** test id forwarded onto the rail. */
  testId?: string;
}

/**
 * Stacked widget-context card deck shared by the chat-input live deck and
 * the chat-view inline deck. Owns:
 *  - newest-first card ordering
 *  - peek transforms (translateY/scale/opacity per index)
 *  - fanned-state expansion height
 *  - per-card click → fan-then-preview semantics
 *  - outside-click collapse (with carve-outs for an open preview modal and
 *    Radix-portaled overlays)
 *  - the preview modal wiring
 *
 * Variant chrome (eyebrow row, remove buttons) is supplied via the
 * `eyebrow` and `renderCardSlotEnd` slots so the live and inline decks
 * share the same geometry math without sharing chrome.
 */
export function WidgetContextDeck({
  snapshots,
  fanned,
  onToggleFan,
  onCollapse,
  eyebrow,
  renderCardSlotEnd,
  boundaryRef,
  className = '',
  style,
  compactCardGrid = false,
  testId,
}: WidgetContextDeckProps) {
  const cards = useMemo(() => [...snapshots].reverse(), [snapshots]);
  const [preview, setPreview] = useState<WidgetContextPreviewShape | null>(null);
  const peekDepth = Math.min(cards.length - 1, 4) * PEEK_STEP;
  const stackHeight = fanned
    ? cards.length * (CARD_HEIGHT + CARD_GAP) - CARD_GAP
    : CARD_HEIGHT + peekDepth;
  const singleCard = cards.length === 1;

  // Outside-click collapse with the same race-protection both surfaces
  // need: pause while the preview modal is open, defer re-attachment one
  // animation frame after close so the click that dismissed the modal
  // can't leak through to a freshly registered listener.
  const rootRef = useRef<HTMLDivElement | null>(null);
  useEffect(() => {
    if (!fanned) return;
    if (preview !== null) return;
    const handler = (e: MouseEvent) => {
      const target = e.target as HTMLElement | null;
      if (!target) return;
      if (target.closest && target.closest('[role="dialog"]')) return;
      if (!document.body.contains(target)) return;
      const boundary = boundaryRef?.current ?? rootRef.current;
      if (boundary && boundary.contains(target)) return;
      onCollapse();
    };
    let attached = false;
    const rafId = requestAnimationFrame(() => {
      document.addEventListener('mousedown', handler);
      attached = true;
    });
    return () => {
      cancelAnimationFrame(rafId);
      if (attached) document.removeEventListener('mousedown', handler);
    };
  }, [fanned, preview, onCollapse, boundaryRef]);

  return (
    <div
      ref={rootRef}
      className={`widget-deck-rail ${fanned ? 'fanned' : ''} ${className}`.trim()}
      style={style}
      data-testid={testId}
      onClick={(e) => e.stopPropagation()}
    >
      {eyebrow}
      <div
        className={`widget-deck-stack ${fanned ? 'fanned' : ''}`}
        style={{ height: `${stackHeight}px` }}
        onClick={(e) => {
          // Stack-background click toggles fan only when the user actually
          // clicked on empty stack space — clicks on cards or remove
          // buttons handle themselves.
          const target = e.target as HTMLElement;
          if (target.closest('.widget-deck-card-remove')) return;
          if (target.closest('.widget-deck-card')) return;
          onToggleFan();
        }}
      >
        {cards.map((s, i) => {
          const Icon = pickWidgetIcon(s.widget_type);
          const hasImage = !!s.image_jpeg_data_url;
          const top = fanned ? i * (CARD_HEIGHT + CARD_GAP) : 0;
          const peekY = fanned ? 0 : i * PEEK_STEP;
          const peekScale = fanned ? 1 : Math.max(1 - i * 0.03, 0.85);
          const peekOpacity = fanned
            ? 1
            : i === 0
              ? 1
              : Math.max(0.85 - (i - 1) * 0.2, 0.25);
          const handleActivate = () => {
            // Multi-card peeked deck: first click fans (so older cards
            // become reachable). Already fanned, or single-card deck:
            // open the preview modal.
            if (!singleCard && !fanned) onToggleFan();
            else setPreview(s);
          };
          return (
            <div
              key={s.widget_id}
              data-i={i}
              data-kind={s.widget_type}
              className="widget-deck-card"
              style={{
                top: `${top}px`,
                transform: `translateY(${peekY}px) scale(${peekScale})`,
                opacity: peekOpacity,
                pointerEvents: fanned || i === 0 ? 'auto' : 'none',
                zIndex: cards.length - i,
                cursor: 'pointer',
                ...(compactCardGrid ? { gridTemplateColumns: '32px 1fr' } : null),
              }}
              title={s.label}
              role="button"
              tabIndex={0}
              onClick={(e) => {
                if ((e.target as HTMLElement).closest('.widget-deck-card-remove')) return;
                handleActivate();
              }}
              onKeyDown={(e) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  handleActivate();
                }
              }}
            >
              <div className={`widget-deck-card-thumb ${hasImage ? 'has-image' : ''}`}>
                {hasImage ? (
                  <img src={s.image_jpeg_data_url} alt="" className="widget-deck-card-thumb-img" />
                ) : (
                  <Icon className="h-3.5 w-3.5" />
                )}
              </div>
              <div className="widget-deck-card-text">
                <div className="widget-deck-card-title">{s.label}</div>
                {s.description && (
                  <div className="widget-deck-card-snippet">{s.description}</div>
                )}
              </div>
              {renderCardSlotEnd ? renderCardSlotEnd(s) : null}
            </div>
          );
        })}
      </div>
      <WidgetContextPreview snapshot={preview} onClose={() => setPreview(null)} />
    </div>
  );
}

export default WidgetContextDeck;
