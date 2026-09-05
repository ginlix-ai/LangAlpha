import React, { useEffect, useLayoutEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { motion, useReducedMotion } from 'framer-motion';
import { Check, ChevronDown, ChevronUp, Minus } from 'lucide-react';
import { STACK_THRESHOLD } from '../utils/groupOrigins';
import type { BulkSelection } from './useBulkSelection';

/**
 * One origin group of rows, stackable like the sources panel's card decks.
 * The uppercase header line (title + count + on/off tally) anchors the group
 * in BOTH states — collapsing folds the rows into a card stack, never the
 * header into a card. A collapsed group that rendered as a card read as a
 * row of the previous group, since cards are this page's row idiom. The
 * stack's cover is the group's real first row (inert), with sliver layers
 * peeking below to imply the rest. Every row stays mounted in both states —
 * toggling animates a clipped region's height (the sources deck's motion),
 * never remounts subtrees. Groups under STACK_THRESHOLD rows render as a
 * plain header — a two-row stack hides more than it tidies. Expansion
 * persists per group id.
 */

// House curve, matching the sources deck's 260ms fold (SourcesPanel.css).
const EASE_OUT = [0.16, 1, 0.3, 1] as const;

// Colon, not a dot: `plugins.` is a locale namespace, and the i18n parity
// sweep reads every bare `plugins.<x>` literal in the tree as a key that must
// resolve in both catalogs. Matches `page:plugins` next door either way.
export const STORE_KEY = 'plugins:deckExpanded';

function readStore(): Record<string, boolean> {
  try {
    return JSON.parse(localStorage.getItem(STORE_KEY) ?? '{}') as Record<string, boolean>;
  } catch {
    return {};
  }
}

function writeStore(id: string, expanded: boolean) {
  try {
    localStorage.setItem(STORE_KEY, JSON.stringify({ ...readStore(), [id]: expanded }));
  } catch {
    // Storage full or unavailable: the deck still works, it just forgets.
  }
}

/** Tri-state select-all box for a group header. */
function SelectAllBox({
  state,
  label,
  onToggle,
}: {
  state: 'none' | 'some' | 'all';
  label: string;
  onToggle: () => void;
}) {
  const filled = state !== 'none';
  return (
    <button
      type="button"
      role="checkbox"
      aria-checked={state === 'some' ? 'mixed' : state === 'all'}
      aria-label={label}
      onClick={onToggle}
      className="flex-shrink-0 inline-flex h-4 w-4 items-center justify-center rounded"
      style={{
        border: filled ? 'none' : '1px solid var(--color-border-muted)',
        backgroundColor: filled ? 'var(--color-accent-primary)' : 'transparent',
      }}
    >
      {state === 'all' && (
        <Check className="h-3 w-3" style={{ color: 'var(--color-btn-primary-text)' }} />
      )}
      {state === 'some' && (
        <Minus className="h-3 w-3" style={{ color: 'var(--color-btn-primary-text)' }} />
      )}
    </button>
  );
}

export function GroupDeck({
  id,
  title,
  icon: Icon,
  count,
  enabledCount,
  badge,
  action,
  defaultExpanded = false,
  forceExpanded = false,
  selection,
  selectionKeys,
  children,
}: {
  id: string;
  title: string;
  icon: React.ComponentType<{ className?: string; style?: React.CSSProperties }>;
  count: number;
  /** Present = the collapsed card and the header show an on/off tally. */
  enabledCount?: number;
  /** Extra identity chip next to the title (e.g. a suppressed-plugin note). */
  badge?: React.ReactNode;
  /** Trailing header affordance (e.g. jump to the owning plugin's card). */
  action?: React.ReactNode;
  defaultExpanded?: boolean;
  /** Filter and select mode both need every row visible. */
  forceExpanded?: boolean;
  selection?: BulkSelection;
  selectionKeys?: string[];
  children: React.ReactNode;
}) {
  const { t } = useTranslation();
  const reducedMotion = useReducedMotion();
  const [stored, setStored] = useState<boolean | null>(() => readStore()[id] ?? null);

  const collapsible = count >= STACK_THRESHOLD;
  const expanded = forceExpanded || !collapsible || (stored ?? defaultExpanded);

  // Collapsed height = the first row's measured height, so the clip cuts
  // exactly at the cover row's bottom border. Measured (not styled): rows
  // wrap freely, and the observer tracks resizes and row churn.
  const rowsRef = useRef<HTMLDivElement>(null);
  const [coverH, setCoverH] = useState<number | null>(null);
  useLayoutEffect(() => {
    const rows = rowsRef.current;
    if (!rows) return;
    const measure = () => {
      const first = rows.firstElementChild;
      if (first instanceof HTMLElement) setCoverH(first.offsetHeight);
    };
    measure();
    const observer = new ResizeObserver(measure);
    observer.observe(rows);
    return () => observer.disconnect();
  }, [count]);

  // Animate only the user's own expand/collapse gesture. A forceExpanded
  // flip (filter, select mode) snaps: the filter's motion is the row set
  // changing, and a fold racing exiting rows animates toward a stale height
  // and pops at the end. Re-measures (window resize, row churn while
  // collapsed) snap for the same reason.
  const prevExpandedRef = useRef(expanded);
  const prevForceRef = useRef(forceExpanded);
  const toggled = prevExpandedRef.current !== expanded;
  const forceFlip = prevForceRef.current !== forceExpanded;
  useEffect(() => {
    prevExpandedRef.current = expanded;
    prevForceRef.current = forceExpanded;
  });
  const bodyTransition =
    toggled && !forceFlip && !reducedMotion
      ? { duration: 0.26, ease: EASE_OUT }
      : { duration: 0 };

  if (count === 0) return null;

  const setExpanded = (next: boolean) => {
    setStored(next);
    writeStore(id, next);
  };

  const tally =
    enabledCount !== undefined
      ? t('plugins.groups.tally', { enabled: enabledCount, disabled: count - enabledCount })
      : null;

  const selecting = !!selection?.selecting && !!selectionKeys?.length;
  const selectedInGroup = selecting
    ? selectionKeys!.filter((k) => selection!.selected.has(k)).length
    : 0;
  const selectAllState: 'none' | 'some' | 'all' =
    selectedInGroup === 0 ? 'none' : selectedInGroup === selectionKeys!.length ? 'all' : 'some';

  const countPill = (
    <span
      className="text-[0.625rem] px-1.5 py-0.5 rounded"
      style={{ color: 'var(--color-text-tertiary)', backgroundColor: 'var(--color-bg-tag)' }}
      data-testid={`deck-count-${id}`}
    >
      {count}
    </span>
  );

  const heading = (
    <>
      <Icon
        className="h-3.5 w-3.5 flex-shrink-0"
        style={{ color: 'var(--color-text-tertiary)' }}
      />
      <h3
        className="text-[0.6875rem] font-medium uppercase tracking-wide truncate"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {title}
      </h3>
      {countPill}
    </>
  );
  const Chevron = expanded ? ChevronUp : ChevronDown;

  return (
    <div className="flex flex-col gap-1.5" data-testid={`deck-${id}`}>
      <div className="flex items-center justify-between gap-2">
        <div className="flex items-center gap-2 min-w-0">
          {selecting && (
            <SelectAllBox
              state={selectAllState}
              label={t('plugins.groups.selectAllAria', { title })}
              onToggle={() =>
                selection!.setMany(selectionKeys!, selectAllState !== 'all')
              }
            />
          )}
          {collapsible ? (
            <button
              type="button"
              aria-expanded={expanded}
              aria-label={t(
                expanded ? 'plugins.groups.collapseAria' : 'plugins.groups.expandAria',
                { title },
              )}
              onClick={() => setExpanded(!expanded)}
              disabled={forceExpanded}
              className="flex items-center gap-2 min-w-0 disabled:cursor-default"
            >
              {heading}
              {!forceExpanded && (
                <Chevron
                  className="h-3 w-3 flex-shrink-0"
                  style={{ color: 'var(--color-text-tertiary)' }}
                />
              )}
            </button>
          ) : (
            <div className="flex items-center gap-2 min-w-0">{heading}</div>
          )}
          {badge}
          {tally && (
            <span
              className="text-[0.6875rem] flex-shrink-0"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              {tally}
            </span>
          )}
        </div>
        {action && <div className="flex items-center flex-shrink-0">{action}</div>}
      </div>
      {/* The body: every row stays mounted in one list (so a caller's
          AnimatePresence keeps working) inside a clip whose height animates
          between the first row's measured height and auto — the toggle is
          one continuous unfold, never a remount, with the sliver layers
          folding away in the same motion. Collapsed, `inert` makes the rows
          decorative (no focus, no clicks) and the whole body is one expand
          target. Slivers keep their border at full strength: the card fill
          alone is invisible on the light page. The clip is safe to keep
          permanently — row selection rings are inset shadows. */}
      <div
        aria-hidden={expanded ? undefined : true}
        data-testid={expanded ? undefined : `deck-cover-${id}`}
        className={expanded ? undefined : 'cursor-pointer'}
        onClick={expanded ? undefined : () => setExpanded(true)}
      >
        <motion.div
          className="overflow-hidden"
          initial={false}
          animate={{ height: expanded ? 'auto' : (coverH ?? 0) }}
          transition={bodyTransition}
        >
          <div
            ref={rowsRef}
            inert={!expanded}
            // Margins, not gap: each row owns the space above it, so a
            // presence exit collapses row + spacing together. A container
            // gap survives until unmount and snaps away in one frame.
            className={`flex flex-col [&>*+*]:mt-1.5${expanded ? '' : ' pointer-events-none'}`}
          >
            {children}
          </div>
        </motion.div>
        {collapsible && (
          <motion.div
            aria-hidden
            className="overflow-hidden"
            initial={false}
            animate={expanded ? { height: 0, opacity: 0 } : { height: 'auto', opacity: 1 }}
            transition={bodyTransition}
          >
            {[8, 16].map((inset) => (
              <div
                key={inset}
                className="rounded-b-lg"
                style={{
                  height: 6,
                  marginLeft: inset,
                  marginRight: inset,
                  backgroundColor: 'var(--color-bg-card)',
                  border: '1px solid var(--color-border-muted)',
                  borderTop: 'none',
                }}
              />
            ))}
          </motion.div>
        )}
      </div>
    </div>
  );
}
