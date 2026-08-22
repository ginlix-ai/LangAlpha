import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
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
 * peeking below to imply the rest. Groups under STACK_THRESHOLD rows render
 * as a plain header — a two-row stack hides more than it tidies. Expansion
 * persists per group id.
 */

const STORE_KEY = 'plugins.deckExpanded';

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
  const [stored, setStored] = useState<boolean | null>(() => readStore()[id] ?? null);

  if (count === 0) return null;

  const collapsible = count >= STACK_THRESHOLD;
  const expanded = forceExpanded || !collapsible || (stored ?? defaultExpanded);
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
      {expanded ? (
        children
      ) : (
        // The collapsed body: the group's real first row as the stack's
        // cover, with sliver layers peeking below to imply the rest. The
        // rows stay mounted — CSS hides all but the first, and `inert`
        // makes the cover decorative (no focus, no clicks) so the whole
        // body is one expand target. Slivers keep their border at full
        // strength: the card fill alone is invisible on the light page.
        <div
          aria-hidden
          data-testid={`deck-cover-${id}`}
          className="cursor-pointer"
          onClick={() => setExpanded(true)}
        >
          <div inert className="pointer-events-none [&>*:not(:first-child)]:hidden">
            {children}
          </div>
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
        </div>
      )}
    </div>
  );
}
