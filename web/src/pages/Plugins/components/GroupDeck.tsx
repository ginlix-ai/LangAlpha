import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Check, ChevronDown, ChevronUp, Minus } from 'lucide-react';
import { STACK_THRESHOLD } from '../utils/groupOrigins';
import type { BulkSelection } from './useBulkSelection';

/**
 * One origin group of rows, stackable like the sources panel's card decks.
 * Collapsed = a summary card (origin identity + count + on/off tally) over
 * two decorative peek layers; expanded = a header line + the rows in normal
 * flow. Rows here are variable-height, so the stack is a summary-card
 * illusion rather than the fixed-height card fan the sources panel animates.
 * Groups under STACK_THRESHOLD rows render as a plain header — a two-row
 * stack hides more than it tidies. Expansion persists per group id.
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

  if (!expanded) {
    return (
      <div className="flex flex-col" data-testid={`deck-${id}`}>
        <button
          type="button"
          aria-expanded={false}
          aria-label={t('plugins.groups.expandAria', { title })}
          onClick={() => setExpanded(true)}
          className="flex items-start justify-between gap-3 py-2.5 px-3 rounded-lg text-left w-full transition-colors hover:bg-foreground/5"
          style={{ backgroundColor: 'var(--color-bg-card)' }}
        >
          <div className="min-w-0 flex flex-col gap-1">
            <div className="flex items-center gap-2 flex-wrap">
              <Icon
                className="h-4 w-4 flex-shrink-0"
                style={{ color: 'var(--color-accent-primary)' }}
              />
              <span
                className="text-sm font-medium truncate"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {title}
              </span>
              {countPill}
              {badge}
            </div>
            {tally && (
              <span className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                {tally}
              </span>
            )}
          </div>
          <ChevronDown
            className="h-4 w-4 flex-shrink-0 mt-0.5"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
        </button>
        {/* Decorative peek layers: the stacked-deck affordance. */}
        <div
          aria-hidden
          className="mx-2 rounded-b-lg"
          style={{ height: 5, marginTop: 2, backgroundColor: 'var(--color-bg-card)', opacity: 0.55 }}
        />
        <div
          aria-hidden
          className="mx-4 rounded-b-lg"
          style={{ height: 5, marginTop: 2, backgroundColor: 'var(--color-bg-card)', opacity: 0.3 }}
        />
      </div>
    );
  }

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
              aria-expanded={true}
              aria-label={t('plugins.groups.collapseAria', { title })}
              onClick={() => setExpanded(false)}
              disabled={forceExpanded}
              className="flex items-center gap-2 min-w-0 disabled:cursor-default"
            >
              <h3
                className="text-[0.6875rem] font-medium uppercase tracking-wide truncate"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {title}
              </h3>
              {countPill}
              {!forceExpanded && (
                <ChevronUp
                  className="h-3 w-3 flex-shrink-0"
                  style={{ color: 'var(--color-text-tertiary)' }}
                />
              )}
            </button>
          ) : (
            <div className="flex items-center gap-2 min-w-0">
              <h3
                className="text-[0.6875rem] font-medium uppercase tracking-wide truncate"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {title}
              </h3>
              {countPill}
            </div>
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
      {children}
    </div>
  );
}
