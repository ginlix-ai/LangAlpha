import { useState } from 'react';
import type { QueryClient } from '@tanstack/react-query';
import { matchesStateFilter, type StateFilter } from '../components/ListControls';
import {
  useBulkRunner,
  useBulkSelection,
  type BulkSelection,
  type BulkTarget,
} from '../components/useBulkSelection';

/**
 * The list chrome every Plugins tab wears: the search box, the state pills,
 * select mode with its bulk runner, and the two verdicts that depend on all of
 * them — whether a deck must stay open, and whether a narrowed list has
 * anything left to show.
 *
 * The verdicts are the reason this is a hook rather than four `useState`s.
 * They were being re-derived per section, and MCP derived them from its own
 * list while the plugin decks below it were filtered independently — so
 * filtering by a plugin's name printed "No matches" directly above the deck
 * that had matched. One narrowing question, answered once, for every section
 * of every tab.
 */

export interface PluginListSurface {
  filter: string;
  setFilter: (value: string) => void;
  stateFilter: StateFilter;
  setStateFilter: (value: StateFilter) => void;
  selection: BulkSelection;
  progress: { done: number; total: number } | null;
  run: (targets: BulkTarget[]) => void;
  /** A search term or a state pill is narrowing the list. */
  narrowed: boolean;
  /** Filter and select mode both need every row visible. */
  forceExpanded: boolean;
  /** The row-level predicate behind the state pills. */
  matchesState: (enabled: boolean, attention?: boolean) => boolean;
  /**
   * Render the single "No matches" notice. Pass the WHOLE population the tab
   * can show, across every section: a notice keyed on one section is a notice
   * that can sit above another section's matching rows.
   */
  noMatches: (visibleTotal: number) => boolean;
  /**
   * Render a named section at all (its header, its hint, its empty-state
   * invitation). A narrowed section with no surviving rows is dropped whole:
   * the notice above already says why, and a bare header reads as a glitch.
   */
  keepsSection: (sectionRows: number) => boolean;
}

export function usePluginListSurface(
  opts: {
    /** The tab's invalidation radius after a bulk run (default: plugin-wide). */
    invalidate?: (qc: QueryClient) => void;
  } = {},
): PluginListSurface {
  const [filter, setFilter] = useState('');
  const [stateFilter, setStateFilter] = useState<StateFilter>('all');
  const selection = useBulkSelection();
  const { progress, run } = useBulkRunner(selection, opts.invalidate);

  const narrowed = !!filter.trim() || stateFilter !== 'all';

  return {
    filter,
    setFilter,
    stateFilter,
    setStateFilter,
    selection,
    progress,
    run,
    narrowed,
    forceExpanded: selection.selecting || narrowed,
    matchesState: (enabled, attention) =>
      matchesStateFilter(stateFilter, enabled, attention),
    noMatches: (visibleTotal) => narrowed && visibleTotal === 0,
    keepsSection: (sectionRows) => sectionRows > 0 || !narrowed,
  };
}
