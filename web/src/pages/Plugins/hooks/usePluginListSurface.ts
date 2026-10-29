import { useState, type ComponentProps } from 'react';
import { useSearchParams } from 'react-router';
import type { QueryClient } from '@tanstack/react-query';
import {
  CONTROLS_MIN_ROWS,
  matchesStateFilter,
  type ListControls,
  type StateCounts,
  type StateFilter,
} from '../components/ListControls';
import type { CellMode } from '../components/surface/Surface';
import {
  useBulkRunner,
  useBulkSelection,
  type BulkRunOptions,
  type BulkSelection,
  type BulkTarget,
} from '../components/useBulkSelection';
import { useJustAdded, type JustAddedKind } from './useJustAdded';
import { useOnceTrue } from './useOnceTrue';

/**
 * The list model every Plugins tab reads: the header's search (`?q=`), the
 * state pills and their counts, select mode with its bulk runner, the rows
 * all of that leaves on screen, and the verdicts that depend on them (must a
 * group stay unfolded, has a narrowed list anything left to show, is the
 * first-run block up), plus the one call that drops the narrowing again.
 *
 * One narrowing question, answered once over the tab's WHOLE population: when
 * a section answered it from its own rows while the groups below it were
 * filtered independently, filtering by a plugin's name printed "No matches"
 * directly above the group that had matched.
 */

export interface PluginListSurface {
  /** The header search, trimmed. */
  filter: string;
  stateFilter: StateFilter;
  selection: BulkSelection;
  progress: { done: number; total: number } | null;
  run: <R>(targets: BulkTarget<R>[], opts: BulkRunOptions<R>) => void;
  /** A search term or a state pill is narrowing the list. */
  narrowed: boolean;
  /** Drop both narrowing controls at once, the way the empty state offers. */
  reset: () => void;
}

/** What a folding `SurfaceGrid` takes. */
export interface GridFold {
  rows: number | undefined;
  hiddenNames: string[];
  reveal: boolean;
}

export interface PluginList<T> extends PluginListSurface {
  /** The rows the search and the state pill leave, in the order given. */
  visible: T[];
  cellMode: (row: T) => CellMode;
  /**
   * A group folded after `rows` rows, unfolded whole while the list is
   * narrowed or being selected from, and for good once it holds what was just
   * added, so the new cell is on screen and stays there after its mark fades.
   */
  fold: (rows: number, names: readonly string[]) => GridFold;
  /** The first-run catalog block is up. */
  showStart: boolean;
  /** The narrowing left nothing on screen. */
  nothingMatches: boolean;
  /** The pills and Select, or null where they are furniture. */
  controls: ComponentProps<typeof ListControls> | null;
}

export function usePluginListSurface<T>(opts: {
  rows: readonly T[];
  /** The header search over one row. */
  matches: (row: T, query: string) => boolean;
  key: (row: T) => string;
  /** What the just-added mark is keyed by. */
  name: (row: T) => string;
  isOn: (row: T) => boolean;
  /** Absent on a tab whose rows never need the user, which keeps that pill
   *  at zero and so hidden. */
  needsUser?: (row: T) => boolean;
  justAdded: JustAddedKind;
  /**
   * Nothing of the user's yet, so the catalog comes first. Held once true,
   * since an install from it lands in this same list and would otherwise
   * take the block, and the install's own dialogs, away mid-flow.
   */
  startWhen: boolean;
  /** The tab's invalidation radius after a bulk run (default: plugin-wide). */
  invalidate?: (qc: QueryClient) => void;
}): PluginList<T> {
  const { rows, key, name, isOn, needsUser = () => false } = opts;
  const [searchParams, setSearchParams] = useSearchParams();
  const [stateFilter, setStateFilter] = useState<StateFilter>('all');
  const selection = useBulkSelection();
  const { progress, run } = useBulkRunner(selection, opts.invalidate);
  const isJustAdded = useJustAdded();
  const startSeen = useOnceTrue(opts.startWhen);

  const filter = (searchParams.get('q') ?? '').trim();
  const narrowed = !!filter || stateFilter !== 'all';
  const forceExpanded = selection.selecting || narrowed;
  const showStart = startSeen && !narrowed;

  // The pills count what the search leaves, so each number is what pressing
  // that pill would show.
  const queried = rows.filter((row) => opts.matches(row, filter));
  const onCount = queried.filter(isOn).length;
  const counts: StateCounts = {
    all: queried.length,
    on: onCount,
    off: queried.length - onCount,
    attention: queried.filter(needsUser).length,
  };
  const visible = queried.filter((row) =>
    matchesStateFilter(stateFilter, isOn(row), needsUser(row)),
  );

  // Select mode works on the rows on screen.
  const shownKeys = visible.map(key);
  const allShownSelected = shownKeys.length > 0 && shownKeys.every((k) => selection.selected.has(k));

  const reset = () => {
    if (filter) {
      setSearchParams(
        (prev) => {
          const next = new URLSearchParams(prev);
          next.delete('q');
          return next;
        },
        { replace: true },
      );
    }
    setStateFilter('all');
  };

  return {
    filter,
    stateFilter,
    selection,
    progress,
    run,
    narrowed,
    reset,
    visible,
    cellMode: (row) => ({
      select: selection.selecting
        ? { checked: selection.selected.has(key(row)), onToggle: () => selection.toggle(key(row)) }
        : undefined,
      justAdded: isJustAdded(opts.justAdded, name(row)),
    }),
    fold: (limit, names) => ({
      rows: forceExpanded ? undefined : limit,
      hiddenNames: [...names],
      reveal: names.some((n) => isJustAdded(opts.justAdded, n)),
    }),
    showStart,
    nothingMatches: narrowed && visible.length === 0,
    // Not on a first visit: the catalog is the point there. A short list is
    // read whole, but a pill still narrowing it stays to be undone: hidden,
    // it would go on filtering with nothing on screen to clear it.
    controls:
      showStart || (rows.length < CONTROLS_MIN_ROWS && stateFilter === 'all')
        ? null
        : {
            counts,
            stateFilter,
            onStateFilterChange: setStateFilter,
            selecting: selection.selecting,
            onStartSelect: selection.start,
            shownCount: shownKeys.length,
            allShownSelected,
            onSelectShown: (on: boolean) => selection.setMany(shownKeys, on),
          },
  };
}
