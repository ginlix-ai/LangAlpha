import { useTranslation } from 'react-i18next';
import { ListChecks } from 'lucide-react';
import { cn } from '@/lib/utils';

/**
 * The row under a list's header, shared by Plugins, Connectors and Skills:
 * the state pills with their counts, and the way into select mode. The search
 * is the page header's, so this row only narrows by state. A non-'all' pill
 * unfolds every group it leaves rows in; Select turns the cells into
 * checkboxes under the BulkActionBar.
 */

export type StateFilter = 'all' | 'on' | 'off' | 'attention';

export type StateCounts = Record<StateFilter, number>;

/** Below this many rows a list is read whole, and pills or a Select over it
 *  are furniture. */
export const CONTROLS_MIN_ROWS = 6;

/** The shared predicate behind the state pills. `attention` is the row's own
 *  definition of needing a human (broken OAuth, missing secret). */
export function matchesStateFilter(
  stateFilter: StateFilter,
  enabled: boolean,
  attention = false,
): boolean {
  switch (stateFilter) {
    case 'on':
      return enabled;
    case 'off':
      return !enabled;
    case 'attention':
      return attention;
    default:
      return true;
  }
}

export function ListControls({
  counts,
  stateFilter,
  onStateFilterChange,
  selecting,
  onStartSelect,
  shownCount,
  allShownSelected,
  onSelectShown,
}: {
  /** Per pill, over the rows the search leaves. */
  counts: StateCounts;
  stateFilter: StateFilter;
  onStateFilterChange: (value: StateFilter) => void;
  selecting: boolean;
  onStartSelect: () => void;
  /** The rows on screen, which "Select all" chooses. */
  shownCount: number;
  allShownSelected: boolean;
  onSelectShown: (on: boolean) => void;
}) {
  const { t } = useTranslation();
  // A search that left nothing has nothing to narrow or select; the empty
  // state under it carries the way back.
  if (counts.all === 0 && stateFilter === 'all' && !selecting) return null;
  const pills: { key: StateFilter; label: string }[] = [
    { key: 'all', label: t('plugins.stateFilter.all') },
    { key: 'on', label: t('plugins.stateFilter.on') },
    { key: 'off', label: t('plugins.stateFilter.off') },
    { key: 'attention', label: t('plugins.stateFilter.attention') },
  ];
  // A pill that would show nothing is not offered, unless it is the one the
  // list is narrowed to, which has to stay to be undone.
  const shown = pills.filter((p) => p.key === 'all' || counts[p.key] > 0 || p.key === stateFilter);
  return (
    <div className="list-controls">
      <div role="group" aria-label={t('plugins.stateFilter.label')} className="list-controls-pills">
        {shown.map((pill) => (
          <button
            key={pill.key}
            type="button"
            aria-pressed={stateFilter === pill.key}
            className={cn('surface-chip', stateFilter === pill.key && 'is-on')}
            onClick={() => onStateFilterChange(pill.key)}
          >
            {pill.label}
            <span className="surface-count">{counts[pill.key]}</span>
          </button>
        ))}
      </div>
      <div className="list-controls-side">
        {selecting ? (
          <button
            type="button"
            className="list-controls-btn"
            disabled={shownCount === 0}
            onClick={() => onSelectShown(!allShownSelected)}
          >
            {allShownSelected
              ? t('plugins.bulk.selectNone')
              : t('plugins.bulk.selectAll', { count: shownCount })}
          </button>
        ) : (
          <button type="button" className="list-controls-btn" onClick={onStartSelect}>
            <ListChecks className="h-3.5 w-3.5" aria-hidden />
            {t('plugins.bulk.select')}
          </button>
        )}
      </div>
    </div>
  );
}
