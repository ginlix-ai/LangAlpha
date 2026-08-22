import { useTranslation } from 'react-i18next';
import { ListChecks, Search, X } from 'lucide-react';
import { HeaderButton } from '@/pages/ChatAgent/components/mcp/McpPrimitives';

/**
 * The tab-level filter row, shared by all three lists: one generous search
 * input, the state pills, and select-mode entry. The filter is client-side
 * and fans open every deck it matches into; a non-'all' state pill does the
 * same. Select flips the rows into checkbox mode with the BulkActionBar.
 */

export type StateFilter = 'all' | 'on' | 'off' | 'attention';

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

function FilterPill({
  active,
  onClick,
  children,
}: {
  active: boolean;
  onClick: () => void;
  children: React.ReactNode;
}) {
  return (
    <button
      type="button"
      aria-pressed={active}
      onClick={onClick}
      className="px-2.5 py-1 text-xs rounded-full transition-colors whitespace-nowrap"
      style={
        active
          ? {
              color: 'var(--color-text-primary)',
              backgroundColor: 'var(--color-bg-card)',
              border: '1px solid var(--color-border-default)',
            }
          : {
              color: 'var(--color-text-tertiary)',
              border: '1px solid transparent',
            }
      }
    >
      {children}
    </button>
  );
}

export function ListControls({
  filter,
  onFilterChange,
  stateFilter,
  onStateFilterChange,
  showAttention = false,
  selecting,
  onStartSelect,
  selectDisabled = false,
}: {
  filter: string;
  onFilterChange: (value: string) => void;
  stateFilter: StateFilter;
  onStateFilterChange: (value: StateFilter) => void;
  /** Offer the "Needs attention" pill (lists whose rows can break). */
  showAttention?: boolean;
  selecting: boolean;
  onStartSelect: () => void;
  selectDisabled?: boolean;
}) {
  const { t } = useTranslation();
  const pills: { key: StateFilter; label: string }[] = [
    { key: 'all', label: t('plugins.stateFilter.all') },
    { key: 'on', label: t('plugins.stateFilter.on') },
    { key: 'off', label: t('plugins.stateFilter.off') },
    ...(showAttention
      ? [{ key: 'attention' as const, label: t('plugins.stateFilter.attention') }]
      : []),
  ];
  return (
    <div className="flex items-center gap-2 flex-wrap">
      <div className="relative flex-1 min-w-[12rem]">
        <Search
          className="h-3.5 w-3.5 absolute left-3 top-1/2 -translate-y-1/2 pointer-events-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        />
        <input
          role="searchbox"
          value={filter}
          onChange={(e) => onFilterChange(e.target.value)}
          placeholder={t('plugins.filter.placeholder')}
          aria-label={t('plugins.filter.placeholder')}
          spellCheck={false}
          className="text-sm pl-9 pr-8 py-2 rounded-md w-full outline-none"
          style={{
            color: 'var(--color-text-primary)',
            backgroundColor: 'var(--color-bg-input)',
            border: '1px solid var(--color-border-muted)',
          }}
        />
        {filter && (
          <button
            type="button"
            aria-label={t('plugins.filter.clear')}
            onClick={() => onFilterChange('')}
            className="absolute right-2 top-1/2 -translate-y-1/2 p-0.5 rounded hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <X className="h-3.5 w-3.5" />
          </button>
        )}
      </div>
      <div className="flex items-center gap-0.5">
        {pills.map((pill) => (
          <FilterPill
            key={pill.key}
            active={stateFilter === pill.key}
            onClick={() => onStateFilterChange(pill.key)}
          >
            {pill.label}
          </FilterPill>
        ))}
      </div>
      {!selecting && (
        <HeaderButton
          variant="secondary"
          icon={ListChecks}
          onClick={onStartSelect}
          disabled={selectDisabled}
        >
          {t('plugins.bulk.select')}
        </HeaderButton>
      )}
    </div>
  );
}
