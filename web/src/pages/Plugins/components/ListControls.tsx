import { useTranslation } from 'react-i18next';
import { ListChecks, Search, X } from 'lucide-react';
import { HeaderButton } from '@/pages/ChatAgent/components/mcp/McpPrimitives';

/**
 * The tab-level filter + select-mode entry, shared by all three lists. The
 * filter is client-side and fans open every deck it matches into; Select
 * flips the rows into checkbox mode with the floating BulkActionBar.
 */

export function ListControls({
  filter,
  onFilterChange,
  selecting,
  onStartSelect,
  selectDisabled = false,
}: {
  filter: string;
  onFilterChange: (value: string) => void;
  selecting: boolean;
  onStartSelect: () => void;
  selectDisabled?: boolean;
}) {
  const { t } = useTranslation();
  return (
    <div className="flex items-center justify-end gap-1.5">
      <div className="relative">
        <Search
          className="h-3 w-3 absolute left-2 top-1/2 -translate-y-1/2 pointer-events-none"
          style={{ color: 'var(--color-text-tertiary)' }}
        />
        <input
          role="searchbox"
          value={filter}
          onChange={(e) => onFilterChange(e.target.value)}
          placeholder={t('plugins.filter.placeholder')}
          aria-label={t('plugins.filter.placeholder')}
          spellCheck={false}
          className="text-xs pl-6 pr-6 py-1.5 rounded-md w-40 focus:w-52 transition-all outline-none"
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
            className="absolute right-1.5 top-1/2 -translate-y-1/2 p-0.5 rounded hover:bg-foreground/10"
            style={{ color: 'var(--color-text-tertiary)' }}
          >
            <X className="h-3 w-3" />
          </button>
        )}
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
