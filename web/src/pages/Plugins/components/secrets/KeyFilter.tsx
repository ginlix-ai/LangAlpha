import { useTranslation } from 'react-i18next';
import { Search, X } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { cn } from '@/lib/utils';
import type { KeyFilterState } from './useKeyFilter';

/**
 * The Secrets list's own filter. The page header's search belongs to the
 * sections that list what can be installed; a long list of keys needs one
 * too, kept in the list it narrows, with the one narrowing that matters when
 * the account is full: the keys no connector reads.
 */
export function KeyFilter({ filter }: { filter: KeyFilterState }) {
  const { t } = useTranslation();
  const { query, setQuery, unreadOnly, setUnreadOnly, unreadCount } = filter;
  const placeholder = t('vault.filter.placeholder');
  return (
    <div className="vault-filter">
      <div className="vault-search">
        <Search className="vault-search-icon" aria-hidden />
        <input
          type="search"
          value={query}
          placeholder={placeholder}
          aria-label={placeholder}
          spellCheck={false}
          autoComplete="off"
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === 'Escape' && query) {
              e.preventDefault();
              e.stopPropagation();
              setQuery('');
            }
          }}
        />
        {query && (
          <button
            type="button"
            className="vault-search-clear"
            aria-label={t('vault.filter.clear')}
            onClick={() => setQuery('')}
          >
            <X className="h-3.5 w-3.5" />
          </button>
        )}
      </div>
      {/* Not offered while what reads each key is unknown. */}
      {unreadCount !== undefined && (unreadCount > 0 || unreadOnly) && (
        <button
          type="button"
          aria-pressed={unreadOnly}
          className={cn('surface-chip', unreadOnly && 'is-on')}
          onClick={() => setUnreadOnly(!unreadOnly)}
        >
          {t('vault.filter.unread')}
          <span className="surface-count">{unreadCount}</span>
        </button>
      )}
    </div>
  );
}

/** In the list's place when the filter matches nothing: what it looked for. */
export function KeyFilterEmpty({ filter }: { filter: KeyFilterState }) {
  const { t } = useTranslation();
  const query = filter.query.trim();
  return (
    <div className="vault-filter-empty">
      <span>{query ? t('vault.filter.empty', { query }) : t('vault.filter.emptyUnread')}</span>
      <HeaderButton variant="secondary" icon={X} onClick={filter.clear}>
        {t('vault.filter.clear')}
      </HeaderButton>
    </div>
  );
}
