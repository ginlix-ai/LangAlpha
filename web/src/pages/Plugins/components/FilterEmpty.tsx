import { Link } from 'react-router';
import { useTranslation } from 'react-i18next';
import { ChevronRight, X } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { useMarketplace } from '@/hooks/usePlugins';
import { VIEW_PARAM } from '../utils/detailParam';
import { inScope, matchesQuery, SCOPE_TAB, type DiscoverScope } from '../utils/marketplace';
import type { StateFilter } from './ListControls';
import { EmptyState } from './EmptyState';

/**
 * What a narrowed list says when nothing survives the filter, the same on
 * every section. "No matches" leaves the user to work out which of the two
 * controls emptied the page and whether the section is empty or merely
 * filtered, so the sentence names what was asked for, and the one control
 * that undoes it sits under the sentence. A search the catalog can answer
 * says so beside it: what is not installed yet may well be there.
 */

/** Both controls are narrowing: the sentence has to carry both. */
const WITH_QUERY: Record<StateFilter, string> = {
  all: 'plugins.filter.empty.query',
  on: 'plugins.filter.empty.queryOn',
  off: 'plugins.filter.empty.queryOff',
  attention: 'plugins.filter.empty.queryAttention',
};

/** Only a state pill is narrowing ('all' cannot narrow, so it never lands). */
const STATE_ONLY: Record<StateFilter, string> = {
  all: 'plugins.filter.noMatches',
  on: 'plugins.filter.empty.on',
  off: 'plugins.filter.empty.off',
  attention: 'plugins.filter.empty.attention',
};

export function FilterEmpty({
  noun,
  filter,
  stateFilter,
  onReset,
  catalogScope,
}: {
  /** What this section lists, already translated ("plugins", "connectors"). */
  noun: string;
  filter: string;
  stateFilter: StateFilter;
  /** Drop the search and the state pill both. */
  onReset: () => void;
  /** The catalog side to count matches on, where the section has one. */
  catalogScope?: DiscoverScope;
}) {
  const { t } = useTranslation();
  const query = filter.trim();
  return (
    <EmptyState
      message={
        query
          ? t(WITH_QUERY[stateFilter], { noun, query })
          : t(STATE_ONLY[stateFilter], { noun })
      }
      action={
        <div className="filter-empty-actions">
          <HeaderButton variant="secondary" icon={X} onClick={onReset}>
            {query ? t('plugins.filter.reset') : t('plugins.filter.resetState')}
          </HeaderButton>
          {catalogScope && query && <CatalogMatches scope={catalogScope} query={query} />}
        </div>
      }
    />
  );
}

/** "3 in the catalog match "wind"", carrying the search to Discover. Nothing
 *  when the catalog has none, or has not answered. */
function CatalogMatches({ scope, query }: { scope: DiscoverScope; query: string }) {
  const { t } = useTranslation();
  const { data } = useMarketplace();
  const count = data?.configured
    ? data.entries.filter((e) => inScope(e, scope) && matchesQuery(e, query)).length
    : 0;
  if (count === 0) return null;
  const params = new URLSearchParams({ tab: SCOPE_TAB[scope], [VIEW_PARAM]: 'discover', q: query });
  return (
    <Link to={`?${params.toString()}`} className="filter-empty-catalog">
      {t('plugins.filter.catalogMatches', { count, query })}
      <ChevronRight className="h-3.5 w-3.5" aria-hidden />
    </Link>
  );
}
