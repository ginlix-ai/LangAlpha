import type React from 'react';
import { useTranslation } from 'react-i18next';
import { Search } from 'lucide-react';
import { Input } from '@/components/ui/input';
import type { StockSearchHit } from '@/lib/marketUtils';
import { FormCard } from './FormCard';

/**
 * The search page of the add dialogs: the field, and the hits under it read
 * as a card's table, the ticker in mono and the name in the words' face.
 */
export function SymbolSearchCard({
  query,
  onQueryChange,
  hits,
  loading,
  onSelect,
  copy,
}: {
  query: string;
  onQueryChange: (next: string) => void;
  hits: StockSearchHit[];
  loading: boolean;
  onSelect: (hit: StockSearchHit) => void;
  /** The dialog's own strings, which say the same in both. */
  copy: { searching: string; noResults: string; searchHint: string; searchPlaceholder: string };
}): React.ReactElement {
  const { t } = useTranslation();
  const empty = loading && hits.length === 0
    ? copy.searching
    : hits.length === 0 && query.trim().length >= 1
      ? copy.noResults
      : hits.length === 0
        ? copy.searchHint
        : null;
  return (
    <FormCard title={t('common.search')} meta={hits.length > 0 ? hits.length : null}>
      <div className="relative">
        <Search className="absolute left-3 top-1/2 -translate-y-1/2 h-4 w-4" style={{ color: 'var(--color-text-secondary)' }} />
        <Input
          placeholder={copy.searchPlaceholder}
          value={query}
          onChange={(e) => onQueryChange(e.target.value)}
          className="dash-form-input pl-10 border"
          autoFocus
        />
      </div>
      <div className="dash-form-scroll">
        {empty !== null ? (
          <p className="term-word dash-form-empty">{empty}</p>
        ) : (
          <ul className="dash-form-hits">
            {hits.map((hit, index) => {
              const exchange = hit.exchangeShortName || hit.stockExchange;
              return (
                <li key={`${hit.symbol}-${index}`}>
                  <button type="button" onClick={() => onSelect(hit)} className="dash-form-hit">
                    <span className="term-code dash-form-hit-symbol">{hit.symbol}</span>
                    <span className="term-word dash-form-hit-name">{hit.name}</span>
                    {exchange && <span className="term-code dash-form-hit-exchange">{exchange}</span>}
                  </button>
                </li>
              );
            })}
          </ul>
        )}
      </div>
    </FormCard>
  );
}
