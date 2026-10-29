import React, { useEffect, useId, useRef, useState } from 'react';
import { Search } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';
import { normalizeSymbolInput, readTypedTicker, type StockSearchHit } from '@/lib/marketUtils';
import './SymbolSearch.css';

interface SymbolSearchProps {
  /** The hit rides along when there was one, so a host can show its name before the quote arrives. */
  onPick: (symbol: string, hit?: StockSearchHit) => void;
}

/**
 * The ticker search behind a chart's symbol. Enter takes the highlighted hit,
 * or the typed text when it already reads as a ticker, and neither of them
 * while a search is resting or in flight: `goog` + a fast Enter must not open
 * GOOG while GOOGL is on its way, and `nvda` typed over `goog` must not open
 * the GOOGL still listed underneath it. Dismissal belongs to whatever holds it
 * (a popover), so Escape is not handled here.
 */
export function SymbolSearch({ onPick }: SymbolSearchProps): React.ReactElement {
  const { t } = useTranslation();
  const listId = useId();
  const inputRef = useRef<HTMLInputElement>(null);
  const [query, setQuery] = useState('');
  const { hits, loading } = useSymbolSearch(query, 8);
  const [highlight, setHighlight] = useState(0);

  // Every host gets the same symbol: uppercase, and never an empty one.
  const pick = (symbol: string, hit?: StockSearchHit) => {
    const sym = normalizeSymbolInput(symbol);
    if (!sym) return;
    if (hit) onPick(sym, hit);
    else onPick(sym);
  };

  useEffect(() => { inputRef.current?.focus(); }, []);
  useEffect(() => { setHighlight(0); }, [hits]);

  const typedTicker = readTypedTicker(query);
  const optionId = (i: number) => `${listId}-opt-${i}`;
  const listOpen = hits.length > 0 || (query.trim() !== '' && !loading);

  const onKeyDown = (e: React.KeyboardEvent<HTMLInputElement>) => {
    if (e.key === 'ArrowDown') { e.preventDefault(); setHighlight((h) => Math.min(h + 1, hits.length - 1)); return; }
    if (e.key === 'ArrowUp') { e.preventDefault(); setHighlight((h) => Math.max(h - 1, 0)); return; }
    if (e.key === 'Enter') {
      e.preventDefault();
      // The list stays up through a refetch on purpose, so what it shows may
      // still answer the query that was typed over. Only settled hits count.
      const hit = hits[highlight];
      if (hit && !loading) pick(hit.symbol, hit);
      else if (typedTicker && !loading) pick(typedTicker);
    }
  };

  return (
    <div className="symbol-search">
      {/* The box draws the ring and the focused edge for the field inside it
          (tokens.css); the field itself stays borderless and unringed. */}
      <div className="symbol-search-field rings-within owns-its-edge">
        <Search className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
        <input
          ref={inputRef}
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          onKeyDown={onKeyDown}
          placeholder={t('marketView.symbolSearch.placeholder')}
          aria-label={t('marketView.symbolSearch.placeholder')}
          role="combobox"
          aria-expanded={listOpen}
          aria-controls={listId}
          aria-haspopup="listbox"
          aria-autocomplete="list"
          aria-activedescendant={hits[highlight] ? optionId(highlight) : undefined}
          autoComplete="off"
          spellCheck={false}
        />
        {loading && <Loader size={12} className="text-current" />}
      </div>
      {listOpen && (
        <ul id={listId} role="listbox" className="symbol-search-list">
          {hits.map((hit, i) => (
            <li
              key={`${hit.symbol}-${i}`}
              id={optionId(i)}
              role="option"
              aria-selected={i === highlight}
              className={`symbol-search-hit${i === highlight ? ' is-active' : ''}`}
              onMouseEnter={() => setHighlight(i)}
              onMouseDown={(e) => e.preventDefault()}
              onClick={() => pick(hit.symbol, hit)}
            >
              <span className="symbol-search-symbol">{hit.symbol}</span>
              {hit.name && <span className="symbol-search-name">{hit.name}</span>}
            </li>
          ))}
          {hits.length === 0 && !loading && (
            <li role="presentation" className="symbol-search-none">
              {typedTicker ? t('marketView.symbolSearch.openTyped', { symbol: typedTicker }) : t('marketView.symbolSearch.noMatches')}
            </li>
          )}
        </ul>
      )}
    </div>
  );
}
