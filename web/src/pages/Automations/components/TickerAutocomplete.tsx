import { useEffect, useMemo, useRef, useState } from 'react';
import { Input } from '@/components/ui/input';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';
import type { StockSearchHit } from '@/lib/marketUtils';
import { INDEX_SYMBOLS } from '../utils/price';

type SearchResult = StockSearchHit & { isIndex: boolean };

interface TickerAutocompleteProps {
  value: string;
  onChange: (value: string) => void;
  label: string;
}

export default function TickerAutocomplete({ value, onChange, label }: TickerAutocompleteProps) {
  const [query, setQuery] = useState(value);
  const { hits } = useSymbolSearch(query, 20);
  const [showDropdown, setShowDropdown] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  // Sync external value changes (e.g., template switch)
  useEffect(() => {
    setQuery(value);
  }, [value]);

  // The static index list answers on the keystroke; the stock hits join it
  // once the debounced search returns. The two never share a symbol.
  const results = useMemo<SearchResult[]>(() => {
    if (!query) return [];
    const q = query.toUpperCase();
    const indexMatches: SearchResult[] = INDEX_SYMBOLS
      .filter((idx) => idx.symbol.includes(q) || idx.name.toUpperCase().includes(q))
      .map((idx) => ({ symbol: idx.symbol, name: idx.name, exchangeShortName: 'INDEX', isIndex: true }));
    return [...indexMatches, ...hits.map((r) => ({ ...r, isIndex: false }))];
  }, [query, hits]);

  useEffect(() => {
    function handleClickOutside(e: MouseEvent) {
      if (containerRef.current && !containerRef.current.contains(e.target as Node)) {
        setShowDropdown(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  return (
    <div ref={containerRef} className="relative w-32">
      <Input
        aria-label={label}
        value={query}
        onChange={(e) => {
          const val = e.target.value.toUpperCase();
          setQuery(val);
          onChange(val);
          if (val.length >= 1) setShowDropdown(true);
        }}
        onFocus={() => { if (results.length > 0) setShowDropdown(true); }}
        placeholder="NVDA"
        required
        className="font-mono uppercase"
      />
      {showDropdown && results.length > 0 && (
        <div
          className="absolute z-50 mt-1 w-72 max-h-56 overflow-y-auto rounded-md border p-1 shadow-lg"
          style={{ backgroundColor: 'var(--color-bg-elevated)', borderColor: 'var(--color-border-default)' }}
        >
          {results.map((item) => (
            <button
              key={item.symbol}
              type="button"
              className="flex w-full items-center gap-2 rounded px-2.5 py-1.5 text-left text-[0.8125rem] hover:bg-accent/15"
              onClick={() => {
                setQuery(item.symbol);
                onChange(item.symbol);
                setShowDropdown(false);
              }}
            >
              <span className="min-w-[6ch] shrink-0 font-mono font-medium" style={{ color: 'var(--color-text-primary)' }}>
                {item.symbol}
              </span>
              {item.name && (
                <span className="truncate" style={{ color: 'var(--color-text-tertiary)' }}>
                  {item.name}
                  {item.exchangeShortName ? ` (${item.exchangeShortName})` : ''}
                </span>
              )}
            </button>
          ))}
        </div>
      )}
    </div>
  );
}
