import { Search } from 'lucide-react';
import React, { useEffect, useRef, useState, type CSSProperties } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { PaletteSearchField } from '@/components/nav/PaletteSearchField';
import { useSlashKey } from '@/hooks/useSlashKey';
import { useSymbolSearch } from '@/hooks/useSymbolSearch';
import { resolveDualName } from '@/lib/displayName';
import type { StockSearchHit } from '@/lib/marketUtils';
import { buildMarketViewUrl } from '@/pages/MarketView/utils/marketRoute';
import './DashboardHeader.css';

const FIELD_CLASS =
  'dashboard-search-form owns-its-edge relative flex items-center gap-2 h-10 px-3 rounded-[var(--cs-radius-control,0.75rem)] border';

// The open card styles draw it as their other fields, open and flat, and
// focus darkens its edge to ink rather than lighting it amber (the focus
// edge is the shared rule's, tokens.css).
const fieldStyle = (focused: boolean) =>
  ({
    '--focus-edge': 'var(--color-shell-ink, var(--color-accent-primary))',
    '--focus-halo': 'var(--cs-frame-shadow, 0 0 0 1px var(--color-accent-soft))',
    backgroundColor: 'var(--color-shell-fill, var(--color-bg-input))',
    borderColor: focused ? 'var(--color-shell-ink, var(--color-accent-primary))' : 'var(--color-shell-edge, var(--color-border-muted))',
    boxShadow: focused ? 'var(--cs-frame-shadow, 0 0 0 1px var(--color-accent-soft))' : 'none',
  }) as CSSProperties;

/** The `/` that brings the field up. */
function SlashBadge() {
  return (
    <span
      aria-hidden
      className="text-xs border rounded px-1.5 py-0.5 shrink-0"
      style={{
        color: 'var(--color-text-quaternary, var(--color-text-secondary))',
        borderColor: 'var(--color-border-default)',
      }}
    >
      /
    </span>
  );
}

/**
 * The phone's field: the command palette's, at Tickers. One search box
 * rather than a dropdown under a header the keyboard crowds out; a picked
 * ticker opens the same Market view.
 */
export function DashboardPaletteField() {
  const { t } = useTranslation();
  return (
    <PaletteSearchField
      start={{ kind: 'tickers' }}
      aria-label={t('dashboard.searchLabel')}
      className={`${FIELD_CLASS} text-left`}
      style={fieldStyle(false)}
    >
      <Search className="dashboard-search-icon" style={{ color: 'var(--color-icon-muted)' }} />
      <span className="dashboard-search-input dashboard-search-button-text">
        <span className="dashboard-search-placeholder">{t('dashboard.searchPlaceholder')}</span>
      </span>
      <SlashBadge />
    </PaletteSearchField>
  );
}

/** The desktop's field: symbols in a dropdown under it, Enter opening the first in Market. */
export function DashboardSymbolField() {
  const navigate = useNavigate();
  const { t } = useTranslation();
  const searchInputRef = useRef<HTMLInputElement>(null);
  const [searchValue, setSearchValue] = useState('');
  const { hits: searchResults, loading: searchLoading } = useSymbolSearch(searchValue, 12);
  const [showDropdown, setShowDropdown] = useState(false);
  const [searchFocused, setSearchFocused] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  useSlashKey(() => searchInputRef.current?.focus());

  // Close search dropdown on outside click
  useEffect(() => {
    function handleClickOutside(event: MouseEvent) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target as Node)) {
        setShowDropdown(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const openSymbol = (symbol: string) => {
    setSearchValue(symbol);
    setShowDropdown(false);
    navigate(buildMarketViewUrl({ symbol }));
  };

  const handleSelectStock = (stock: StockSearchHit) => {
    openSymbol(stock.symbol.trim().toUpperCase());
  };

  const handleSubmit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const q = searchValue.trim();
    if (!q) return;
    // Nothing is taken while a search is resting or in flight: the rows on
    // screen may still answer the query that was typed over, and the raw text
    // may yet resolve to a hit of its own. The same rule as SymbolSearch.
    if (searchLoading) return;

    // Default to the first search result when available
    if (searchResults.length > 0) {
      handleSelectStock(searchResults[0]);
      return;
    }

    openSymbol(q.toUpperCase());
  };

  return (
    <div className="dashboard-search-wrapper" ref={dropdownRef}>
      <form onSubmit={handleSubmit} className={`${FIELD_CLASS} group transition-all`} style={fieldStyle(searchFocused)}>
        <Search
          className="dashboard-search-icon transition-colors"
          style={{
            color: searchFocused ? 'var(--color-shell-ink, var(--color-accent-primary))' : 'var(--color-icon-muted)',
          }}
        />
        <input
          ref={searchInputRef}
          type="text"
          aria-label={t('dashboard.searchLabel')}
          placeholder={t('dashboard.searchPlaceholder')}
          value={searchValue}
          onChange={(e) => { setSearchValue(e.target.value); setShowDropdown(e.target.value.trim() !== ''); }}
          onFocus={() => {
            setSearchFocused(true);
            if (searchValue.trim()) setShowDropdown(true);
          }}
          onBlur={() => setSearchFocused(false)}
          className="dashboard-search-input"
          autoComplete="off"
          style={{
            backgroundColor: 'transparent',
            border: 'none',
            color: 'var(--color-text-primary)',
          }}
        />
        {!searchFocused && !searchValue && <SlashBadge />}
      </form>
      {showDropdown && searchValue.trim() && (
        <div data-slot="menu" className="dashboard-search-dropdown">
          {searchLoading && searchResults.length === 0 ? (
            <div className="dashboard-search-dropdown-item dashboard-search-dropdown-loading">
              {t('dashboard.searching')}
            </div>
          ) : searchResults.length === 0 ? (
            <div className="dashboard-search-dropdown-item dashboard-search-dropdown-empty">
              {t('dashboard.noResults')}
            </div>
          ) : (
            searchResults.map((stock, index) => {
              const dualName = resolveDualName(
                { name: stock.name, nameLocal: stock.nameLocal, nameEn: stock.nameEn },
                stock.symbol,
              );
              return (
                <button
                  key={`${stock.symbol}-${index}`}
                  type="button"
                  data-slot="menu-item"
                  className="dashboard-search-dropdown-item"
                  onClick={() => handleSelectStock(stock)}
                >
                  <span className="dashboard-search-dropdown-symbol">{stock.symbol}</span>
                  <span className="dashboard-search-dropdown-name">
                    {dualName.primary}
                    {dualName.secondary && (
                      <span className="dashboard-search-dropdown-name-secondary">{dualName.secondary}</span>
                    )}
                  </span>
                </button>
              );
            })
          )}
        </div>
      )}
    </div>
  );
}
