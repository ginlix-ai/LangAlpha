import { Search, HelpCircle, Mail, LayoutGrid, Pencil } from 'lucide-react';
import AvatarDropdown from './AvatarDropdown';
import React, { useState, useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { searchStocks } from '@/lib/marketUtils';
import { useNavigate } from 'react-router-dom';
import { useIsMobile } from '@/hooks/useIsMobile';
import './DashboardHeader.css';

interface StockResult {
  symbol: string;
  name?: string;
  [key: string]: unknown;
}

interface DashboardHeaderProps {
  onStockSearch?: (symbol: string, stock: StockResult | null) => void;
  onScrollToTop?: () => void;
  /** When provided (desktop only), show the Classic/Custom segmented toggle and the Edit button in Custom mode. */
  layoutToggle?: {
    mode: 'classic' | 'custom';
    onModeChange: (mode: 'classic' | 'custom') => void;
    editMode?: boolean;
    onEditModeChange?: (edit: boolean) => void;
  };
}

const DashboardHeader: React.FC<DashboardHeaderProps> = ({ onStockSearch, onScrollToTop, layoutToggle }) => {
  const navigate = useNavigate();
  const isMobile = useIsMobile();
  const { t } = useTranslation();
  const [showHelpPopover, setShowHelpPopover] = useState(false);
  const helpRef = useRef<HTMLDivElement>(null);
  const searchInputRef = useRef<HTMLInputElement>(null);

  // Search state
  const [searchValue, setSearchValue] = useState('');
  const [searchResults, setSearchResults] = useState<StockResult[]>([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const [searchFocused, setSearchFocused] = useState(false);
  const dropdownRef = useRef<HTMLDivElement>(null);

  // Global "/" shortcut to focus search
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === '/' && !e.ctrlKey && !e.metaKey && !e.altKey) {
        const tag = document.activeElement?.tagName;
        if (tag === 'INPUT' || tag === 'TEXTAREA' || (document.activeElement as HTMLElement)?.isContentEditable) return;
        e.preventDefault();
        searchInputRef.current?.focus();
      }
    };
    document.addEventListener('keydown', handleKey);
    return () => document.removeEventListener('keydown', handleKey);
  }, []);

  // Close help popover on outside click
  useEffect(() => {
    if (!showHelpPopover) return;
    const handleClickOutside = (e: MouseEvent) => {
      if (helpRef.current && !helpRef.current.contains(e.target as Node)) {
        setShowHelpPopover(false);
      }
    };
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, [showHelpPopover]);

  // Stock search with debounce (300ms)
  useEffect(() => {
    const query = searchValue.trim();
    if (!query || query.length < 1) {
      setSearchResults([]);
      setSearchLoading(false);
      setShowDropdown(false);
      return;
    }

    const timeoutId = setTimeout(async () => {
      setSearchLoading(true);
      setShowDropdown(true);
      try {
        const result = await searchStocks(query, 12);
        setSearchResults((result.results || []) as StockResult[]);
      } catch (error) {
        console.error('Stock search failed:', error);
        setSearchResults([]);
      } finally {
        setSearchLoading(false);
      }
    }, 300);

    return () => clearTimeout(timeoutId);
  }, [searchValue]);

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

  const handleSelectStock = (stock: StockResult) => {
    if (stock?.symbol) {
      const symbol = stock.symbol.trim().toUpperCase();
      setSearchValue(symbol);
      setShowDropdown(false);
      if (onStockSearch) {
        onStockSearch(symbol, stock);
      } else {
        navigate(`/market?symbol=${encodeURIComponent(symbol)}`);
      }
    }
  };

  const handleSubmit = (e: React.FormEvent<HTMLFormElement>) => {
    e.preventDefault();
    const q = searchValue.trim();
    if (!q) return;

    // Default to the first search result when available
    if (searchResults.length > 0) {
      handleSelectStock(searchResults[0]);
      return;
    }

    const symbol = q.toUpperCase();
    setSearchValue(symbol);
    setShowDropdown(false);
    if (onStockSearch) {
      onStockSearch(symbol, null);
    } else {
      navigate(`/market?symbol=${encodeURIComponent(symbol)}`);
    }
  };

  return (
    <>
      <div
        className="sticky top-0 z-30 flex items-center justify-between px-4 sm:px-6 py-3"
        style={{
          backgroundColor: 'var(--color-bg-page)',
          borderBottom: '1px solid var(--color-border-muted)',
          backdropFilter: 'blur(12px)',
          WebkitBackdropFilter: 'blur(12px)',
          cursor: isMobile ? 'pointer' : undefined,
        }}
        onClick={(e) => {
          if (!isMobile) return;
          if ((e.target as HTMLElement).closest('button, a, input, form')) return;
          onScrollToTop?.();
        }}
      >
        {/* Search */}
        <div className="flex-1 min-w-0 max-w-xl">
          <div className="dashboard-search-wrapper" ref={dropdownRef}>
            <form
              onSubmit={handleSubmit}
              className="dashboard-search-form relative group flex items-center gap-2 h-10 px-3 rounded-xl border transition-all"
              style={{
                backgroundColor: 'var(--color-bg-input)',
                borderColor: searchFocused ? 'var(--color-accent-primary)' : 'var(--color-border-muted)',
                boxShadow: searchFocused ? '0 0 0 1px var(--color-accent-soft)' : 'none',
              }}
            >
              <Search
                className="dashboard-search-icon transition-colors"
                style={{ color: searchFocused ? 'var(--color-accent-primary)' : 'var(--color-icon-muted)' }}
              />
              <input
                ref={searchInputRef}
                type="text"
                placeholder={t('dashboard.searchPlaceholder')}
                value={searchValue}
                onChange={(e) => setSearchValue(e.target.value)}
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
              {/* "/" shortcut badge */}
              {!searchFocused && !searchValue && (
                <span
                  className="text-xs border rounded px-1.5 py-0.5 flex-shrink-0"
                  style={{
                    color: 'var(--color-text-quaternary, var(--color-text-secondary))',
                    borderColor: 'var(--color-border-default)',
                  }}
                >
                  /
                </span>
              )}
            </form>
            {showDropdown && searchValue.trim() && (
              <div className="dashboard-search-dropdown">
                {searchLoading ? (
                  <div className="dashboard-search-dropdown-item dashboard-search-dropdown-loading">
                    {t('dashboard.searching')}
                  </div>
                ) : searchResults.length === 0 ? (
                  <div className="dashboard-search-dropdown-item dashboard-search-dropdown-empty">
                    {t('dashboard.noResults')}
                  </div>
                ) : (
                  searchResults.slice(0, 12).map((stock, index) => (
                    <button
                      key={`${stock.symbol}-${index}`}
                      type="button"
                      className="dashboard-search-dropdown-item"
                      onClick={() => handleSelectStock(stock)}
                    >
                      <span className="dashboard-search-dropdown-symbol">{stock.symbol}</span>
                      <span className="dashboard-search-dropdown-name">{stock.name || stock.symbol}</span>
                    </button>
                  ))
                )}
              </div>
            )}
          </div>
        </div>

        {/* Right actions */}
        <div className="flex items-center gap-3 ml-3 shrink-0">
          {/* Layout toggle — hidden on mobile */}
          {layoutToggle && !isMobile && (
            <>
              <div
                className="flex items-center rounded-lg border p-0.5"
                style={{
                  backgroundColor: 'var(--color-bg-input)',
                  borderColor: 'var(--color-border-muted)',
                }}
                role="tablist"
                aria-label={t('dashboard.layoutToggle.groupAria')}
              >
                <button
                  type="button"
                  onClick={() => layoutToggle.onModeChange('classic')}
                  role="tab"
                  aria-selected={layoutToggle.mode === 'classic'}
                  className="px-2.5 py-1 text-xs rounded-md transition-colors"
                  style={{
                    backgroundColor:
                      layoutToggle.mode === 'classic' ? 'var(--color-bg-elevated)' : 'transparent',
                    color:
                      layoutToggle.mode === 'classic'
                        ? 'var(--color-text-primary)'
                        : 'var(--color-text-secondary)',
                    fontWeight: layoutToggle.mode === 'classic' ? 600 : 400,
                  }}
                >
                  {t('dashboard.layoutToggle.classic')}
                </button>
                <button
                  type="button"
                  onClick={() => layoutToggle.onModeChange('custom')}
                  role="tab"
                  aria-selected={layoutToggle.mode === 'custom'}
                  className="px-2.5 py-1 text-xs rounded-md transition-colors flex items-center gap-1"
                  style={{
                    backgroundColor:
                      layoutToggle.mode === 'custom' ? 'var(--color-bg-elevated)' : 'transparent',
                    color:
                      layoutToggle.mode === 'custom'
                        ? 'var(--color-text-primary)'
                        : 'var(--color-text-secondary)',
                    fontWeight: layoutToggle.mode === 'custom' ? 600 : 400,
                  }}
                >
                  <LayoutGrid size={12} />
                  {t('dashboard.layoutToggle.custom')}
                </button>
              </div>
              {layoutToggle.mode === 'custom' && layoutToggle.onEditModeChange && (
                <button
                  type="button"
                  onClick={() => layoutToggle.onEditModeChange?.(!layoutToggle.editMode)}
                  className="p-2 rounded-md transition-colors"
                  style={{
                    color: layoutToggle.editMode ? 'var(--color-text-on-accent)' : 'var(--color-text-secondary)',
                    backgroundColor: layoutToggle.editMode ? 'var(--color-accent-primary)' : 'transparent',
                  }}
                  aria-label={t(layoutToggle.editMode ? 'dashboard.layoutToggle.exitEditLayout' : 'dashboard.layoutToggle.editLayout')}
                  title={t(layoutToggle.editMode ? 'dashboard.layoutToggle.exitEditLayout' : 'dashboard.layoutToggle.editLayout')}
                >
                  <Pencil size={16} />
                </button>
              )}
            </>
          )}

          {/* Help — hidden on mobile to save space */}
          <div className="relative hidden sm:block" ref={helpRef}>
            <button
              className="p-2 transition-colors"
              style={{ color: showHelpPopover ? 'var(--color-text-primary)' : 'var(--color-text-secondary)' }}
              onClick={() => setShowHelpPopover((prev) => !prev)}
              onMouseEnter={(e) => (e.currentTarget.style.color = 'var(--color-text-primary)')}
              onMouseLeave={(e) => {
                if (!showHelpPopover) e.currentTarget.style.color = 'var(--color-text-secondary)';
              }}
            >
              <HelpCircle size={20} />
            </button>
            {showHelpPopover && (
              <div
                className="absolute right-0 top-full mt-2 z-50 rounded-lg shadow-lg"
                style={{
                  backgroundColor: 'var(--color-bg-elevated)',
                  border: '1px solid var(--color-border-elevated)',
                  width: '280px',
                  maxWidth: 'calc(100vw - 32px)',
                  padding: '16px',
                }}
              >
                <p
                  className="text-sm font-medium mb-3"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {t('dashboard.contactMessage')}
                </p>
                {((import.meta.env.VITE_CONTACT_EMAILS as string) || '').split(',').filter(Boolean).map((email: string, idx: number, arr: string[]) => (
                  <div
                    key={email}
                    className="flex items-center gap-2 px-3 py-2 rounded-md cursor-pointer transition-colors hover:opacity-80"
                    style={{ backgroundColor: 'var(--color-bg-input)', marginBottom: idx < arr.length - 1 ? '8px' : undefined }}
                    onClick={() => {
                      window.location.href = `mailto:${email.trim()}`;
                      setShowHelpPopover(false);
                    }}
                  >
                    <Mail className="h-4 w-4 flex-shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                    <div className="min-w-0">
                      <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{t('dashboard.classic.emailLabel')}</p>
                      <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{email.trim()}</p>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>

          {/* Divider */}
          <div className="h-8 w-[1px] mx-2 hidden sm:block" style={{ backgroundColor: 'var(--color-border-muted)' }} />

          {/* User avatar + dropdown */}
          <AvatarDropdown />
        </div>
      </div>
    </>
  );
};

export default DashboardHeader;
