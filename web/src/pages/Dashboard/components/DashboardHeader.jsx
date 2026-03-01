import { Search, HelpCircle, User, Mail } from 'lucide-react';
import UserConfigPanel from './UserConfigPanel';
import React, { useState, useEffect, useRef } from 'react';
import { useTranslation } from 'react-i18next';
import { getCurrentUser, searchStocks } from '../utils/api';
import { useNavigate } from 'react-router-dom';
import { useAuth } from '../../../contexts/AuthContext';
import './DashboardHeader.css';

const DashboardHeader = ({ title = 'LangAlpha', onStockSearch, onModifyPreferences, onStartOnboarding }) => {
  const navigate = useNavigate();
  const { isLoggedIn } = useAuth();
  const { t } = useTranslation();
  const [isUserPanelOpen, setIsUserPanelOpen] = useState(false);
  const [avatarUrl, setAvatarUrl] = useState(null);
  const [showHelpPopover, setShowHelpPopover] = useState(false);
  const helpRef = useRef(null);

  // Search state
  const [searchValue, setSearchValue] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef(null);

  // Close help popover on outside click
  useEffect(() => {
    if (!showHelpPopover) return;
    const handleClickOutside = (e) => {
      if (helpRef.current && !helpRef.current.contains(e.target)) {
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
        const result = await searchStocks(query, 50);
        setSearchResults(result.results || []);
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
    function handleClickOutside(event) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setShowDropdown(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  useEffect(() => {
    if (!isLoggedIn) return;
    const fetchUser = async () => {
      try {
        const data = await getCurrentUser();
        const url = data?.user?.avatar_url;
        const version = data?.user?.updated_at;
        setAvatarUrl(url ? `${url}?v=${version}` : null);
      } catch (err) {
        console.error('Failed to fetch user:', err);
      }
    };
    fetchUser();
  }, [isLoggedIn]);

  // Note: Codex OAuth no longer uses server-side redirect callbacks.
  // The flow is now frontend-driven with a paste dialog in UserConfigPanel.

  const handlePanelClose = () => {
    setIsUserPanelOpen(false);
    if (isLoggedIn) {
      getCurrentUser().then(data => {
        const url = data?.user?.avatar_url;
        const version = data?.user?.updated_at;
        setAvatarUrl(url ? `${url}?v=${version}` : null);
      }).catch(() => {});
    }
  };

  // Handle stock selection from dropdown
  const handleSelectStock = (stock) => {
    if (stock?.symbol) {
      const symbol = stock.symbol.trim().toUpperCase();
      setSearchValue(symbol);
      setShowDropdown(false);
      // If onStockSearch callback is provided, use it; otherwise navigate
      if (onStockSearch) {
        onStockSearch(symbol, stock);
      } else {
        navigate(`/market?symbol=${encodeURIComponent(symbol)}`);
      }
    }
  };

  // Handle search form submit
  const handleSubmit = (e) => {
    e.preventDefault();
    const q = searchValue.trim();
    if (q) {
      const symbol = q.toUpperCase();
      setSearchValue(symbol);
      setShowDropdown(false);
      // If onStockSearch callback is provided, use it; otherwise navigate
      if (onStockSearch) {
        onStockSearch(symbol, null);
      } else {
        navigate(`/market?symbol=${encodeURIComponent(symbol)}`);
      }
    }
  };

  return (
    <>
      <div className="flex items-center justify-between px-3 sm:px-5 py-1.5" style={{ backgroundColor: 'var(--color-bg-card-gradient, var(--color-bg-elevated))', borderBottom: '1px solid var(--color-bg-card-border, var(--color-border-muted))', backdropFilter: 'blur(8px)', WebkitBackdropFilter: 'blur(8px)' }}>
        <h1 className="dashboard-title-font text-base font-medium" style={{ color: 'var(--color-text-primary)', letterSpacing: '0.15px' }}>{title}</h1>
        <div className="flex items-center gap-4 flex-1 max-w-md mx-2 sm:mx-8">
          <div className="dashboard-search-wrapper" ref={dropdownRef}>
            <form 
              onSubmit={handleSubmit} 
              className="dashboard-search-form flex items-center gap-2 h-11 px-3 rounded-xl border transition-colors"
              style={{
                backgroundColor: 'var(--color-bg-input)',
                borderColor: 'var(--color-border-muted)',
              }}
            >
              <Search className="dashboard-search-icon" style={{ color: 'var(--color-icon-muted)' }} />
              <input
                type="text"
                placeholder={t('dashboard.searchPlaceholder')}
                value={searchValue}
                onChange={(e) => setSearchValue(e.target.value)}
                onFocus={() => searchValue.trim() && setShowDropdown(true)}
                className="dashboard-search-input"
                autoComplete="off"
                style={{
                  backgroundColor: 'transparent',
                  border: 'none',
                  color: 'var(--color-text-primary)',
                }}
              />
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
        <div className="flex items-center gap-4">
          <div className="relative" ref={helpRef}>
            <HelpCircle
              className="h-5 w-5 cursor-pointer transition-colors"
              style={{ color: showHelpPopover ? 'var(--color-text-primary)' : 'var(--color-icon-muted)' }}
              onClick={() => setShowHelpPopover((prev) => !prev)}
            />
            {showHelpPopover && (
              <div
                className="absolute right-0 top-full mt-2 z-50 rounded-lg shadow-lg"
                style={{
                  backgroundColor: 'var(--color-bg-elevated)',
                  border: '1px solid var(--color-border-elevated)',
                  width: '280px',
                  padding: '16px',
                }}
              >
                <p
                  className="text-sm font-medium mb-3"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {t('dashboard.contactMessage')}
                </p>
                {(import.meta.env.VITE_CONTACT_EMAILS || '').split(',').filter(Boolean).map((email, idx, arr) => (
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
                      <p className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>Email</p>
                      <p className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>{email.trim()}</p>
                    </div>
                  </div>
                ))}
              </div>
            )}
          </div>
          <div 
            className="h-7 w-7 rounded-full flex items-center justify-center cursor-pointer transition-colors hover:bg-primary/30 overflow-hidden" 
            style={{ backgroundColor: 'var(--color-accent-soft)' }}
            onClick={() => setIsUserPanelOpen(true)}
          >
            {avatarUrl ? (
              <img src={avatarUrl} alt="avatar" className="h-full w-full object-cover" onError={() => setAvatarUrl(null)} />
            ) : (
              <User className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
            )}
          </div>
        </div>
      </div>
      
      <UserConfigPanel
        isOpen={isUserPanelOpen}
        onClose={handlePanelClose}
        onModifyPreferences={onModifyPreferences}
        onStartOnboarding={onStartOnboarding}
        initialTab={null}
      />
    </>
  );
};

export default DashboardHeader;
