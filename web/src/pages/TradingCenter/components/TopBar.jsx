import React, { useState, useEffect, useRef } from 'react';
import { Search } from 'lucide-react';
import { searchStocks } from '../utils/api';
import './TopBar.css';

const TopBar = ({ onStockSearch }) => {
  const [searchValue, setSearchValue] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searchLoading, setSearchLoading] = useState(false);
  const [showDropdown, setShowDropdown] = useState(false);
  const dropdownRef = useRef(null);

  // Same API and debounce as Dashboard Add Watchlist: GET /api/v1/market-data/search/stocks
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

  useEffect(() => {
    function handleClickOutside(event) {
      if (dropdownRef.current && !dropdownRef.current.contains(event.target)) {
        setShowDropdown(false);
      }
    }
    document.addEventListener('mousedown', handleClickOutside);
    return () => document.removeEventListener('mousedown', handleClickOutside);
  }, []);

  const handleSelectStock = (stock) => {
    if (stock?.symbol) {
      const symbol = stock.symbol.trim().toUpperCase();
      onStockSearch(symbol, stock);
      setSearchValue(symbol);
      setShowDropdown(false);
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    const q = searchValue.trim();
    if (q) {
      onStockSearch(q.toUpperCase(), null);
      setShowDropdown(false);
    }
  };

  return (
    <div className="trading-top-bar">
      <div className="trading-top-bar-left">
        <h1 className="trading-top-bar-title">Trade</h1>
        <div className="trading-search-wrapper" ref={dropdownRef}>
          <form onSubmit={handleSubmit} className="trading-search-form">
            <Search className="trading-search-icon" size={18} />
            <input
              type="text"
              placeholder="Search by symbol or company name..."
              value={searchValue}
              onChange={(e) => setSearchValue(e.target.value)}
              onFocus={() => searchValue.trim() && setShowDropdown(true)}
              className="trading-search-input"
              autoComplete="off"
            />
          </form>
          {showDropdown && searchValue.trim() && (
            <div className="trading-search-dropdown">
              {searchLoading ? (
                <div className="trading-search-dropdown-item trading-search-dropdown-loading">
                  Searching...
                </div>
              ) : searchResults.length === 0 ? (
                <div className="trading-search-dropdown-item trading-search-dropdown-empty">
                  No results found
                </div>
              ) : (
                searchResults.slice(0, 12).map((stock, index) => (
                  <button
                    key={`${stock.symbol}-${index}`}
                    type="button"
                    className="trading-search-dropdown-item"
                    onClick={() => handleSelectStock(stock)}
                  >
                    <span className="trading-search-dropdown-symbol">{stock.symbol}</span>
                    <span className="trading-search-dropdown-name">{stock.name || stock.symbol}</span>
                  </button>
                ))
              )}
            </div>
          )}
        </div>
      </div>
      <div className="trading-top-bar-right">
        <div className="trading-top-bar-icon">
          <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
            <path d="M10 2a6 6 0 00-6 6c0 4.314 6 10 6 10s6-5.686 6-10a6 6 0 00-6-6zm0 8a2 2 0 110-4 2 2 0 010 4z"/>
          </svg>
        </div>
        <div className="trading-top-bar-icon">
          <svg width="20" height="20" viewBox="0 0 20 20" fill="currentColor">
            <path d="M10 2a8 8 0 100 16 8 8 0 000-16zm0 2a6 6 0 110 12 6 6 0 010-12zm0 4a2 2 0 100 4 2 2 0 000-4z"/>
          </svg>
        </div>
        <div className="trading-user-avatar">
          <svg width="32" height="32" viewBox="0 0 32 32" fill="currentColor">
            <circle cx="16" cy="12" r="6"/>
            <path d="M8 26c0-4.418 3.582-8 8-8s8 3.582 8 8"/>
          </svg>
        </div>
      </div>
    </div>
  );
};

export default TopBar;
