import React, { useState } from 'react';
import './TopBar.css';

const TopBar = ({ onStockSearch }) => {
  const [searchValue, setSearchValue] = useState('');

  const handleSearch = (e) => {
    e.preventDefault();
    if (searchValue.trim()) {
      onStockSearch(searchValue.trim().toUpperCase());
    }
  };

  return (
    <div className="trading-top-bar">
      <div className="trading-top-bar-left">
        <h1 className="trading-top-bar-title">Trade</h1>
        <form onSubmit={handleSearch} className="trading-search-form">
          <input
            type="text"
            placeholder="Search"
            value={searchValue}
            onChange={(e) => setSearchValue(e.target.value)}
            className="trading-search-input"
          />
        </form>
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
