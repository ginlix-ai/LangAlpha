import React, { useState, useCallback } from 'react';
import { X, ChevronLeft, ChevronRight, BarChart3 } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useWatchlistData } from '../../Dashboard/hooks/useWatchlistData';
import { usePortfolioData } from '../../Dashboard/hooks/usePortfolioData';
import AddWatchlistItemDialog from '../../Dashboard/components/AddWatchlistItemDialog';
import AddPortfolioHoldingDialog from '../../Dashboard/components/AddPortfolioHoldingDialog';
import ConfirmDialog from '../../Dashboard/components/ConfirmDialog';
import './TradingSidebarPanel.css';

function TradingSidebarPanel({ activeSymbol, onSymbolClick }) {
  const navigate = useNavigate();
  const [expanded, setExpanded] = useState(false);
  const [activeTab, setActiveTab] = useState('watchlist');
  const watchlist = useWatchlistData();
  const portfolio = usePortfolioData();

  const [deleteConfirm, setDeleteConfirm] = useState({
    open: false,
    title: '',
    message: '',
    onConfirm: null,
  });

  const handlePortfolioDelete = useCallback(
    (holdingId) => {
      setDeleteConfirm(portfolio.handleDelete(holdingId));
    },
    [portfolio.handleDelete]
  );

  const runDeleteConfirm = useCallback(async () => {
    if (deleteConfirm.onConfirm) await deleteConfirm.onConfirm();
    setDeleteConfirm((p) => ({ ...p, open: false }));
  }, [deleteConfirm.onConfirm]);

  const formatPrice = (price) => {
    if (price == null || price === 0) return '--';
    return Number(price).toLocaleString(undefined, { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };

  const formatChange = (val) => {
    if (val == null) return '--';
    const sign = val >= 0 ? '+' : '';
    return `${sign}${val.toFixed(2)}%`;
  };

  const changeClass = (isPositive, val) => {
    if (val == null || val === 0) return 'trading-sidebar-row-change--neutral';
    return isPositive ? 'trading-sidebar-row-change--positive' : 'trading-sidebar-row-change--negative';
  };

  const renderRows = (items, keyField, changeField, onDelete) => {
    return items.map((row) => {
      const isActive = activeSymbol && row.symbol === activeSymbol.toUpperCase();
      return (
        <div
          key={row[keyField]}
          className={`trading-sidebar-row${isActive ? ' trading-sidebar-row--active' : ''}`}
          onClick={() => onSymbolClick?.(row.symbol)}
        >
          <span className="trading-sidebar-row-symbol">{row.symbol}</span>
          <span className="trading-sidebar-row-price">{formatPrice(row.price)}</span>
          <span className={`trading-sidebar-row-change ${changeClass(row.isPositive, row[changeField])}`}>
            {formatChange(row[changeField])}
          </span>
          <span className="trading-sidebar-row-actions">
            <button
              className="trading-sidebar-row-delete"
              onClick={(e) => { e.stopPropagation(); onDelete(row[keyField]); }}
              title="Remove"
            >
              <X size={12} />
            </button>
          </span>
        </div>
      );
    });
  };

  const renderSkeletons = () =>
    Array.from({ length: 4 }).map((_, i) => (
      <div key={i} className="trading-sidebar-skeleton">
        <div className="trading-sidebar-skeleton-bar" style={{ width: 48 }} />
        <div className="trading-sidebar-skeleton-bar" style={{ width: 54, marginLeft: 'auto' }} />
        <div className="trading-sidebar-skeleton-bar" style={{ width: 52 }} />
      </div>
    ));

  const isWatchlist = activeTab === 'watchlist';
  const currentRows = isWatchlist ? watchlist.rows : portfolio.rows;
  const currentLoading = isWatchlist ? watchlist.loading : portfolio.loading;

  // Collapsed state — thin toggle strip
  if (!expanded) {
    return (
      <div className="trading-sidebar trading-sidebar--collapsed">
        <button
          className="trading-sidebar-expand-btn"
          onClick={() => setExpanded(true)}
          title="Show Watchlist & Portfolio"
        >
          <BarChart3 size={16} />
          <ChevronLeft size={14} />
        </button>

        {/* Dialogs still need to be mounted for add operations */}
        <AddWatchlistItemDialog
          open={watchlist.modalOpen}
          onClose={() => watchlist.setModalOpen(false)}
          onAdd={watchlist.handleAdd}
          watchlistId={watchlist.currentWatchlistId}
        />
        <AddPortfolioHoldingDialog
          open={portfolio.modalOpen}
          onClose={() => portfolio.setModalOpen(false)}
          onAdd={portfolio.handleAdd}
        />
      </div>
    );
  }

  return (
    <div className="trading-sidebar">
      <ConfirmDialog
        open={deleteConfirm.open}
        title={deleteConfirm.title}
        message={deleteConfirm.message}
        confirmLabel="Delete"
        onConfirm={runDeleteConfirm}
        onOpenChange={(open) => !open && setDeleteConfirm((p) => ({ ...p, open: false }))}
      />

      {/* Tab toggle */}
      <div className="trading-sidebar-tabs">
        <button
          className={`trading-sidebar-tab${activeTab === 'watchlist' ? ' trading-sidebar-tab--active' : ''}`}
          onClick={() => setActiveTab('watchlist')}
        >
          Watchlist
        </button>
        <button
          className={`trading-sidebar-tab${activeTab === 'portfolio' ? ' trading-sidebar-tab--active' : ''}`}
          onClick={() => setActiveTab('portfolio')}
        >
          Portfolio
        </button>
        <button
          className="trading-sidebar-collapse-btn"
          onClick={() => setExpanded(false)}
          title="Collapse"
        >
          <ChevronRight size={14} />
        </button>
      </div>

      {/* Section header */}
      <div className="trading-sidebar-section-header">
        <span className="trading-sidebar-section-title">
          {isWatchlist ? 'WATCHLIST' : 'PORTFOLIO'}
        </span>
        <button
          className="trading-sidebar-add-btn"
          onClick={() =>
            isWatchlist
              ? watchlist.setModalOpen(true)
              : portfolio.setModalOpen(true)
          }
          title={isWatchlist ? 'Add to watchlist' : 'Add holding'}
        >
          +
        </button>
      </div>

      {/* List */}
      <div className="trading-sidebar-list">
        {currentLoading
          ? renderSkeletons()
          : currentRows.length === 0
            ? (
              <div className="trading-sidebar-empty">
                <div className="trading-sidebar-empty-text">
                  {isWatchlist
                    ? 'No stocks in your watchlist yet. Click + to add one.'
                    : 'No holdings in your portfolio yet. Click + to add one.'}
                </div>
              </div>
            )
            : isWatchlist
              ? renderRows(currentRows, 'item_id', 'changePercent', watchlist.handleDelete)
              : renderRows(currentRows, 'holding_id', 'unrealizedPlPercent', handlePortfolioDelete)}
      </div>

      {/* Footer */}
      {currentRows.length > 0 && !currentLoading && (
        <div className="trading-sidebar-footer">
          <button
            className="trading-sidebar-footer-link"
            onClick={() => navigate('/')}
          >
            View all
          </button>
        </div>
      )}

      {/* Dialogs */}
      <AddWatchlistItemDialog
        open={watchlist.modalOpen}
        onClose={() => watchlist.setModalOpen(false)}
        onAdd={watchlist.handleAdd}
        watchlistId={watchlist.currentWatchlistId}
      />
      <AddPortfolioHoldingDialog
        open={portfolio.modalOpen}
        onClose={() => portfolio.setModalOpen(false)}
        onAdd={portfolio.handleAdd}
      />
    </div>
  );
}

export default TradingSidebarPanel;
