import React, { useState, useCallback, useEffect, useMemo } from 'react';
import { X, ChevronLeft, ChevronRight, BarChart3 } from 'lucide-react';
import { useNavigate } from 'react-router-dom';
import { useWatchlistData } from '../../Dashboard/hooks/useWatchlistData';
import { usePortfolioData } from '../../Dashboard/hooks/usePortfolioData';
import { useMarketDataWSContext } from '../contexts/MarketDataWSContext';
import AddWatchlistItemDialog from '../../Dashboard/components/AddWatchlistItemDialog';
import AddPortfolioHoldingDialog from '../../Dashboard/components/AddPortfolioHoldingDialog';
import ConfirmDialog from '../../Dashboard/components/ConfirmDialog';
import './MarketSidebarPanel.css';

function MarketSidebarPanel({ activeSymbol, onSymbolClick }) {
  const navigate = useNavigate();
  const [expanded, setExpanded] = useState(false);
  const [activeTab, setActiveTab] = useState('watchlist');
  const watchlist = useWatchlistData();
  const portfolio = usePortfolioData();
  const { prices: wsPrices, connectionStatus: wsStatus, subscribe: wsSubscribe, unsubscribe: wsUnsubscribe } = useMarketDataWSContext();

  // Subscribe all sidebar symbols to WS feed
  useEffect(() => {
    const symbols = [...new Set([
      ...watchlist.rows.map((r) => r.symbol),
      ...portfolio.rows.map((r) => r.symbol),
    ])].filter(Boolean);
    if (symbols.length) wsSubscribe(symbols);
    return () => { if (symbols.length) wsUnsubscribe(symbols); };
  }, [watchlist.rows, portfolio.rows, wsSubscribe, wsUnsubscribe]);

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
    if (val == null || val === 0) return 'market-sidebar-row-change--neutral';
    return isPositive ? 'market-sidebar-row-change--positive' : 'market-sidebar-row-change--negative';
  };

  const renderRows = (items, keyField, changeField, onDelete) => {
    return items.map((row) => {
      const isActive = activeSymbol && row.symbol === activeSymbol.toUpperCase();
      return (
        <div
          key={row[keyField]}
          className={`market-sidebar-row${isActive ? ' market-sidebar-row--active' : ''}`}
          onClick={() => onSymbolClick?.(row.symbol)}
        >
          <span className="market-sidebar-row-symbol">{row.symbol}</span>
          <span className="market-sidebar-row-price">{formatPrice(row.price)}</span>
          <span className={`market-sidebar-row-change ${changeClass(row.isPositive, row[changeField])}`}>
            {formatChange(row[changeField])}
          </span>
          <span className="market-sidebar-row-actions">
            <button
              className="market-sidebar-row-delete"
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
      <div key={i} className="market-sidebar-skeleton">
        <div className="market-sidebar-skeleton-bar" style={{ width: 48 }} />
        <div className="market-sidebar-skeleton-bar" style={{ width: 54, marginLeft: 'auto' }} />
        <div className="market-sidebar-skeleton-bar" style={{ width: 52 }} />
      </div>
    ));

  const isWatchlist = activeTab === 'watchlist';
  const currentLoading = isWatchlist ? watchlist.loading : portfolio.loading;

  // Overlay WS live prices onto rows
  const currentRows = useMemo(() => {
    const rows = isWatchlist ? watchlist.rows : portfolio.rows;
    return rows.map((row) => {
      const ws = wsPrices.get(row.symbol);
      if (!ws) return row;
      const changeField = isWatchlist ? 'changePercent' : 'unrealizedPlPercent';
      const wsChangePercent = parseFloat(ws.changePercent);
      return {
        ...row,
        price: ws.price,
        [changeField]: isNaN(wsChangePercent) ? row[changeField] : wsChangePercent,
        isPositive: ws.change >= 0,
      };
    });
  }, [isWatchlist, watchlist.rows, portfolio.rows, wsPrices]);

  // Collapsed state — thin toggle strip
  if (!expanded) {
    return (
      <div className="market-sidebar market-sidebar--collapsed">
        <button
          className="market-sidebar-expand-btn"
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
    <div className="market-sidebar">
      <ConfirmDialog
        open={deleteConfirm.open}
        title={deleteConfirm.title}
        message={deleteConfirm.message}
        confirmLabel="Delete"
        onConfirm={runDeleteConfirm}
        onOpenChange={(open) => !open && setDeleteConfirm((p) => ({ ...p, open: false }))}
      />

      {/* Tab toggle */}
      <div className="market-sidebar-tabs">
        <button
          className={`market-sidebar-tab${activeTab === 'watchlist' ? ' market-sidebar-tab--active' : ''}`}
          onClick={() => setActiveTab('watchlist')}
        >
          Watchlist
        </button>
        <button
          className={`market-sidebar-tab${activeTab === 'portfolio' ? ' market-sidebar-tab--active' : ''}`}
          onClick={() => setActiveTab('portfolio')}
        >
          Portfolio
        </button>
        <button
          className="market-sidebar-collapse-btn"
          onClick={() => setExpanded(false)}
          title="Collapse"
        >
          <ChevronRight size={14} />
        </button>
      </div>

      {/* Section header */}
      <div className="market-sidebar-section-header">
        <span className="market-sidebar-section-title">
          {isWatchlist ? 'WATCHLIST' : 'PORTFOLIO'}
          {wsStatus === 'connected' && <span className="market-sidebar-live-dot" title="Live prices" />}
        </span>
        <button
          className="market-sidebar-add-btn"
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
      <div className="market-sidebar-list">
        {currentLoading
          ? renderSkeletons()
          : currentRows.length === 0
            ? (
              <div className="market-sidebar-empty">
                <div className="market-sidebar-empty-text">
                  {isWatchlist
                    ? 'No stocks in your watchlist yet. Click + to add one.'
                    : 'No holdings in your portfolio yet. Click + to add one.'}
                </div>
              </div>
            )
            : isWatchlist
              ? renderRows(currentRows, 'watchlist_item_id', 'changePercent', watchlist.handleDelete)
              : renderRows(currentRows, 'user_portfolio_id', 'unrealizedPlPercent', handlePortfolioDelete)}
      </div>

      {/* Footer */}
      {currentRows.length > 0 && !currentLoading && (
        <div className="market-sidebar-footer">
          <button
            className="market-sidebar-footer-link"
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

export default MarketSidebarPanel;
