import React, { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Plus, ArrowUpRight, ArrowDownRight, Trash2, Pencil, Eye, EyeOff, Sunrise, Sunset, MoreVertical } from 'lucide-react';
import { motion, AnimatePresence } from 'framer-motion';
import { useNavigate } from 'react-router-dom';
import { getExtendedHoursInfo } from '@/lib/marketUtils';
import { useIsMobile } from '@/hooks/useIsMobile';
import { createFormatter } from '@/lib/format';
import { ContextMenu, ContextMenuTrigger, ContextMenuContent, ContextMenuItem } from '@/components/ui/context-menu';
import { DropdownMenu, DropdownMenuTrigger, DropdownMenuContent, DropdownMenuItem } from '@/components/ui/dropdown-menu';
import {
  formatPortfolioMoney,
  normalizePortfolioCurrency,
  summarizePortfolioByCurrency,
} from '../utils/portfolioSummary';

const fmt2 = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmt1 = createFormatter({ minimumFractionDigits: 1, maximumFractionDigits: 1 });
const fmtInt = createFormatter({ maximumFractionDigits: 0 });

interface WatchlistRow {
  watchlist_item_id?: string | number;
  symbol: string;
  price: number;
  change: number;
  changePercent: number;
  isPositive: boolean;
  quoteAvailable?: boolean;
  previousClose?: number | null;
  earlyTradingChangePercent?: number | null;
  lateTradingChangePercent?: number | null;
  [key: string]: unknown;
}

interface PortfolioRow {
  user_portfolio_id?: string | number;
  symbol: string;
  price: number;
  quantity?: number | null;
  average_cost?: number | null;
  currency: string;
  marketValue?: number | null;
  unrealizedPlPercent?: number | null;
  isPositive?: boolean;
  quoteAvailable?: boolean;
  previousClose?: number | null;
  earlyTradingChangePercent?: number | null;
  lateTradingChangePercent?: number | null;
  [key: string]: unknown;
}

// TODO: type properly once marketUtils exports this
type MarketStatusData = Parameters<typeof getExtendedHoursInfo>[0];

interface WatchlistItemProps {
  item: WatchlistRow;
  index: number;
  onDelete?: (id: string) => void;
  marketStatus: MarketStatusData;
  isMobile: boolean;
}

function WatchlistItem({ item, index, onDelete, marketStatus, isMobile }: WatchlistItemProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const hasQuote = item.quoteAvailable !== false;
  const pos = hasQuote ? item.isPositive ?? true : true;
  const pctStr = hasQuote ? (pos ? '+' : '') + fmt2(Number(item.changePercent)) + '%' : 'N/A';
  const hasId = !!item.watchlist_item_id;

  // Extended hours: show when not regular session and data available
  const { extPct, extType, extPrice: _extPrice, extChange: _extChange } = getExtendedHoursInfo(marketStatus, item, { shortLabels: true });
  const extColor = extType === 'pre' ? '#fbbf24' : '#3b82f6';

  const rowContent = (
    <motion.div
      data-testid={item.symbol ? `watchlist-row-${item.symbol}` : undefined}
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05 }}
      className="flex items-center justify-between p-3 rounded-xl border border-transparent transition-all cursor-pointer"
      style={{ backgroundColor: 'transparent' }}
      onClick={() => navigate(`/market?symbol=${encodeURIComponent(item.symbol)}`)}
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
        e.currentTarget.style.borderColor = 'var(--color-border-muted)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
        e.currentTarget.style.borderColor = 'transparent';
      }}
    >
      <div>
        <div className="font-bold text-sm" style={{ color: 'var(--color-text-primary)' }}>
          {item.symbol}
        </div>
        <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>{t('dashboard.portfolioWatchlistCard.stock')}</div>
      </div>

      <div className="flex items-center gap-4">
        <div className="text-right">
          <div className="text-sm font-medium dashboard-mono" style={{ color: 'var(--color-text-primary)' }}>
            {hasQuote
              ? fmt2(Number(extType && item.previousClose != null ? item.previousClose : item.price))
              : 'N/A'}
          </div>
          <div
            className="text-xs font-medium dashboard-mono"
            style={{
              color: hasQuote
                ? pos ? 'var(--color-profit)' : 'var(--color-loss)'
                : 'var(--color-text-secondary)',
            }}
          >
            {hasQuote ? (pos ? '+' : '') + fmt2(Number(item.change)) : 'N/A'}
          </div>
        </div>

        <div className="text-right">
          <div
            className="w-16 py-1 rounded-lg text-center text-xs font-bold"
            style={{
              backgroundColor: hasQuote
                ? pos ? 'var(--color-profit-soft)' : 'var(--color-loss-soft)'
                : 'var(--color-bg-subtle)',
              color: hasQuote
                ? pos ? 'var(--color-profit)' : 'var(--color-loss)'
                : 'var(--color-text-secondary)',
            }}
          >
            {pctStr}
          </div>
          {hasQuote && extType && extPct != null && (
            <div className="text-[10px] mt-0.5 text-center flex items-center justify-center gap-0.5" style={{ color: extColor }}>
              {extType === 'pre' ? <Sunrise size={10} /> : <Sunset size={10} />}
              {fmt2(Number(item.price))} {extPct >= 0 ? '+' : ''}{fmt2(extPct)}%
            </div>
          )}
        </div>

        {/* Mobile: visible menu button */}
        {isMobile && hasId && (
          <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
              <button
                className="p-1 -mr-1 rounded-md transition-colors"
                style={{ color: 'var(--color-text-tertiary)' }}
                onClick={(e) => e.stopPropagation()}
              >
                <MoreVertical size={16} />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem variant="destructive" onSelect={() => onDelete?.(String(item.watchlist_item_id))}>
                <Trash2 className="h-3.5 w-3.5" />
                {t('dashboard.portfolioWatchlistCard.delete')}
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>
    </motion.div>
  );

  // Desktop: wrap with right-click context menu
  if (!isMobile && hasId) {
    return (
      <ContextMenu>
        <ContextMenuTrigger asChild>{rowContent}</ContextMenuTrigger>
        <ContextMenuContent>
          <ContextMenuItem variant="destructive" onSelect={() => onDelete?.(String(item.watchlist_item_id))}>
            <Trash2 className="h-3.5 w-3.5" />
            {t('dashboard.portfolioWatchlistCard.delete')}
          </ContextMenuItem>
        </ContextMenuContent>
      </ContextMenu>
    );
  }

  return rowContent;
}

interface PortfolioItemProps {
  item: PortfolioRow;
  index: number;
  onEdit?: (item: PortfolioRow) => void;
  onDelete?: (id: string) => void;
  valuesHidden: boolean;
  marketStatus: MarketStatusData;
  isMobile: boolean;
}

function PortfolioItem({ item, index, onEdit, onDelete, valuesHidden, marketStatus, isMobile }: PortfolioItemProps) {
  const { t, i18n } = useTranslation();
  const navigate = useNavigate();
  const hasQuote = item.quoteAvailable !== false;
  const pos = hasQuote ? item.isPositive ?? true : true;
  const currency = normalizePortfolioCurrency(item.currency);
  const plStr =
    hasQuote && item.unrealizedPlPercent != null
      ? (pos ? '+' : '') + fmt2(Number(item.unrealizedPlPercent)) + '%'
      : 'N/A';
  const hasId = !!item.user_portfolio_id;

  // Extended hours
  const { extPct, extType, extPrice: _extPrice2 } = getExtendedHoursInfo(marketStatus, item, { shortLabels: true });
  const extColor = extType === 'pre' ? '#fbbf24' : '#3b82f6';
  const displayMarketValue =
    hasQuote && item.marketValue != null
      ? formatPortfolioMoney(item.marketValue, currency, i18n.language)
      : 'N/A';
  const displayPrice =
    hasQuote
      ? formatPortfolioMoney(
          Number(extType && item.previousClose != null ? item.previousClose : item.price),
          currency,
          i18n.language,
        )
      : 'N/A';

  const rowContent = (
    <motion.div
      data-testid={item.symbol ? `portfolio-row-${item.symbol}` : undefined}
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: index * 0.05 }}
      className="flex items-center justify-between p-3 rounded-xl border border-transparent transition-all cursor-pointer"
      style={{ backgroundColor: 'transparent' }}
      onClick={() => navigate(`/market?symbol=${encodeURIComponent(item.symbol)}`)}
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
        e.currentTarget.style.borderColor = 'var(--color-border-muted)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
        e.currentTarget.style.borderColor = 'transparent';
      }}
    >
      <div>
        <div className="font-bold text-sm" style={{ color: 'var(--color-text-primary)' }}>
          {item.symbol}
        </div>
        <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {valuesHidden
            ? t('dashboard.portfolioWatchlistCard.sharesHidden')
            : item.quantity != null
              ? t('dashboard.portfolioWatchlistCard.shares', { qty: fmtInt(Number(item.quantity)) })
              : ''}
        </div>
      </div>

      <div className="flex items-center gap-4">
        <div className="text-right">
          <div className="text-sm font-medium dashboard-mono" style={{ color: 'var(--color-text-primary)' }}>
            {valuesHidden
              ? '******'
              : displayMarketValue}
          </div>
          <div className="text-xs dashboard-mono" style={{ color: 'var(--color-text-secondary)' }}>
            {valuesHidden
              ? '***'
              : displayPrice}
          </div>
        </div>

        <div className="text-right">
          <div
            className="w-16 py-1 rounded-lg text-center text-xs font-bold"
            style={{
              backgroundColor: hasQuote
                ? pos ? 'var(--color-profit-soft)' : 'var(--color-loss-soft)'
                : 'var(--color-bg-subtle)',
              color: hasQuote
                ? pos ? 'var(--color-profit)' : 'var(--color-loss)'
                : 'var(--color-text-secondary)',
            }}
          >
            {plStr}
          </div>
          {hasQuote && extType && extPct != null && (
            <div className="text-[10px] mt-0.5 text-center flex items-center justify-center gap-0.5" style={{ color: extColor }}>
              {extType === 'pre' ? <Sunrise size={10} /> : <Sunset size={10} />}
              {formatPortfolioMoney(item.price, currency, i18n.language)} {extPct >= 0 ? '+' : ''}{fmt2(extPct)}%
            </div>
          )}
        </div>

        {/* Mobile: visible menu button */}
        {isMobile && hasId && (
          <DropdownMenu modal={false}>
            <DropdownMenuTrigger asChild>
              <button
                className="p-1 -mr-1 rounded-md transition-colors"
                style={{ color: 'var(--color-text-tertiary)' }}
                onClick={(e) => e.stopPropagation()}
              >
                <MoreVertical size={16} />
              </button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="end">
              <DropdownMenuItem onSelect={() => onEdit?.(item)}>
                <Pencil className="h-3.5 w-3.5" />
                {t('dashboard.portfolioWatchlistCard.edit')}
              </DropdownMenuItem>
              <DropdownMenuItem variant="destructive" onSelect={() => onDelete?.(String(item.user_portfolio_id))}>
                <Trash2 className="h-3.5 w-3.5" />
                {t('dashboard.portfolioWatchlistCard.delete')}
              </DropdownMenuItem>
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>
    </motion.div>
  );

  // Desktop: wrap with right-click context menu
  if (!isMobile && hasId) {
    return (
      <ContextMenu>
        <ContextMenuTrigger asChild>{rowContent}</ContextMenuTrigger>
        <ContextMenuContent>
          <ContextMenuItem onSelect={() => onEdit?.(item)}>
            <Pencil className="h-3.5 w-3.5" />
            {t('dashboard.portfolioWatchlistCard.edit')}
          </ContextMenuItem>
          <ContextMenuItem variant="destructive" onSelect={() => onDelete?.(String(item.user_portfolio_id))}>
            <Trash2 className="h-3.5 w-3.5" />
            {t('dashboard.portfolioWatchlistCard.delete')}
          </ContextMenuItem>
        </ContextMenuContent>
      </ContextMenu>
    );
  }

  return rowContent;
}

interface AddNewButtonProps {
  label: string;
  onClick?: () => void;
}

function AddNewButton({ label, onClick }: AddNewButtonProps) {
  return (
    <button
      onClick={onClick}
      className="flex items-center justify-center gap-2 w-full py-3 mt-2 rounded-xl border border-dashed text-sm font-medium transition-all"
      style={{
        borderColor: 'var(--color-border-default)',
        color: 'var(--color-text-secondary)',
      }}
      onMouseEnter={(e) => {
        e.currentTarget.style.borderColor = 'var(--color-border-elevated)';
        e.currentTarget.style.color = 'var(--color-text-primary)';
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.borderColor = 'var(--color-border-default)';
        e.currentTarget.style.color = 'var(--color-text-secondary)';
        e.currentTarget.style.backgroundColor = '';
      }}
    >
      <Plus size={16} /> {label}
    </button>
  );
}

type PWTabKey = 'watchlist' | 'portfolio';

interface PortfolioWatchlistCardProps {
  watchlistRows?: WatchlistRow[];
  watchlistLoading?: boolean;
  onWatchlistAdd?: () => void;
  onWatchlistDelete?: (id: string) => void;
  portfolioRows?: PortfolioRow[];
  portfolioLoading?: boolean;
  hasRealHoldings?: boolean;
  onPortfolioAdd?: () => void;
  onPortfolioDelete?: (id: string) => void;
  onPortfolioEdit?: (item: PortfolioRow) => void;
  marketStatus: MarketStatusData;
}

function PortfolioWatchlistCard({
  watchlistRows = [],
  watchlistLoading = false,
  onWatchlistAdd,
  onWatchlistDelete,
  portfolioRows = [],
  portfolioLoading = false,
  hasRealHoldings = false,
  onPortfolioAdd,
  onPortfolioDelete,
  onPortfolioEdit,
  marketStatus,
}: PortfolioWatchlistCardProps) {
  const { t, i18n } = useTranslation();
  const isMobile = useIsMobile();
  const [activeTab, setActiveTabRaw] = useState<PWTabKey>(() => (localStorage.getItem('portfolio_active_tab') as PWTabKey) || 'watchlist');
  const [valuesHidden, setValuesHiddenRaw] = useState(() => localStorage.getItem('portfolio_values_hidden') === 'true');

  const setActiveTab = (tab: PWTabKey) => {
    setActiveTabRaw(tab);
    localStorage.setItem('portfolio_active_tab', tab);
  };
  const setValuesHidden = (updater: boolean | ((prev: boolean) => boolean)) => {
    setValuesHiddenRaw((prev) => {
      const next = typeof updater === 'function' ? updater(prev) : updater;
      localStorage.setItem('portfolio_values_hidden', String(next));
      return next;
    });
  };

  const portfolioSummaries = React.useMemo(
    () => summarizePortfolioByCurrency(portfolioRows),
    [portfolioRows],
  );
  const visibleSummaries = React.useMemo(
    () => portfolioSummaries.filter((summary) => summary.totalValue !== 0),
    [portfolioSummaries],
  );
  const visiblePlSummaries = React.useMemo(
    () => visibleSummaries.filter((summary) => summary.totalCost > 0),
    [visibleSummaries],
  );

  return (
    <div
      className="dashboard-glass-card p-6 flex flex-col"
      style={{ minHeight: '200px', maxHeight: 'clamp(300px, calc(100vh - 420px), 800px)' }}
    >
      {/* Header with tab switcher */}
      <div className="flex items-center justify-between mb-6">
        <h2 className="text-xl font-bold" style={{ color: 'var(--color-text-primary)' }}>
          {activeTab === 'watchlist'
            ? t('dashboard.portfolioWatchlistCard.headerWatchlist')
            : t('dashboard.portfolioWatchlistCard.headerPortfolio')}
        </h2>
        <div className="flex rounded-xl p-1" style={{ backgroundColor: 'var(--color-bg-tag)' }}>
          <button
            onClick={() => setActiveTab('watchlist')}
            className="px-3 py-1 text-xs font-medium rounded-lg transition-all"
            style={{
              backgroundColor: activeTab === 'watchlist' ? 'var(--color-bg-elevated)' : 'transparent',
              color: activeTab === 'watchlist' ? 'var(--color-text-primary)' : 'var(--color-text-secondary)',
            }}
          >
            {t('dashboard.portfolioWatchlistCard.tabWatch')}
          </button>
          <button
            onClick={() => setActiveTab('portfolio')}
            className="px-3 py-1 text-xs font-medium rounded-lg transition-all"
            style={{
              backgroundColor: activeTab === 'portfolio' ? 'var(--color-bg-elevated)' : 'transparent',
              color: activeTab === 'portfolio' ? 'var(--color-text-primary)' : 'var(--color-text-secondary)',
            }}
          >
            {t('dashboard.portfolioWatchlistCard.tabHoldings')}
          </button>
        </div>
      </div>

      {/* Content */}
      <div className="flex-1 min-h-0 overflow-y-auto pr-1 space-y-1">
        <AnimatePresence mode="wait">
          {activeTab === 'watchlist' ? (
            <motion.div
              key="watchlist"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="flex flex-col gap-1"
            >
              {watchlistLoading
                ? Array.from({ length: 5 }).map((_, i) => (
                    <div key={i} className="flex items-center gap-3 p-3 animate-pulse">
                      <div className="flex-1">
                        <div
                          className="h-4 rounded mb-1"
                          style={{ backgroundColor: 'var(--color-border-default)', width: '40%' }}
                        />
                        <div
                          className="h-3 rounded"
                          style={{ backgroundColor: 'var(--color-border-default)', width: '25%' }}
                        />
                      </div>
                    </div>
                  ))
                : watchlistRows.map((item, i) => (
                    <WatchlistItem
                      key={item.watchlist_item_id ?? item.symbol}
                      item={item}
                      index={i}
                      onDelete={onWatchlistDelete}
                      marketStatus={marketStatus}
                      isMobile={isMobile}
                    />
                  ))}
              <AddNewButton label={t('dashboard.portfolioWatchlistCard.addSymbol')} onClick={onWatchlistAdd} />
            </motion.div>
          ) : (
            <motion.div
              key="portfolio"
              initial={{ opacity: 0, y: 10 }}
              animate={{ opacity: 1, y: 0 }}
              exit={{ opacity: 0, y: -10 }}
              className="flex flex-col gap-1"
            >
              {/* Summary card */}
              {hasRealHoldings && (
                <div
                  className="p-4 rounded-2xl border mb-4"
                  style={{
                    background: `linear-gradient(135deg, var(--color-accent-soft) 0%, var(--color-bg-card) 100%)`,
                    borderColor: 'var(--color-accent-overlay)',
                  }}
                >
                  <div className="flex items-center justify-between mb-1">
                    <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                      {visibleSummaries.length > 1
                        ? t('dashboard.portfolioWatchlistCard.netAssetValueByCurrency')
                        : t('dashboard.portfolioWatchlistCard.netAssetValue')}
                    </div>
                    <button
                      onClick={() => setValuesHidden((h) => !h)}
                      className="p-1 rounded-md transition-colors"
                      style={{ color: 'var(--color-text-secondary)' }}
                      onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)'; }}
                      onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = 'transparent'; }}
                    >
                      {valuesHidden ? <EyeOff size={14} /> : <Eye size={14} />}
                    </button>
                  </div>
                  <div
                    className={`${visibleSummaries.length > 1 ? 'text-xl' : 'text-2xl'} font-bold mb-2 dashboard-mono`}
                    style={{ color: 'var(--color-text-primary)' }}
                  >
                    {valuesHidden
                      ? '********'
                      : visibleSummaries.length > 0
                        ? visibleSummaries.map((summary) => (
                            <div key={summary.currency}>
                              {formatPortfolioMoney(summary.totalValue, summary.currency, i18n.language)}
                            </div>
                          ))
                        : '--'}
                  </div>
                  {!valuesHidden && visiblePlSummaries.length > 0 && (
                    <div className="flex flex-wrap gap-2">
                      {visiblePlSummaries.map((summary) => (
                        <div
                          key={summary.currency}
                          className="flex items-center gap-2 text-xs font-medium w-fit px-2 py-1 rounded-full"
                          style={{
                            backgroundColor: summary.isPlPositive
                              ? 'var(--color-profit-soft)'
                              : 'var(--color-loss-soft)',
                            color: summary.isPlPositive ? 'var(--color-profit)' : 'var(--color-loss)',
                          }}
                        >
                          {summary.isPlPositive ? <ArrowUpRight size={12} /> : <ArrowDownRight size={12} />}
                          {summary.isPlPositive ? '+' : '-'}
                          {formatPortfolioMoney(Math.abs(summary.totalPl), summary.currency, i18n.language)} ({fmt1(Math.abs(summary.totalPlPct))}%)
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}

              {portfolioLoading
                ? Array.from({ length: 3 }).map((_, i) => (
                    <div key={i} className="flex items-center gap-3 p-3 animate-pulse">
                      <div className="flex-1">
                        <div
                          className="h-4 rounded mb-1"
                          style={{ backgroundColor: 'var(--color-border-default)', width: '40%' }}
                        />
                        <div
                          className="h-3 rounded"
                          style={{ backgroundColor: 'var(--color-border-default)', width: '25%' }}
                        />
                      </div>
                    </div>
                  ))
                : portfolioRows.map((item, i) => (
                    <PortfolioItem
                      key={item.user_portfolio_id ?? item.symbol}
                      item={item}
                      index={i}
                      onEdit={onPortfolioEdit}
                      onDelete={onPortfolioDelete}
                      valuesHidden={valuesHidden}
                      marketStatus={marketStatus}
                      isMobile={isMobile}
                    />
                  ))}
              <AddNewButton label={t('dashboard.portfolioWatchlistCard.addTransaction')} onClick={onPortfolioAdd} />
            </motion.div>
          )}
        </AnimatePresence>
      </div>
    </div>
  );
}

export default PortfolioWatchlistCard;
