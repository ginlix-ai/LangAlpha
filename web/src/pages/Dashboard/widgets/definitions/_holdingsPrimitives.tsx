import { useMemo } from 'react';
import { motion } from '@/lib/framer';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import {
  Plus,
  Pencil,
  Trash2,
  Sunrise,
  Sunset,
  ArrowUpRight,
  ArrowDownRight,
  Eye,
  EyeOff,
} from 'lucide-react';
import { getExtendedHoursInfo } from '@/lib/marketUtils';
import { createFormatter, grouped2, integer } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney, quoteCurrency } from '@/lib/bars';
import {
  ContextMenu,
  ContextMenuTrigger,
  ContextMenuContent,
  ContextMenuItem,
} from '@/components/ui/context-menu';
import type { WatchlistRow } from '../../hooks/useWatchlistData';
import type { PortfolioRow } from '../../hooks/usePortfolioData';
import { portfolioSummary } from './_holdingsHelpers';
import {
  formatPortfolioMoney,
  normalizePortfolioCurrency,
} from '../../utils/portfolioSummary';

type MarketStatusData = Parameters<typeof getExtendedHoursInfo>[0];

const fmt1 = createFormatter({ minimumFractionDigits: 1, maximumFractionDigits: 1 });

interface WatchlistRowItemProps {
  item: WatchlistRow;
  index: number;
  marketStatus: MarketStatusData;
  onDelete?: (id: string) => void;
}

export function WatchlistRowItem({ item, index, marketStatus, onDelete }: WatchlistRowItemProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const navigate = useNavigate();
  const hasQuote = item.quoteAvailable !== false;
  const pos = hasQuote ? item.isPositive ?? true : true;
  const pctStr = hasQuote ? (pos ? '+' : '') + grouped2(Number(item.changePercent), locale) + '%' : 'N/A';
  const hasId = !!item.watchlist_item_id;
  // A watchlist mixes venues: prefer the currency the snapshot was quoted in,
  // with the row's own listing suffix as the fallback for older payloads. An
  // index level prints bare.
  const code = quoteCurrency(item.currency, item.symbol, item.assetClass);

  const { extPct, extType } = getExtendedHoursInfo(marketStatus, item, { shortLabels: true });
  const extColor = extType === 'pre' ? '#fbbf24' : '#3b82f6';

  const row = (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: Math.min(index, 8) * 0.05 }}
      className="flex items-center gap-2 p-3 rounded-xl border border-transparent transition-all cursor-pointer"
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
      {/* The ticker is the row's identity and never truncates at a width a card
          actually takes; the cap only catches a pathological symbol. */}
      <div className="min-w-0 max-w-[50%] flex-none">
        <div className="font-bold text-sm truncate" style={{ color: 'var(--color-text-primary)' }} title={item.symbol}>
          {item.symbol}
        </div>
        <div className="text-xs truncate" style={{ color: 'var(--color-text-secondary)' }}>
          {t('dashboard.widgets.holdings.stock')}
        </div>
      </div>
      {/* A CN¥ price is wide enough that price and badge stop fitting side by
          side in a narrow card. They wrap rather than run past its edge. */}
      <div className="flex flex-1 min-w-0 flex-wrap items-center justify-end gap-x-2 gap-y-1">
        <div className="text-right whitespace-nowrap">
          <div
            className="text-sm font-medium dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {hasQuote
              ? formatMoney(Number(extType && item.previousClose != null ? item.previousClose : item.price), code, locale)
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
            {hasQuote ? formatMoney(Number(item.change), code, locale, { signed: true }) : 'N/A'}
          </div>
        </div>
        <div className="text-right shrink-0">
          <div
            className="min-w-16 px-2 py-1 rounded-lg text-center text-xs font-bold whitespace-nowrap"
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
            <div
              className="text-[0.625rem] mt-0.5 text-center flex items-center justify-center gap-0.5 whitespace-nowrap"
              style={{ color: extColor }}
            >
              {extType === 'pre' ? <Sunrise size={10} /> : <Sunset size={10} />}
              {formatMoney(Number(item.price), code, locale)} {extPct >= 0 ? '+' : ''}
              {grouped2(extPct, locale)}%
            </div>
          )}
        </div>
      </div>
    </motion.div>
  );

  if (hasId) {
    return (
      <ContextMenu>
        <ContextMenuTrigger asChild>{row}</ContextMenuTrigger>
        <ContextMenuContent>
          <ContextMenuItem variant="destructive" onSelect={() => onDelete?.(String(item.watchlist_item_id))}>
            <Trash2 className="h-3.5 w-3.5" />
            {t('dashboard.widgets.holdings.delete')}
          </ContextMenuItem>
        </ContextMenuContent>
      </ContextMenu>
    );
  }
  return row;
}

interface PortfolioRowItemProps {
  item: PortfolioRow;
  index: number;
  marketStatus: MarketStatusData;
  valuesHidden: boolean;
  onEdit?: (row: PortfolioRow) => void;
  onDelete?: (id: string) => void;
}

export function PortfolioRowItem({
  item,
  index,
  marketStatus,
  valuesHidden,
  onEdit,
  onDelete,
}: PortfolioRowItemProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const navigate = useNavigate();
  const hasQuote = item.quoteAvailable !== false;
  const pos = hasQuote ? item.isPositive ?? true : true;
  const currency = normalizePortfolioCurrency(item.currency);
  const plStr =
    hasQuote && item.unrealizedPlPercent != null
      ? (pos ? '+' : '') + grouped2(Number(item.unrealizedPlPercent), locale) + '%'
      : 'N/A';
  const hasId = !!item.user_portfolio_id;

  const { extPct, extType } = getExtendedHoursInfo(marketStatus, item, { shortLabels: true });
  const extColor = extType === 'pre' ? '#fbbf24' : '#3b82f6';
  const displayMarketValue =
    hasQuote && item.marketValue != null
      ? formatPortfolioMoney(item.marketValue, currency, locale)
      : 'N/A';
  const displayPrice =
    hasQuote
      ? formatPortfolioMoney(
          Number(extType && item.previousClose != null ? item.previousClose : item.price),
          currency,
          locale,
        )
      : 'N/A';

  const row = (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: Math.min(index, 8) * 0.05 }}
      className="flex items-center gap-2 p-3 rounded-xl border border-transparent transition-all cursor-pointer"
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
      <div className="min-w-0 max-w-[50%] flex-none">
        <div className="font-bold text-sm truncate" style={{ color: 'var(--color-text-primary)' }} title={item.symbol}>
          {item.symbol}
        </div>
        <div className="text-xs truncate" style={{ color: 'var(--color-text-secondary)' }}>
          {valuesHidden
            ? t('dashboard.widgets.holdings.sharesHidden')
            : item.quantity != null
              ? t('dashboard.widgets.holdings.shares', { n: integer(Number(item.quantity), locale) })
              : ''}
        </div>
      </div>
      <div className="flex flex-1 min-w-0 flex-wrap items-center justify-end gap-x-2 gap-y-1">
        <div className="text-right whitespace-nowrap">
          <div
            className="text-sm font-medium dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
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
        <div className="text-right shrink-0">
          <div
            className="min-w-16 px-2 py-1 rounded-lg text-center text-xs font-bold whitespace-nowrap"
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
            <div
              className="text-[0.625rem] mt-0.5 text-center flex items-center justify-center gap-0.5 whitespace-nowrap"
              style={{ color: extColor }}
            >
              {extType === 'pre' ? <Sunrise size={10} /> : <Sunset size={10} />}
              {formatPortfolioMoney(item.price, currency, locale)} {extPct >= 0 ? '+' : ''}
              {grouped2(extPct, locale)}%
            </div>
          )}
        </div>
      </div>
    </motion.div>
  );

  if (hasId) {
    return (
      <ContextMenu>
        <ContextMenuTrigger asChild>{row}</ContextMenuTrigger>
        <ContextMenuContent>
          <ContextMenuItem onSelect={() => onEdit?.(item)}>
            <Pencil className="h-3.5 w-3.5" />
            {t('dashboard.widgets.holdings.edit')}
          </ContextMenuItem>
          <ContextMenuItem variant="destructive" onSelect={() => onDelete?.(String(item.user_portfolio_id))}>
            <Trash2 className="h-3.5 w-3.5" />
            {t('dashboard.widgets.holdings.delete')}
          </ContextMenuItem>
        </ContextMenuContent>
      </ContextMenu>
    );
  }
  return row;
}

interface HoldingsAddButtonProps {
  label: string;
  onClick?: () => void;
}

export function HoldingsAddButton({ label, onClick }: HoldingsAddButtonProps) {
  return (
    <button
      type="button"
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

interface PortfolioNavSummaryProps {
  rows: PortfolioRow[];
  valuesHidden: boolean;
  onToggleHidden: () => void;
}

export function PortfolioNavSummary({ rows, valuesHidden, onToggleHidden }: PortfolioNavSummaryProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const summaries = useMemo(() => portfolioSummary(rows), [rows]);
  const visibleSummaries = useMemo(
    () => summaries.filter((summary) => summary.totalValue !== 0),
    [summaries],
  );
  const visiblePlSummaries = useMemo(
    () => visibleSummaries.filter((summary) => summary.totalCost > 0),
    [visibleSummaries],
  );

  return (
    <div
      className="p-4 rounded-2xl border mb-4"
      style={{
        background: 'linear-gradient(135deg, var(--color-accent-soft) 0%, var(--color-bg-card) 100%)',
        borderColor: 'var(--color-accent-overlay)',
      }}
    >
      <div className="flex items-center justify-between mb-1">
        <div className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {visibleSummaries.length > 1 ? t('dashboard.widgets.holdings.navByCurrency') : t('dashboard.widgets.holdings.nav')}
        </div>
        <button
          type="button"
          onClick={onToggleHidden}
          className="p-1 rounded-md transition-colors"
          style={{ color: 'var(--color-text-secondary)' }}
          onMouseEnter={(e) => {
            e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
          }}
          onMouseLeave={(e) => {
            e.currentTarget.style.backgroundColor = 'transparent';
          }}
          aria-label={valuesHidden ? t('dashboard.widgets.holdings.showValues') : t('dashboard.widgets.holdings.hideValues')}
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
                  {formatPortfolioMoney(summary.totalValue, summary.currency, locale)}
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
                backgroundColor: summary.isPlPositive ? 'var(--color-profit-soft)' : 'var(--color-loss-soft)',
                color: summary.isPlPositive ? 'var(--color-profit)' : 'var(--color-loss)',
              }}
            >
              {summary.isPlPositive ? <ArrowUpRight size={12} /> : <ArrowDownRight size={12} />}
              {summary.isPlPositive ? '+' : '-'}
              {formatPortfolioMoney(Math.abs(summary.totalPl), summary.currency, locale)} ({fmt1(Math.abs(summary.totalPlPct), locale)}%)
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

interface HoldingsSkeletonProps {
  count: number;
}

export function HoldingsSkeleton({ count }: HoldingsSkeletonProps) {
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
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
      ))}
    </>
  );
}
