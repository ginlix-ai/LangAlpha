import { motion } from 'framer-motion';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
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
import { createFormatter } from '@/lib/format';
import {
  ContextMenu,
  ContextMenuTrigger,
  ContextMenuContent,
  ContextMenuItem,
} from '@/components/ui/context-menu';
import type { WatchlistRow } from '../../hooks/useWatchlistData';
import type { PortfolioRow } from '../../hooks/usePortfolioData';

type MarketStatusData = Parameters<typeof getExtendedHoursInfo>[0];

const fmt2 = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });
const fmt1 = createFormatter({ minimumFractionDigits: 1, maximumFractionDigits: 1 });
const fmtInt = createFormatter({ maximumFractionDigits: 0 });

interface WatchlistRowItemProps {
  item: WatchlistRow;
  index: number;
  marketStatus: MarketStatusData;
  onDelete?: (id: string) => void;
}

export function WatchlistRowItem({ item, index, marketStatus, onDelete }: WatchlistRowItemProps) {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const pos = item.isPositive;
  const pctStr = (pos ? '+' : '') + fmt2(Number(item.changePercent)) + '%';
  const hasId = !!item.watchlist_item_id;

  const { extPct, extType } = getExtendedHoursInfo(marketStatus, item, { shortLabels: true });
  const extColor = extType === 'pre' ? '#fbbf24' : '#3b82f6';

  const row = (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: Math.min(index, 8) * 0.05 }}
      className="flex items-center justify-between p-3 rounded-xl border border-transparent transition-all cursor-pointer"
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
          {t('dashboard.widgets.holdings.stock')}
        </div>
      </div>
      <div className="flex items-center gap-4">
        <div className="text-right">
          <div
            className="text-sm font-medium dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {fmt2(Number(extType && item.previousClose != null ? item.previousClose : item.price))}
          </div>
          <div
            className="text-xs font-medium dashboard-mono"
            style={{ color: pos ? 'var(--color-profit)' : 'var(--color-loss)' }}
          >
            {(pos ? '+' : '') + fmt2(Number(item.change))}
          </div>
        </div>
        <div className="text-right">
          <div
            className="w-16 py-1 rounded-lg text-center text-xs font-bold"
            style={{
              backgroundColor: pos ? 'var(--color-profit-soft)' : 'var(--color-loss-soft)',
              color: pos ? 'var(--color-profit)' : 'var(--color-loss)',
            }}
          >
            {pctStr}
          </div>
          {extType && extPct != null && (
            <div
              className="text-[10px] mt-0.5 text-center flex items-center justify-center gap-0.5"
              style={{ color: extColor }}
            >
              {extType === 'pre' ? <Sunrise size={10} /> : <Sunset size={10} />}
              {fmt2(Number(item.price))} {extPct >= 0 ? '+' : ''}
              {fmt2(extPct)}%
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
  const navigate = useNavigate();
  const pos = item.isPositive;
  const plStr =
    item.unrealizedPlPercent != null
      ? (pos ? '+' : '') + fmt2(Number(item.unrealizedPlPercent)) + '%'
      : '—';
  const hasId = !!item.user_portfolio_id;

  const { extPct, extType } = getExtendedHoursInfo(marketStatus, item, { shortLabels: true });
  const extColor = extType === 'pre' ? '#fbbf24' : '#3b82f6';

  const row = (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: Math.min(index, 8) * 0.05 }}
      className="flex items-center justify-between p-3 rounded-xl border border-transparent transition-all cursor-pointer"
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
            ? t('dashboard.widgets.holdings.sharesHidden')
            : item.quantity != null
              ? t('dashboard.widgets.holdings.shares', { n: fmtInt(Number(item.quantity)) })
              : ''}
        </div>
      </div>
      <div className="flex items-center gap-4">
        <div className="text-right">
          <div
            className="text-sm font-medium dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {valuesHidden
              ? '******'
              : `$${fmt2(Number(item.marketValue || 0))}`}
          </div>
          <div className="text-xs dashboard-mono" style={{ color: 'var(--color-text-secondary)' }}>
            {valuesHidden
              ? '***'
              : `@${fmt2(Number(extType && item.previousClose != null ? item.previousClose : item.price))}`}
          </div>
        </div>
        <div className="text-right">
          <div
            className="w-16 py-1 rounded-lg text-center text-xs font-bold"
            style={{
              backgroundColor: pos ? 'var(--color-profit-soft)' : 'var(--color-loss-soft)',
              color: pos ? 'var(--color-profit)' : 'var(--color-loss)',
            }}
          >
            {plStr}
          </div>
          {extType && extPct != null && (
            <div
              className="text-[10px] mt-0.5 text-center flex items-center justify-center gap-0.5"
              style={{ color: extColor }}
            >
              {extType === 'pre' ? <Sunrise size={10} /> : <Sunset size={10} />}
              {fmt2(Number(item.price))} {extPct >= 0 ? '+' : ''}
              {fmt2(extPct)}%
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
  const totalValue = rows.reduce((sum, r) => sum + (r.marketValue || 0), 0);
  const totalCost = rows.reduce(
    (sum, r) => sum + (r.average_cost != null ? r.average_cost * (r.quantity || 0) : 0),
    0
  );
  const totalPl = totalCost > 0 ? totalValue - totalCost : 0;
  const totalPlPct = totalCost > 0 ? ((totalValue - totalCost) / totalCost) * 100 : 0;
  const isPlPositive = totalPl >= 0;

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
          {t('dashboard.widgets.holdings.nav')}
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
        className="text-2xl font-bold mb-2 dashboard-mono"
        style={{ color: 'var(--color-text-primary)' }}
      >
        {valuesHidden ? '********' : `$${fmt2(totalValue)}`}
      </div>
      {!valuesHidden && totalCost > 0 && (
        <div
          className="flex items-center gap-2 text-xs font-medium w-fit px-2 py-1 rounded-full"
          style={{
            backgroundColor: isPlPositive ? 'var(--color-profit-soft)' : 'var(--color-loss-soft)',
            color: isPlPositive ? 'var(--color-profit)' : 'var(--color-loss)',
          }}
        >
          {isPlPositive ? <ArrowUpRight size={12} /> : <ArrowDownRight size={12} />}
          {isPlPositive ? '+' : '-'}${fmt2(Math.abs(totalPl))} ({fmt1(totalPlPct)}%)
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
