import { useMemo, type ReactNode } from 'react';
import { motion } from '@/lib/framer';
import { SkeletonBar } from '@/components/ui/skeleton';
import { useLastShape } from '@/lib/lastShape';
import { TWEEN } from '@/lib/motion';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router';
import { Pencil, Trash2, Eye, EyeOff } from 'lucide-react';
import { getExtendedHoursInfo } from '@/lib/marketUtils';
import { integer } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { formatMoney, quoteCurrency } from '@/lib/bars';
import {
  ContextMenu,
  ContextMenuTrigger,
  ContextMenuContent,
  ContextMenuItem,
} from '@/components/ui/context-menu';
import { TermChange, TermKV } from '@/pages/ChatAgent/components/charts/TermCard';
import type { WatchlistRow } from '../../hooks/useWatchlistData';
import type { PortfolioRow } from '../../hooks/usePortfolioData';
import { portfolioSummary } from './_holdingsHelpers';
import {
  formatPortfolioMoney,
  normalizePortfolioCurrency,
} from '../../utils/portfolioSummary';

type MarketStatusData = Parameters<typeof getExtendedHoursInfo>[0];

/** The pre- or after-hours move under a row's change: `AH 199.10 ▲ 0.34%`. */
function ExtendedHours({
  label,
  price,
  pct,
}: {
  label: string;
  price: string;
  pct: number;
}) {
  return (
    <span className="dash-ext">
      <span className="term-label">{label}</span>
      {price}
      <TermChange value={pct} />
    </span>
  );
}

/** The watchlist's table: a row per symbol, its last price and the day's move. */
export function WatchlistTable({ children }: { children: ReactNode }) {
  const { t } = useTranslation();
  return (
    <table className="term-table dash-table">
      <thead>
        <tr>
          <th>{t('dashboard.widgets.holdings.colSymbol')}</th>
          <th className="term-num">{t('dashboard.widgets.holdings.colLast')}</th>
          <th className="term-num">{t('dashboard.widgets.holdings.colChange')}</th>
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  );
}

/** The holdings' table: what is held, its worth, and its gain since bought. */
export function PortfolioTable({ children }: { children: ReactNode }) {
  const { t } = useTranslation();
  return (
    <table className="term-table dash-table">
      <thead>
        <tr>
          <th>{t('dashboard.widgets.holdings.colSymbol')}</th>
          <th>{t('dashboard.widgets.holdings.colShares')}</th>
          <th className="term-num">{t('dashboard.widgets.holdings.colValue')}</th>
          <th className="term-num">{t('dashboard.widgets.holdings.colPl')}</th>
        </tr>
      </thead>
      <tbody>{children}</tbody>
    </table>
  );
}

interface WatchlistRowItemProps {
  item: WatchlistRow;
  marketStatus: MarketStatusData;
  onDelete?: (id: string) => void;
  /** The row's paperclip, set at the end of the symbol's cell. */
  attach?: ReactNode;
}

export function WatchlistRowItem({ item, marketStatus, onDelete, attach }: WatchlistRowItemProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const navigate = useNavigate();
  const hasQuote = item.quoteAvailable !== false;
  const hasId = !!item.watchlist_item_id;
  // A watchlist mixes venues: prefer the currency the snapshot was quoted in,
  // with the row's own listing suffix as the fallback for older payloads. An
  // index level prints bare.
  const code = quoteCurrency(item.currency, item.symbol, item.assetClass);

  const { extPct, extType, extLabel } = getExtendedHoursInfo(marketStatus, item, { shortLabels: true });

  const row = (
    <motion.tr
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={TWEEN.quick}
      className="row-attach-host"
      data-clickable=""
      onClick={() => navigate(`/market?symbol=${encodeURIComponent(item.symbol)}`)}
    >
      <td className="term-fill term-code term-strong dash-attach-cell">
        <span title={item.symbol}>{item.symbol}</span>
        {attach}
      </td>
      <td className="term-num">
        {hasQuote
          ? formatMoney(Number(extType && item.previousClose != null ? item.previousClose : item.price), code, locale)
          : 'N/A'}
      </td>
      <td
        className="term-num"
        title={hasQuote ? formatMoney(Number(item.change), code, locale, { signed: true }) : undefined}
      >
        {hasQuote ? <TermChange value={Number(item.changePercent)} /> : <span className="term-t3">N/A</span>}
        {hasQuote && extType && extLabel && extPct != null && (
          <span className="block">
            <ExtendedHours label={extLabel} price={formatMoney(Number(item.price), code, locale)} pct={extPct} />
          </span>
        )}
      </td>
    </motion.tr>
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
  marketStatus: MarketStatusData;
  valuesHidden: boolean;
  onEdit?: (row: PortfolioRow) => void;
  onDelete?: (id: string) => void;
  /** The row's paperclip, set at the end of the symbol's cell. */
  attach?: ReactNode;
}

export function PortfolioRowItem({
  item,
  marketStatus,
  valuesHidden,
  onEdit,
  onDelete,
  attach,
}: PortfolioRowItemProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const navigate = useNavigate();
  const hasQuote = item.quoteAvailable !== false;
  const currency = normalizePortfolioCurrency(item.currency);
  const hasId = !!item.user_portfolio_id;

  const { extPct, extType, extLabel } = getExtendedHoursInfo(marketStatus, item, { shortLabels: true });
  const displayMarketValue =
    hasQuote && item.marketValue != null
      ? formatPortfolioMoney(item.marketValue, currency, locale)
      : 'N/A';
  // The price the row stands at, kept for the value's tooltip: the regular
  // close while an extended session runs, as the change beside it measures.
  const displayPrice = hasQuote
    ? formatPortfolioMoney(
        Number(extType && item.previousClose != null ? item.previousClose : item.price),
        currency,
        locale,
      )
    : undefined;

  const row = (
    <motion.tr
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={TWEEN.quick}
      className="row-attach-host"
      data-clickable=""
      onClick={() => navigate(`/market?symbol=${encodeURIComponent(item.symbol)}`)}
    >
      <td className="term-code term-strong">
        <span title={item.symbol}>{item.symbol}</span>
      </td>
      <td className="term-fill term-t3 dash-attach-cell">
        {valuesHidden
          ? t('dashboard.widgets.holdings.sharesHidden')
          : item.quantity != null
            ? t('dashboard.widgets.holdings.shares', { n: integer(Number(item.quantity), locale) })
            : ''}
        {attach}
      </td>
      <td className="term-num" title={valuesHidden ? undefined : displayPrice}>
        {valuesHidden ? '******' : displayMarketValue}
      </td>
      <td className="term-num">
        {hasQuote && item.unrealizedPlPercent != null ? (
          <TermChange value={Number(item.unrealizedPlPercent)} />
        ) : (
          <span className="term-t3">N/A</span>
        )}
        {hasQuote && extType && extLabel && extPct != null && (
          <span className="block">
            <ExtendedHours label={extLabel} price={formatPortfolioMoney(item.price, currency, locale)} pct={extPct} />
          </span>
        )}
      </td>
    </motion.tr>
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

/** `+ Add symbol` under the list, in the cards' voice for a list's foot. */
export function HoldingsAddButton({ label, onClick }: HoldingsAddButtonProps) {
  return (
    <button type="button" onClick={onClick} className="term-more widget-drag-cancel" aria-label={label}>
      + {label}
    </button>
  );
}

interface PortfolioNavSummaryProps {
  rows: PortfolioRow[];
  valuesHidden: boolean;
  onToggleHidden: () => void;
}

/** What the holdings are worth, one line per currency, over what they cost
 *  and what they have made. */
export function PortfolioNavSummary({ rows, valuesHidden, onToggleHidden }: PortfolioNavSummaryProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const summaries = useMemo(() => portfolioSummary(rows), [rows]);
  const visibleSummaries = useMemo(
    () => summaries.filter((summary) => summary.totalValue !== 0),
    [summaries],
  );
  const label = visibleSummaries.length > 1 ? t('dashboard.widgets.holdings.navByCurrency') : t('dashboard.widgets.holdings.nav');

  return (
    <div className="mb-2">
      <div className="term-line">
        <span className="term-label term-t3">{label}</span>
        <button
          type="button"
          onClick={onToggleHidden}
          className="widget-card__icon-btn term-push widget-drag-cancel"
          aria-label={valuesHidden ? t('dashboard.widgets.holdings.showValues') : t('dashboard.widgets.holdings.hideValues')}
        >
          {valuesHidden ? <EyeOff size={12} /> : <Eye size={12} />}
        </button>
      </div>
      {valuesHidden ? (
        <div className="term-line">
          <span className="term-big term-hero">********</span>
        </div>
      ) : visibleSummaries.length > 0 ? (
        visibleSummaries.map((summary) => (
          <div key={summary.currency} className="mt-1">
            <div className="term-line">
              <span className="term-big term-hero">{formatPortfolioMoney(summary.totalValue, summary.currency, locale)}</span>
              {summary.totalCost > 0 && <TermChange value={summary.totalPlPct} />}
            </div>
            {summary.totalCost > 0 && (
              <div className="term-figures">
                <TermKV
                  cols={1}
                  pairs={[
                    [t('dashboard.widgets.holdings.cost'), formatPortfolioMoney(summary.totalCost, summary.currency, locale)],
                    [
                      t('dashboard.widgets.holdings.totalPl'),
                      (summary.isPlPositive ? '+' : '-') +
                        formatPortfolioMoney(Math.abs(summary.totalPl), summary.currency, locale),
                    ],
                  ]}
                />
              </div>
            )}
          </div>
        ))
      ) : (
        <div className="term-line">
          <span className="term-big term-hero">--</span>
        </div>
      )}
    </div>
  );
}

const SKELETON_ROWS = { watchlist: 5, portfolio: 3 } as const;
const MAX_SKELETON_ROWS = 10;

/** How many skeleton rows a list should show: as many as it held the last
 *  time it loaded here, so the card neither grows nor shrinks when the rows
 *  land. `loadedCount` is null until the list has arrived, so neither a
 *  skeleton nor a failed read is remembered as the list's size. */
export function useHoldingsSkeletonRows(list: keyof typeof SKELETON_ROWS, loadedCount: number | null): number {
  const last = useLastShape(`dashboard:${list}.rows`, loadedCount);
  return Math.min(last ?? SKELETON_ROWS[list], MAX_SKELETON_ROWS);
}

interface HoldingsSkeletonProps {
  count: number;
  /** The table's columns, so each bar sits where its figure will. */
  cols?: number;
}

/** Rows of the table's own cells with bars in them, so each is as tall as the
 *  row that replaces it. */
export function HoldingsSkeleton({ count, cols = 3 }: HoldingsSkeletonProps) {
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        // The cell holds its saved height until the rows land (see the fit
        // measurement in WidgetFrame).
        <tr key={i} data-fit-hold="">
          {Array.from({ length: cols }).map((__, c) => (
            <td key={c} className={c === 0 ? 'term-fill' : 'term-num'}>
              <SkeletonBar style={{ width: c === 0 ? '3.5em' : '4.5em' }} />
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}
