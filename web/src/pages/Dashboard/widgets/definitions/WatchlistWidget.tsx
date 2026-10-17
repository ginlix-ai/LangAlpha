import { useMemo, useState } from 'react';
import { AnimatePresence, motion } from '@/lib/framer';
import { useTranslation } from 'react-i18next';
import { ArrowDown, ArrowUp, Eye } from 'lucide-react';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { WatchlistConfigSchema } from '../framework/configSchemas';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import {
  serializeQuoteRowsToMarkdown,
  serializeQuoteRowToMarkdown,
  wrapWidgetContext,
} from '../framework/snapshotSerializers';
import { RowAttachButton } from '../../components/RowAttachButton';
import type { WidgetRenderProps } from '../types';
import { watchlistQuoteRow } from '../../hooks/useWatchlistData';
import {
  HoldingsAddButton,
  HoldingsSkeleton,
  WatchlistRowItem,
} from './_holdingsPrimitives';

type WatchlistConfig = Record<string, never>;

type SortKey = 'added' | 'symbol' | 'price' | 'change';
type SortDir = 'asc' | 'desc';
interface SortState { key: SortKey; dir: SortDir }

const SORT_KEYS: SortKey[] = ['added', 'symbol', 'price', 'change'];
// Not dotted: the locale-key test treats dotted string literals as i18n keys.
const SORT_STORAGE_KEY = 'langalpha:watchlist-sort';
const DEFAULT_SORT: SortState = { key: 'added', dir: 'asc' };
// Numeric sorts default to biggest-first; names default to A–Z.
const FIRST_DIR: Record<SortKey, SortDir> = { added: 'asc', symbol: 'asc', price: 'desc', change: 'desc' };

function loadSort(): SortState {
  try {
    const parsed = JSON.parse(localStorage.getItem(SORT_STORAGE_KEY) ?? 'null');
    if (parsed && SORT_KEYS.includes(parsed.key) && (parsed.dir === 'asc' || parsed.dir === 'desc')) return parsed;
  } catch { /* storage unavailable or corrupt: fall back to default */ }
  return DEFAULT_SORT;
}

function sortRows<T extends { symbol: string; price: number; changePercent: number; quoteAvailable?: boolean }>(
  rows: T[],
  { key, dir }: SortState,
): T[] {
  if (key === 'added') return rows;
  const sign = dir === 'asc' ? 1 : -1;
  const value = (r: T) => (key === 'price' ? r.price : r.changePercent);
  return [...rows].sort((a, b) => {
    if (key === 'symbol') return sign * a.symbol.localeCompare(b.symbol);
    // Rows without a quote always sink to the bottom, whichever direction.
    const aOk = a.quoteAvailable !== false && Number.isFinite(value(a));
    const bOk = b.quoteAvailable !== false && Number.isFinite(value(b));
    if (aOk !== bOk) return aOk ? -1 : 1;
    return sign * (value(a) - value(b));
  });
}

function WatchlistWidget({ instance }: WidgetRenderProps<WatchlistConfig>) {
  const { t } = useTranslation();
  const { watchlist, watchlistHandlers, dashboard } = useDashboardContext();
  const showSkeleton = watchlist.loading && watchlist.rows.length === 0;
  const [sort, setSort] = useState<SortState>(loadSort);
  const SortArrow = sort.dir === 'asc' ? ArrowUp : ArrowDown;
  const sortedRows = useMemo(() => sortRows(watchlist.rows, sort), [watchlist.rows, sort]);

  const selectSort = (key: SortKey) => {
    const next: SortState =
      key === sort.key && key !== 'added'
        ? { key, dir: sort.dir === 'asc' ? 'desc' : 'asc' }
        : { key, dir: FIRST_DIR[key] };
    setSort(next);
    try { localStorage.setItem(SORT_STORAGE_KEY, JSON.stringify(next)); } catch { /* per-viewer convenience only */ }
  };

  // Register snapshot exporters: full table + per-row.
  useWidgetContextExport(instance.id, {
    full: () => {
      const rows = watchlist.rows.map(watchlistQuoteRow);
      const body = serializeQuoteRowsToMarkdown(rows);
      const text = wrapWidgetContext('watchlist.list', { count: rows.length }, body);
      return {
        widget_type: 'watchlist.list',
        widget_id: instance.id,
        label: t('dashboard.widgets.watchlist.title') + ' · ' + rows.length,
        description: rows.length ? `${rows.length} symbol${rows.length === 1 ? '' : 's'}` : 'empty',
        captured_at: new Date().toISOString(),
        text,
        data: { rows },
      };
    },
    rows: (rowId: string) => {
      const row = watchlist.rows.find((r) => (r.watchlist_item_id ?? r.symbol) === rowId);
      if (!row) return null;
      // Note: pre/post-market price is intentionally omitted. The QuoteRow
      // contract treats preMarket/postMarket as *prices*, but the row data
      // here only carries previousClose + a change-percent — passing
      // previousClose under the preMarket key would label yesterday's close
      // as the pre-market price in the agent's view. Adding correct ext-
      // hours info would need marketStatus + getExtendedHoursInfo here.
      const cleaned = watchlistQuoteRow(row);
      const body = serializeQuoteRowToMarkdown(cleaned);
      const text = wrapWidgetContext('watchlist.list/row', { symbol: row.symbol }, body);
      return {
        widget_type: 'watchlist.list/row',
        widget_id: `${instance.id}/${rowId}`,
        label:
          row.symbol +
          (row.quoteAvailable !== false && row.changePercent !== undefined
            ? ` · ${row.changePercent >= 0 ? '+' : ''}${row.changePercent.toFixed(2)}%`
            : ''),
        description: t('dashboard.widgets.watchlist.title'),
        captured_at: new Date().toISOString(),
        text,
        data: { row: cleaned },
      };
    },
  });

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-3 border-b"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2.5 min-w-0">
          <Eye
            className="h-3.5 w-3.5 shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[0.625rem] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('dashboard.widgets.watchlist.header')}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {watchlist.rows.length}
          </span>
        </div>
      </div>
      <div className="flex items-center gap-1 mb-2" role="group" aria-label={t('dashboard.widgets.watchlist.sort.label')}>
        {SORT_KEYS.map((key) => {
          const active = sort.key === key;
          return (
            <button
              key={key}
              type="button"
              aria-pressed={active}
              onClick={() => selectSort(key)}
              className="flex items-center gap-0.5 rounded px-1.5 py-0.5 text-[0.625rem] font-medium uppercase tracking-wide transition-colors hover:bg-accent/50"
              style={{
                color: active ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                background: active ? 'var(--color-border-muted)' : undefined,
              }}
            >
              {t(`dashboard.widgets.watchlist.sort.${key}`)}
              {active && key !== 'added' && <SortArrow className="h-2.5 w-2.5" />}
            </button>
          );
        })}
      </div>
      <div className="flex-1 min-h-0 overflow-y-auto pr-1">
        <AnimatePresence mode="wait">
          <motion.div
            key="watchlist"
            initial={{ opacity: 0, y: 10 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -10 }}
            className="flex flex-col gap-1"
          >
            {showSkeleton ? (
              <HoldingsSkeleton count={5} />
            ) : (
              sortedRows.map((row, i) => (
                <div
                  key={row.watchlist_item_id ?? row.symbol}
                  className="row-attach-host relative"
                >
                  <WatchlistRowItem
                    item={row}
                    index={i}
                    marketStatus={dashboard.marketStatus}
                    onDelete={watchlistHandlers.onDelete}
                  />
                  <span className="absolute right-1 top-1/2 -translate-y-1/2">
                    <RowAttachButton
                      instanceId={instance.id}
                      rowId={String(row.watchlist_item_id ?? row.symbol)}
                    />
                  </span>
                </div>
              ))
            )}

            <HoldingsAddButton label={t('dashboard.widgets.watchlist.addSymbol')} onClick={watchlistHandlers.onAdd} />
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}

registerWidget<WatchlistConfig>({
  type: 'watchlist.list',
  titleKey: 'dashboard.widgets.watchlist.title',
  descriptionKey: 'dashboard.widgets.watchlist.description',
  category: 'personal',
  icon: Eye,
  component: WatchlistWidget,
  defaultConfig: {},
  configSchema: WatchlistConfigSchema,
  defaultSize: { w: 4, h: 26 },
  minSize: { w: 3, h: 15 },
});

export default WatchlistWidget;
