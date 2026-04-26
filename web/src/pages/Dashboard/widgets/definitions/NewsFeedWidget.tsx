import { useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion } from 'framer-motion';
import { Newspaper, Clock, Search, X } from 'lucide-react';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { NewsFeedConfigSchema } from '../framework/configSchemas';
import type { WidgetRenderProps } from '../types';

type NewsFeedSource = 'market' | 'portfolio' | 'watchlist';
type NewsFeedConfig = { source?: NewsFeedSource; limit?: number };

type DateRangeKey = 'all' | '1h' | '6h' | '24h' | '7d';

const SOURCE_KEY: Record<NewsFeedSource, string> = {
  market: 'dashboard.widgets.newsFeed.tab_market',
  portfolio: 'dashboard.widgets.newsFeed.tab_portfolio',
  watchlist: 'dashboard.widgets.newsFeed.tab_watchlist',
};

const SOURCES: NewsFeedSource[] = ['market', 'portfolio', 'watchlist'];

const DATE_RANGES: { key: DateRangeKey; labelKey: string }[] = [
  { key: 'all', labelKey: 'dashboard.widgets.newsFeed.range_all' },
  { key: '1h', labelKey: 'dashboard.widgets.newsFeed.range_1h' },
  { key: '6h', labelKey: 'dashboard.widgets.newsFeed.range_6h' },
  { key: '24h', labelKey: 'dashboard.widgets.newsFeed.range_24h' },
  { key: '7d', labelKey: 'dashboard.widgets.newsFeed.range_7d' },
];

interface NewsItem {
  id?: string | number;
  title: string;
  source?: string;
  time?: string;
  image?: string | null;
  favicon?: string | null;
  tickers?: string[];
  isHot?: boolean;
  articleUrl?: string | null;
}

function parseRelativeTime(timeStr: string | undefined | null): number | null {
  if (!timeStr) return null;
  const now = Date.now();
  const m = timeStr.match(/^(\d+)\s*(min|hr|hrs|hour|hours|day|days)/i);
  // Unparseable strings return null (not now). Otherwise items with odd time
  // formats silently bucket into every recent-window filter.
  if (!m) return null;
  const val = parseInt(m[1], 10);
  const unit = m[2].toLowerCase();
  if (unit === 'min') return now - val * 60 * 1000;
  if (unit.startsWith('hr') || unit.startsWith('hour')) return now - val * 3600 * 1000;
  if (unit.startsWith('day')) return now - val * 86400 * 1000;
  return null;
}

function getDateRangeCutoff(key: DateRangeKey): number {
  if (key === 'all') return 0;
  const now = Date.now();
  switch (key) {
    case '1h': return now - 3600 * 1000;
    case '6h': return now - 6 * 3600 * 1000;
    case '24h': return now - 24 * 3600 * 1000;
    case '7d': return now - 7 * 86400 * 1000;
    default: return 0;
  }
}

function NewsRow({
  item,
  idx,
  onClick,
}: {
  item: NewsItem;
  idx: number;
  onClick: () => void;
}) {
  const sentimentColor = item.isHot ? 'var(--color-profit)' : 'var(--color-text-secondary)';
  const tickers = (item.tickers?.length ?? 0) > 0 ? item.tickers! : null;

  return (
    <motion.div
      initial={{ opacity: 0, y: 8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ delay: Math.min(idx, 8) * 0.03 }}
      onClick={onClick}
      className="group flex items-start gap-3 px-2 py-2.5 rounded-md border border-transparent transition-colors cursor-pointer"
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
      }}
    >
      {item.image ? (
        <div className="relative h-12 w-16 flex-shrink-0 overflow-hidden rounded-md">
          <img
            src={item.image}
            alt=""
            className="h-full w-full object-cover transition-transform duration-500 group-hover:scale-110"
            onError={(e) => {
              (e.target as HTMLImageElement).style.display = 'none';
            }}
          />
        </div>
      ) : null}

      <div className="flex-1 min-w-0">
        <div className="flex items-center gap-1.5 mb-0.5">
          <span
            className="w-1.5 h-1.5 rounded-full flex-shrink-0"
            style={{ backgroundColor: sentimentColor }}
          />
          {item.favicon ? (
            <img
              src={item.favicon}
              alt=""
              className="w-3.5 h-3.5 rounded-sm flex-shrink-0"
              onError={(e) => {
                (e.target as HTMLImageElement).style.display = 'none';
              }}
            />
          ) : null}
          {item.source ? (
            <span
              className="text-[9.5px] font-semibold uppercase tracking-wide truncate"
              style={{ color: 'var(--color-accent-light)' }}
            >
              {item.source}
            </span>
          ) : null}
          {item.time ? (
            <span
              className="text-[10px] flex items-center gap-0.5 flex-shrink-0"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <Clock size={9} /> {item.time}
            </span>
          ) : null}
        </div>
        <h3
          className="text-[13px] font-medium leading-snug line-clamp-2"
          style={{ color: 'var(--color-text-primary)' }}
          title={item.title}
        >
          {item.title}
        </h3>
        {tickers ? (
          <div className="flex items-center gap-1 mt-1">
            {tickers.slice(0, 4).map((t) => (
              <span
                key={t}
                className="text-[9.5px] font-bold px-1.5 py-0.5 rounded"
                style={{
                  backgroundColor: 'var(--color-accent-soft)',
                  color: 'var(--color-accent-light)',
                }}
              >
                {t}
              </span>
            ))}
            {tickers.length > 4 ? (
              <span className="text-[9.5px]" style={{ color: 'var(--color-text-tertiary)' }}>
                +{tickers.length - 4}
              </span>
            ) : null}
          </div>
        ) : null}
      </div>
    </motion.div>
  );
}

function NewsFeedWidget({ instance, updateConfig }: WidgetRenderProps<NewsFeedConfig>) {
  const { t } = useTranslation();
  const { dashboard, portfolioNews, watchlistNews, modals } = useDashboardContext();
  const initialSource: NewsFeedSource = instance.config.source ?? 'market';
  const [activeTab, setActiveTab] = useState<NewsFeedSource>(initialSource);
  const [tickerFilter, setTickerFilter] = useState('');
  const [dateRange, setDateRange] = useState<DateRangeKey>('all');

  const sources: Record<NewsFeedSource, { items: NewsItem[]; loading: boolean }> = {
    market: { items: dashboard.newsItems as NewsItem[], loading: dashboard.newsLoading },
    portfolio: { items: portfolioNews.items as NewsItem[], loading: portfolioNews.loading },
    watchlist: { items: watchlistNews.items as NewsItem[], loading: watchlistNews.loading },
  };
  const { items, loading } = sources[activeTab];

  const switchTab = (key: NewsFeedSource) => {
    setActiveTab(key);
    setTickerFilter('');
    setDateRange('all');
    updateConfig({ source: key });
  };

  const hasFilters = tickerFilter.trim() !== '' || dateRange !== 'all';

  const filteredItems = useMemo(() => {
    let result = items;
    const query = tickerFilter.trim().toUpperCase();
    if (query) {
      result = result.filter((item) => item.tickers?.some((t) => t.toUpperCase().includes(query)));
    }
    if (dateRange !== 'all') {
      const cutoff = getDateRangeCutoff(dateRange);
      result = result.filter((item) => {
        const ts = parseRelativeTime(item.time);
        return ts !== null && ts >= cutoff;
      });
    }
    return result;
  }, [items, tickerFilter, dateRange]);

  return (
    <div className="dashboard-glass-card p-5 flex flex-col h-full">
      <div
        className="flex items-baseline justify-between mb-3 pb-3 border-b gap-3"
        style={{ borderColor: 'var(--color-border-muted)' }}
      >
        <div className="flex items-baseline gap-2.5 min-w-0">
          <Newspaper
            className="h-3.5 w-3.5 flex-shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('dashboard.widgets.newsFeed.header', { label: t(SOURCE_KEY[activeTab]) })}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {items.length}
          </span>
        </div>
        <div
          className="flex rounded-full p-[2px] flex-shrink-0"
          style={{ backgroundColor: 'var(--color-bg-subtle)' }}
        >
          {SOURCES.map((key) => {
            const isActive = activeTab === key;
            return (
              <button
                key={key}
                type="button"
                onClick={() => switchTab(key)}
                className="px-2.5 py-[3px] text-[10.5px] uppercase tracking-wider rounded-full transition-colors"
                style={{
                  backgroundColor: isActive ? 'var(--color-bg-card)' : 'transparent',
                  color: isActive ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                  boxShadow: isActive ? '0 1px 2px rgba(0,0,0,0.04)' : 'none',
                }}
              >
                {t(SOURCE_KEY[key])}
              </button>
            );
          })}
        </div>
      </div>

      <div className="flex items-center gap-2 mb-3 flex-wrap">
        <div
          className="flex items-center gap-1.5 h-7 px-2 rounded-md border"
          style={{
            backgroundColor: 'var(--color-bg-subtle)',
            borderColor: 'var(--color-border-muted)',
            width: tickerFilter ? 160 : 130,
            transition: 'width 0.2s',
          }}
        >
          <Search size={12} style={{ color: 'var(--color-text-tertiary)', flexShrink: 0 }} />
          <input
            type="text"
            placeholder={t('dashboard.widgets.newsFeed.tickerPlaceholder')}
            value={tickerFilter}
            onChange={(e) => setTickerFilter(e.target.value)}
            className="flex-1 text-[11px] bg-transparent border-none outline-none min-w-0"
            style={{ color: 'var(--color-text-primary)' }}
          />
          {tickerFilter ? (
            <button
              type="button"
              onClick={() => setTickerFilter('')}
              className="flex-shrink-0"
              style={{ color: 'var(--color-text-tertiary)' }}
              aria-label={t('dashboard.widgets.newsFeed.clearTicker')}
            >
              <X size={11} />
            </button>
          ) : null}
        </div>

        <div
          className="flex items-center gap-0.5 p-0.5 rounded-md"
          style={{ backgroundColor: 'var(--color-bg-subtle)' }}
        >
          {DATE_RANGES.map((dr) => {
            const isActive = dateRange === dr.key;
            return (
              <button
                key={dr.key}
                type="button"
                onClick={() => setDateRange(dr.key)}
                className="px-1.5 py-[3px] rounded text-[10px] uppercase tracking-wider transition-colors"
                style={{
                  backgroundColor: isActive ? 'var(--color-bg-card)' : 'transparent',
                  color: isActive ? 'var(--color-text-primary)' : 'var(--color-text-tertiary)',
                  boxShadow: isActive ? '0 1px 2px rgba(0,0,0,0.04)' : 'none',
                }}
              >
                {t(dr.labelKey)}
              </button>
            );
          })}
        </div>

        {hasFilters ? (
          <button
            type="button"
            onClick={() => {
              setTickerFilter('');
              setDateRange('all');
            }}
            className="text-[10px] uppercase tracking-wider transition-colors"
            style={{ color: 'var(--color-text-tertiary)' }}
            onMouseEnter={(e) => {
              e.currentTarget.style.color = 'var(--color-text-primary)';
            }}
            onMouseLeave={(e) => {
              e.currentTarget.style.color = 'var(--color-text-tertiary)';
            }}
          >
            {t('dashboard.widgets.newsFeed.clear')}
          </button>
        ) : null}
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto -mx-1 px-1">
        <AnimatePresence mode="wait">
          <motion.div
            key={activeTab}
            initial={{ opacity: 0, y: 6 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: -6 }}
            transition={{ duration: 0.15 }}
            className="flex flex-col gap-0.5"
          >
            {loading && filteredItems.length === 0 ? (
              Array.from({ length: 5 }).map((_, i) => (
                <div key={i} className="flex items-start gap-3 p-2 animate-pulse">
                  <div
                    className="h-12 w-16 flex-shrink-0 rounded-md"
                    style={{ backgroundColor: 'var(--color-bg-subtle)' }}
                  />
                  <div className="flex-1">
                    <div
                      className="h-3 rounded mb-1.5"
                      style={{ backgroundColor: 'var(--color-bg-subtle)', width: '35%' }}
                    />
                    <div
                      className="h-3.5 rounded"
                      style={{ backgroundColor: 'var(--color-bg-subtle)', width: `${60 + (i % 3) * 15}%` }}
                    />
                  </div>
                </div>
              ))
            ) : filteredItems.length === 0 ? (
              <div className="h-full flex flex-col items-center justify-center gap-2 py-8">
                <div
                  className="h-9 w-9 rounded-full flex items-center justify-center"
                  style={{ backgroundColor: 'var(--color-bg-subtle)' }}
                >
                  <Newspaper className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
                </div>
                <div
                  className="dashboard-mono text-sm"
                  style={{ color: 'var(--color-text-primary)' }}
                >
                  {hasFilters
                    ? t('dashboard.widgets.newsFeed.emptyFiltered')
                    : activeTab === 'market'
                      ? t('dashboard.widgets.newsFeed.emptyMarket')
                      : t('dashboard.widgets.newsFeed.emptyAddTo', { label: t(SOURCE_KEY[activeTab]).toLowerCase() })}
                </div>
              </div>
            ) : (
              filteredItems.map((item, idx) => (
                <NewsRow
                  key={item.id ?? `${idx}-${item.title}`}
                  item={item}
                  idx={idx}
                  onClick={() => {
                    if (item.id != null) modals.openNews(item.id, item.articleUrl ?? null);
                  }}
                />
              ))
            )}
          </motion.div>
        </AnimatePresence>
      </div>
    </div>
  );
}

registerWidget<NewsFeedConfig>({
  type: 'news.feed',
  titleKey: 'dashboard.widgets.newsFeed.title',
  descriptionKey: 'dashboard.widgets.newsFeed.description',
  category: 'intel',
  icon: Newspaper,
  component: NewsFeedWidget,
  defaultConfig: { source: 'market' },
  configSchema: NewsFeedConfigSchema,
  defaultSize: { w: 8, h: 29 },
  minSize: { w: 4, h: 18 },
});

export default NewsFeedWidget;
