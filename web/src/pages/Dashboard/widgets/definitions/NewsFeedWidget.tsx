import { useEffect, useMemo, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AnimatePresence, motion } from 'framer-motion';
import { Newspaper, Clock, Search, X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { useDashboardContext } from '../framework/DashboardDataContext';
import { registerWidget } from '../framework/WidgetRegistry';
import { NewsFeedConfigSchema } from '../framework/configSchemas';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import {
  serializeNewsItemsToMarkdown,
  wrapWidgetContext,
  type NewsArticleDetail,
} from '../framework/snapshotSerializers';
import { buildNewsArticleSnapshot } from '../../utils/newsArticleFetch';
import type { NewsSentimentItem } from '../../utils/newsItem';
import { RowAttachButton } from '../../components/RowAttachButton';
import type { WidgetRenderProps } from '../types';

type NewsFeedSource = 'top' | 'market' | 'portfolio' | 'watchlist';
type NewsFeedConfig = { source?: NewsFeedSource; limit?: number };

type DateRangeKey = 'all' | '1h' | '6h' | '24h' | '7d';

const SOURCE_KEY: Record<NewsFeedSource, string> = {
  top: 'dashboard.widgets.newsFeed.tab_top',
  market: 'dashboard.widgets.newsFeed.tab_market',
  portfolio: 'dashboard.widgets.newsFeed.tab_portfolio',
  watchlist: 'dashboard.widgets.newsFeed.tab_watchlist',
};

const SOURCES: NewsFeedSource[] = ['top', 'market', 'portfolio', 'watchlist'];

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
  publishedAt?: string | null;
  image?: string | null;
  favicon?: string | null;
  tickers?: string[];
  isHot?: boolean;
  articleUrl?: string | null;
  author?: string | null;
  description?: string | null;
  keywords?: string[];
  sentiments?: NewsSentimentItem[] | null;
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
              className="text-[0.5938rem] font-semibold uppercase tracking-wide truncate"
              style={{ color: 'var(--color-accent-light)' }}
            >
              {item.source}
            </span>
          ) : null}
          {item.time ? (
            <span
              className="text-[0.625rem] flex items-center gap-0.5 flex-shrink-0"
              style={{ color: 'var(--color-text-tertiary)' }}
            >
              <Clock size={9} /> {item.time}
            </span>
          ) : null}
        </div>
        <h3
          className="text-[0.8125rem] font-medium leading-snug line-clamp-2"
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
                className="text-[0.5938rem] font-bold px-1.5 py-0.5 rounded"
                style={{
                  backgroundColor: 'var(--color-accent-soft)',
                  color: 'var(--color-accent-light)',
                }}
              >
                {t}
              </span>
            ))}
            {tickers.length > 4 ? (
              <span className="text-[0.5938rem]" style={{ color: 'var(--color-text-tertiary)' }}>
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
  const [sourceFilter, setSourceFilter] = useState('all');

  const sources: Record<NewsFeedSource, { items: NewsItem[]; loading: boolean }> = {
    top: { items: dashboard.curatedItems as NewsItem[], loading: dashboard.curatedLoading },
    market: { items: dashboard.newsItems as NewsItem[], loading: dashboard.newsLoading },
    portfolio: { items: portfolioNews.items as NewsItem[], loading: portfolioNews.loading },
    watchlist: { items: watchlistNews.items as NewsItem[], loading: watchlistNews.loading },
  };
  const { items, loading } = sources[activeTab];

  // Infinite scroll — only the Top feed is cursor-paginated (TickerTick). As the
  // user nears the end (rootMargin below), prefetch the next page.
  const hasNextPage = activeTab === 'top' && !!dashboard.curatedHasNextPage;
  const isFetchingNextPage = activeTab === 'top' && !!dashboard.curatedIsFetchingNextPage;
  const fetchNextPage = activeTab === 'top' ? dashboard.curatedFetchNextPage : undefined;

  const scrollRef = useRef<HTMLDivElement | null>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  // Read the latest paging state inside the observer without re-creating it on
  // every page (which would re-fire on an already-visible sentinel and cascade).
  const pageStateRef = useRef({ hasNextPage, isFetchingNextPage, fetchNextPage });
  pageStateRef.current = { hasNextPage, isFetchingNextPage, fetchNextPage };

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      (entries) => {
        if (!entries[0]?.isIntersecting) return;
        const s = pageStateRef.current;
        if (s.hasNextPage && !s.isFetchingNextPage && s.fetchNextPage) s.fetchNextPage();
      },
      { root: scrollRef.current, rootMargin: '500px' },
    );
    obs.observe(el);
    return () => obs.disconnect();
  }, [activeTab]);

  // Publisher facet derived from the active feed (pre-filter), for the source dropdown.
  const sourceOptions = useMemo(
    () => Array.from(new Set(items.map((i) => i.source).filter((s): s is string => !!s))).sort(),
    [items],
  );

  const switchTab = (key: NewsFeedSource) => {
    setActiveTab(key);
    setTickerFilter('');
    setDateRange('all');
    setSourceFilter('all');
    updateConfig({ source: key });
  };

  const hasFilters = tickerFilter.trim() !== '' || dateRange !== 'all' || sourceFilter !== 'all';

  const filteredItems = useMemo(() => {
    let result = items;
    const query = tickerFilter.trim().toUpperCase();
    if (query) {
      result = result.filter((item) => item.tickers?.some((t) => t.toUpperCase().includes(query)));
    }
    if (sourceFilter !== 'all') {
      result = result.filter((item) => item.source === sourceFilter);
    }
    if (dateRange !== 'all') {
      const cutoff = getDateRangeCutoff(dateRange);
      result = result.filter((item) => {
        // Filter on the raw ISO timestamp — not the "24m ago" display string,
        // whose unit format ("24m" vs "24 min") and wording vary by source and
        // locale, which silently broke the Top/Market tabs' time filter.
        const ts = item.publishedAt ? new Date(item.publishedAt).getTime() : NaN;
        return Number.isFinite(ts) && ts >= cutoff;
      });
    }
    return result;
  }, [items, tickerFilter, sourceFilter, dateRange]);

  // Snapshot exporter: full = visible filtered list, rows = single headline.
  useWidgetContextExport(instance.id, {
    full: () => {
      const newsItems = filteredItems.map((it) => ({
        title: it.title,
        source: it.source,
        publishedAt: it.time,
        url: it.articleUrl ?? undefined,
        tickers: it.tickers,
      }));
      const body = serializeNewsItemsToMarkdown(newsItems);
      const text = wrapWidgetContext(
        'news.feed',
        {
          tab: activeTab,
          count: newsItems.length,
          dateRange,
          tickerFilter: tickerFilter || undefined,
          sourceFilter: sourceFilter !== 'all' ? sourceFilter : undefined,
        },
        body,
      );
      return {
        widget_type: 'news.feed',
        widget_id: instance.id,
        label: t('dashboard.widgets.newsFeed.title') + ` · ${t(SOURCE_KEY[activeTab])}`,
        description: `${newsItems.length} headline${newsItems.length === 1 ? '' : 's'}`,
        captured_at: new Date().toISOString(),
        text,
        data: { items: newsItems, tab: activeTab, dateRange, tickerFilter, sourceFilter },
      };
    },
    rows: async (rowId) => {
      const item = items.find((it) => String(it.id ?? `${it.title}`) === rowId);
      if (!item) return null;
      const fallback: NewsArticleDetail = {
        title: item.title,
        source: item.source,
        publishedAt: item.time,
        url: item.articleUrl ?? undefined,
        tickers: item.tickers,
      };
      return buildNewsArticleSnapshot({
        instanceId: instance.id,
        rowId,
        articleId: item.id,
        fallback,
      });
    },
  });

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
            className="text-[0.625rem] font-semibold uppercase tracking-[0.14em]"
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
                className="px-2.5 py-[3px] text-[0.6563rem] uppercase tracking-wider rounded-full transition-colors"
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
            className="flex-1 text-[0.6875rem] bg-transparent border-none outline-none focus-visible:ring-1 focus-visible:ring-ring min-w-0"
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
                className="px-1.5 py-[3px] rounded text-[0.625rem] uppercase tracking-wider transition-colors"
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

        {sourceOptions.length > 0 ? (
          <select
            value={sourceFilter}
            onChange={(e) => setSourceFilter(e.target.value)}
            aria-label={t('dashboard.widgets.newsFeed.sourceLabel')}
            className="h-7 px-2 rounded-md border text-[0.6875rem] outline-none focus-visible:ring-1 focus-visible:ring-ring cursor-pointer max-w-[140px]"
            style={{
              backgroundColor: 'var(--color-bg-subtle)',
              borderColor: 'var(--color-border-muted)',
              color: sourceFilter === 'all' ? 'var(--color-text-tertiary)' : 'var(--color-text-primary)',
            }}
          >
            <option value="all">{t('dashboard.widgets.newsFeed.allSources')}</option>
            {sourceOptions.map((s) => (
              <option key={s} value={s}>
                {s}
              </option>
            ))}
          </select>
        ) : null}

        {hasFilters ? (
          <button
            type="button"
            onClick={() => {
              setTickerFilter('');
              setDateRange('all');
              setSourceFilter('all');
            }}
            className="text-[0.625rem] uppercase tracking-wider transition-colors"
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

      <div ref={scrollRef} className="flex-1 min-h-0 overflow-y-auto -mx-1 px-1">
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
                      : activeTab === 'top'
                        ? t('dashboard.widgets.newsFeed.emptyTop')
                        : t('dashboard.widgets.newsFeed.emptyAddTo', { label: t(SOURCE_KEY[activeTab]).toLowerCase() })}
                </div>
              </div>
            ) : (
              filteredItems.map((item, idx) => {
                const rowId = String(item.id ?? `${item.title}`);
                return (
                  <div
                    key={item.id ?? `${idx}-${item.title}`}
                    className="row-attach-host relative"
                  >
                    <NewsRow
                      item={item}
                      idx={idx}
                      onClick={() => {
                        if (item.id != null) {
                          modals.openNews(item.id, {
                            title: item.title,
                            source: item.source,
                            publishedAt: item.publishedAt ?? null,
                            tickers: item.tickers,
                            articleUrl: item.articleUrl ?? null,
                            author: item.author ?? null,
                            description: item.description ?? null,
                            keywords: item.keywords,
                            sentiments: item.sentiments ?? null,
                            imageUrl: item.image ?? null,
                            favicon: item.favicon ?? null,
                          });
                        }
                      }}
                    />
                    <span className="absolute right-1 top-1/2 -translate-y-1/2 z-10">
                      <RowAttachButton instanceId={instance.id} rowId={rowId} />
                    </span>
                  </div>
                );
              })
            )}
          </motion.div>
        </AnimatePresence>

        {/* Infinite-scroll trigger (Top feed). Sits below the list so the
            observer fires as the user nears the end and prefetches the next page. */}
        <div ref={sentinelRef} aria-hidden className="h-px w-full" />
        {isFetchingNextPage ? (
          <div className="flex justify-center py-3">
            <Loader size={20} className="text-[color:var(--color-accent-primary)]" />
          </div>
        ) : null}
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
