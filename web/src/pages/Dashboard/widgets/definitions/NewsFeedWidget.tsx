import { useEffect, useEffectEvent, useMemo, useRef, useState, type ReactNode } from 'react';
import { useTranslation } from 'react-i18next';
import { Newspaper, Search, X } from 'lucide-react';
import { Loader } from '@/components/ui/loader';
import { SwapCell } from '@/components/ui/crossfade';
import { SkeletonBar } from '@/components/ui/skeleton';
import { useNow } from '@/hooks/useNow';
import { useLocale } from '@/hooks/useLocale';
import { clockTime, relativeTime, shortMonthDay } from '@/lib/format';
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
import { isWithinDateRange, type NewsDateRange, type NewsSentimentItem } from '../../utils/newsItem';
import { RowAttachButton } from '../../components/RowAttachButton';
import { FaviconImg } from '@/pages/ChatAgent/components/charts/FaviconImg';
import { hostOf } from '@/pages/ChatAgent/components/charts/cardReaders';
import { WidgetCardMeta } from '../framework/widgetCardSlots';
import type { WidgetRenderProps } from '../types';

type NewsFeedSource = 'top' | 'market' | 'portfolio' | 'watchlist';
type NewsFeedConfig = { source?: NewsFeedSource; limit?: number };

const SOURCE_KEY: Record<NewsFeedSource, string> = {
  top: 'dashboard.widgets.newsFeed.tab_top',
  market: 'dashboard.widgets.newsFeed.tab_market',
  portfolio: 'dashboard.widgets.newsFeed.tab_portfolio',
  watchlist: 'dashboard.widgets.newsFeed.tab_watchlist',
};

const SOURCES: NewsFeedSource[] = ['top', 'market', 'portfolio', 'watchlist'];

const DATE_RANGES: { key: NewsDateRange; labelKey: string }[] = [
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

/** When a story ran: its clock time today, its date before that. */
function storyTime(publishedAt: string | null | undefined, locale: string, now: number): string {
  if (!publishedAt) return '';
  const d = new Date(publishedAt);
  if (Number.isNaN(d.getTime())) return '';
  const today = new Date(now);
  const sameDay =
    d.getFullYear() === today.getFullYear() && d.getMonth() === today.getMonth() && d.getDate() === today.getDate();
  return sameDay ? clockTime(d, locale) : shortMonthDay(d, locale);
}

/** One story on one line: when, the headline, who ran it, what it names. */
function NewsRow({
  item,
  onClick,
  attach,
}: {
  item: NewsItem;
  onClick: () => void;
  attach: ReactNode;
}) {
  const locale = useLocale();
  const now = useNow();
  const time = storyTime(item.publishedAt, locale, now);
  const tickers = item.tickers ?? [];

  return (
    <tr className="row-attach-host" data-clickable="" onClick={onClick}>
      <td className="term-t3" title={relativeTime(item.publishedAt, locale, now) || undefined}>{time}</td>
      <td className="term-fill dash-attach-cell">
        <span title={item.title}>{item.title}</span>
        {attach}
      </td>
      <td className="term-t3" title={item.source ?? undefined}>
        <span className="dash-source">
          <FaviconImg
            domain={hostOf(item.articleUrl ?? undefined)}
            icon={item.favicon ?? undefined}
            name={item.source}
            size={12}
          />
          <span className="dash-source-name">{item.source}</span>
        </span>
      </td>
      <td className="term-code">
        {tickers[0] ?? ''}
        {tickers.length > 1 && <span className="term-t3"> +{tickers.length - 1}</span>}
      </td>
    </tr>
  );
}

function NewsFeedWidget({ instance, updateConfig }: WidgetRenderProps<NewsFeedConfig>) {
  const { t } = useTranslation();
  const { dashboard, portfolioNews, watchlistNews, modals } = useDashboardContext();
  const initialSource: NewsFeedSource = instance.config.source ?? 'market';
  const [activeTab, setActiveTab] = useState<NewsFeedSource>(initialSource);
  const [tickerFilter, setTickerFilter] = useState('');
  const [dateRange, setDateRange] = useState<NewsDateRange>('all');
  const [sourceFilter, setSourceFilter] = useState('all');
  const now = useNow();

  const sources: Record<NewsFeedSource, { items: NewsItem[]; loading: boolean }> = {
    top: { items: dashboard.curatedItems as NewsItem[], loading: dashboard.curatedLoading },
    market: { items: dashboard.newsItems as NewsItem[], loading: dashboard.newsLoading },
    portfolio: { items: portfolioNews.items as NewsItem[], loading: portfolioNews.loading },
    watchlist: { items: watchlistNews.items as NewsItem[], loading: watchlistNews.loading },
  };
  const { items, loading } = sources[activeTab];

  // Infinite scroll — the Top (TickerTick) and Market feeds are cursor-
  // paginated; portfolio/watchlist are single-page. As the user nears the end
  // (rootMargin below), prefetch the next page. The Market chain feed (non-CN
  // users) never serves a cursor, so hasNextPage stays false there.
  const paging =
    activeTab === 'top'
      ? {
          hasNextPage: !!dashboard.curatedHasNextPage,
          isFetchingNextPage: !!dashboard.curatedIsFetchingNextPage,
          fetchNextPage: dashboard.curatedFetchNextPage,
        }
      : activeTab === 'market'
        ? {
            hasNextPage: !!dashboard.newsHasNextPage,
            isFetchingNextPage: !!dashboard.newsIsFetchingNextPage,
            fetchNextPage: dashboard.newsFetchNextPage,
          }
        : { hasNextPage: false, isFetchingNextPage: false, fetchNextPage: undefined };
  const { hasNextPage, isFetchingNextPage, fetchNextPage } = paging;

  const scrollRef = useRef<HTMLDivElement | null>(null);
  const sentinelRef = useRef<HTMLDivElement | null>(null);
  // Read the latest paging state inside the observer without re-creating it on
  // every page (which would re-fire on an already-visible sentinel and cascade).
  const loadNextPageIfIdle = useEffectEvent(() => {
    if (hasNextPage && !isFetchingNextPage && fetchNextPage) fetchNextPage();
  });

  useEffect(() => {
    const el = sentinelRef.current;
    if (!el) return;
    const obs = new IntersectionObserver(
      (entries) => {
        if (!entries[0]?.isIntersecting) return;
        loadNextPageIfIdle();
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
    // The feeds dissolve in one cell at the same scroll offset, so the next
    // one opens at its top rather than out of view below a short list.
    if (key !== activeTab) scrollRef.current?.scrollTo({ top: 0 });
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
      result = result.filter((item) => isWithinDateRange(item.publishedAt, dateRange, now));
    }
    return result;
  }, [items, tickerFilter, sourceFilter, dateRange, now]);
  const showSkeleton = loading && filteredItems.length === 0;

  // Snapshot exporter: full = visible filtered list, rows = single headline.
  useWidgetContextExport(instance.id, {
    full: () => {
      const newsItems = filteredItems.map((it) => ({
        title: it.title,
        source: it.source,
        publishedAt: it.publishedAt ?? undefined,
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
        publishedAt: item.publishedAt ?? undefined,
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
    <div className="widget-term dash-fill dash-news">
      {/* Held back while the feed loads: a count of 0 that turns into the real
          one reads as a change, not a load. */}
      {!showSkeleton && <WidgetCardMeta>{items.length}</WidgetCardMeta>}
      <div className="dash-chips">
        {SOURCES.map((key) => (
          <button
            key={key}
            type="button"
            className="term-chip widget-drag-cancel"
            // Named outright: Terminal draws the chip's brackets as generated
            // content, which would otherwise be read as part of its name.
            aria-label={t(SOURCE_KEY[key])}
            aria-pressed={activeTab === key}
            onClick={() => switchTab(key)}
          >
            {t(SOURCE_KEY[key])}
          </button>
        ))}

        <div className="dash-filters widget-drag-cancel">
          <label className="dash-field rings-within owns-its-edge">
            <Search size={10} aria-hidden="true" />
            <input
              type="text"
              placeholder={t('dashboard.widgets.newsFeed.tickerPlaceholder')}
              value={tickerFilter}
              onChange={(e) => setTickerFilter(e.target.value)}
            />
            {tickerFilter ? (
              <button
                type="button"
                onClick={() => setTickerFilter('')}
                className="inline-flex"
                aria-label={t('dashboard.widgets.newsFeed.clearTicker')}
              >
                <X size={10} />
              </button>
            ) : null}
          </label>

          <span className="dash-ranges">
            {DATE_RANGES.map((dr) => (
              <button
                key={dr.key}
                type="button"
                className="dash-range"
                aria-pressed={dateRange === dr.key}
                onClick={() => setDateRange(dr.key)}
              >
                {t(dr.labelKey)}
              </button>
            ))}
          </span>

          {sourceOptions.length > 0 ? (
            <select
              value={sourceFilter}
              onChange={(e) => setSourceFilter(e.target.value)}
              aria-label={t('dashboard.widgets.newsFeed.sourceLabel')}
              className="dash-field"
              data-set={sourceFilter !== 'all' ? '' : undefined}
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
              className="dash-range"
              onClick={() => {
                setTickerFilter('');
                setDateRange('all');
                setSourceFilter('all');
              }}
            >
              {t('dashboard.widgets.newsFeed.clear')}
            </button>
          ) : null}
        </div>
      </div>

      <div ref={scrollRef} className="dash-scroll">
        {/* The tabs dissolve in one grid cell, so a switch never passes through
            an empty feed. A tab the reader picks rises; the skeleton giving way
            to its rows crossfades, and rows never enter one by one. */}
        <SwapCell
          as="div"
          className={!showSkeleton && filteredItems.length === 0 ? 'h-full' : 'items-start'}
          riseKey={activeTab}
          swapKey={`${activeTab}:${showSkeleton ? 'loading' : 'ready'}`}
        >
          {showSkeleton ? (
            <table className="term-table dash-table">
              <tbody>
                {Array.from({ length: 6 }).map((_, i) => (
                  <tr key={i}>
                    <td><SkeletonBar style={{ width: '4ch' }} /></td>
                    <td className="term-fill"><SkeletonBar style={{ width: `${55 + (i % 3) * 15}%` }} /></td>
                    <td><SkeletonBar style={{ width: '6ch' }} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : filteredItems.length === 0 ? (
            <div className="dash-empty">
              <span>
                {hasFilters
                  ? t('dashboard.widgets.newsFeed.emptyFiltered')
                  : activeTab === 'market'
                    ? t('dashboard.widgets.newsFeed.emptyMarket')
                    : activeTab === 'top'
                      ? t('dashboard.widgets.newsFeed.emptyTop')
                      : t('dashboard.widgets.newsFeed.emptyAddTo', { label: t(SOURCE_KEY[activeTab]).toLowerCase() })}
              </span>
            </div>
          ) : (
            <table className="term-table dash-table">
              <tbody>
                {filteredItems.map((item, idx) => {
                  const rowId = String(item.id ?? `${item.title}`);
                  return (
                    <NewsRow
                      key={item.id ?? `${idx}-${item.title}`}
                      item={item}
                      attach={<RowAttachButton instanceId={instance.id} rowId={rowId} />}
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
                  );
                })}
              </tbody>
            </table>
          )}
        </SwapCell>

        {/* Infinite-scroll trigger (Top + Market feeds). Sits below the list so
            the observer fires as the user nears the end and prefetches the next page. */}
        <div ref={sentinelRef} aria-hidden className="h-px w-full" />
        {isFetchingNextPage ? (
          <div className="flex justify-center py-2">
            <Loader size={14} className="text-(--color-accent-primary)" />
          </div>
        ) : null}
      </div>
    </div>
  );
}

registerWidget<NewsFeedConfig>({
  type: 'news.feed',
  titleKey: 'dashboard.widgets.newsFeed.title',
  cardTitleKey: 'dashboard.widgets.newsFeed.cardTitle',
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
