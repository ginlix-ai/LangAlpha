import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { useQuery } from '@tanstack/react-query';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { shortMonthDay, shortWeekday } from '@/lib/format';
import { localDateStr } from '@/lib/utils';
import { CalendarDays } from 'lucide-react';
import { SwapCell } from '@/components/ui/crossfade';
import { SkeletonBar } from '@/components/ui/skeleton';
import { formatMoney } from '@/lib/bars';
import { TermTag } from '@/pages/ChatAgent/components/charts/TermCard';
import { getEarningsCalendar } from '../../utils/api';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import { serializeRowsToMarkdown, wrapWidgetContext } from '../framework/snapshotSerializers';
import { EarningsConfigSchema } from '../framework/configSchemas';
import { RowAttachButton } from '../../components/RowAttachButton';
import { WidgetCardMeta } from '../framework/widgetCardSlots';
import type { WidgetRenderProps } from '../types';

type EarningsConfig = { window?: '1w' | '2w' | '1m'; tickers?: 'all' | 'portfolio' };

interface EarningsEntry {
  symbol: string;
  date: string;
  companyName?: string;
  epsEstimated?: number | null;
  /** `amc` / `bmo`, where a payload carries it. */
  time?: string | null;
  [key: string]: unknown;
}

type BucketKey = 'today' | 'tomorrow' | 'week' | 'next' | 'later';

const BUCKET_KEY: Record<BucketKey, string> = {
  today: 'dashboard.widgets.earningsCalendar.bucket_today',
  tomorrow: 'dashboard.widgets.earningsCalendar.bucket_tomorrow',
  week: 'dashboard.widgets.earningsCalendar.bucket_thisWeek',
  next: 'dashboard.widgets.earningsCalendar.bucket_nextWeek',
  later: 'dashboard.widgets.earningsCalendar.bucket_later',
};

const WINDOW_DAYS: Record<NonNullable<EarningsConfig['window']>, number> = {
  '1w': 7,
  '2w': 14,
  '1m': 31,
};

function toDate(dateStr: string): Date {
  return new Date(dateStr + 'T00:00:00');
}

function bucketFor(dateStr: string, now: number): BucketKey {
  const today = new Date(now);
  const startOfToday = new Date(today.getFullYear(), today.getMonth(), today.getDate()).getTime();
  const d = toDate(dateStr).getTime();
  const dayMs = 86_400_000;
  if (d >= startOfToday && d < startOfToday + dayMs) return 'today';
  if (d >= startOfToday + dayMs && d < startOfToday + 2 * dayMs) return 'tomorrow';
  if (d >= startOfToday + 2 * dayMs && d < startOfToday + 7 * dayMs) return 'week';
  if (d >= startOfToday + 7 * dayMs && d < startOfToday + 14 * dayMs) return 'next';
  return 'later';
}


function formatDateRight(dateStr: string, bucket: BucketKey, locale: string): string {
  const d = toDate(dateStr);
  if (bucket === 'today') return 'Today';
  if (bucket === 'tomorrow') return 'Tomorrow';
  if (Number.isNaN(d.getTime())) return '';
  if (bucket === 'week') return shortWeekday(d, locale);
  return shortMonthDay(d, locale);
}

/** When in the session a report lands, where the payload says. */
const SESSION_LABEL: Record<string, string> = { amc: 'AMC', bmo: 'BMO' };

function sessionOf(item: EarningsEntry): string | undefined {
  return typeof item.time === 'string' ? SESSION_LABEL[item.time.toLowerCase()] : undefined;
}

function epsOf(item: EarningsEntry): number | null {
  return typeof item.epsEstimated === 'number' && Number.isFinite(item.epsEstimated) ? item.epsEstimated : null;
}

// What the card shows, so an attached calendar or row tells the agent the same.
function serializeEarningsToMarkdown(items: EarningsEntry[]): string {
  if (!items.length) return '_no upcoming earnings in window_';
  return serializeRowsToMarkdown(
    items.map((item) => ({ ...item, session: sessionOf(item) ?? '', eps: epsOf(item) })),
    [
      { key: 'symbol', label: 'symbol' },
      { key: 'date', label: 'date' },
      { key: 'session', label: 'session' },
      { key: 'companyName', label: 'company', format: (v) => (typeof v === 'string' ? v : '') },
      { key: 'eps', label: 'eps_est' },
    ],
  );
}

function EarningsRow({
  item,
  bucket,
  instanceId,
}: {
  item: EarningsEntry;
  bucket: BucketKey;
  instanceId: string;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  // formatDateRight returns 'Today'/'Tomorrow' fallthrough or the locale's
  // weekday/short-month string. Translate the today/tomorrow shortcuts so
  // they swap with locale; the others come from native Intl.DateTimeFormat.
  const raw = formatDateRight(item.date, bucket, locale);
  const label = raw === 'Today'
    ? t('dashboard.widgets.earningsCalendar.bucket_today')
    : raw === 'Tomorrow'
      ? t('dashboard.widgets.earningsCalendar.bucket_tomorrow')
      : raw;
  const session = sessionOf(item);
  const eps = epsOf(item);
  return (
    <tr className="row-attach-host">
      <td className="term-t3">{label}</td>
      <td className="term-code term-strong">{item.symbol}</td>
      <td className="term-fill dash-attach-cell">
        {session && <TermTag>{session}</TermTag>}
        {item.companyName && (
          <span className={session ? 'term-t3 term-aside' : 'term-t3'}>{item.companyName}</span>
        )}
        <RowAttachButton instanceId={instanceId} rowId={`${item.symbol}-${item.date}`} />
      </td>
      <td className="term-num term-t2">
        {eps != null ? t('dashboard.widgets.earningsCalendar.estEps', { value: formatMoney(eps, 'USD', locale) }) : ''}
      </td>
    </tr>
  );
}

function EarningsCalendarWidget({ instance }: WidgetRenderProps<EarningsConfig>) {
  const { t } = useTranslation();
  const windowDays = WINDOW_DAYS[instance.config.window ?? '2w'];

  // Use local dates end-to-end: the `from`/`to` we send must match the same
  // day the user's clock is on, or the >= todayStr filter below will drop
  // "today's" earnings for anyone east of UTC.
  // Both ends come from one clock reading; the query key changes only when a
  // date string does.
  const now = useNow();
  const todayStr = localDateStr(now);
  const toStr = localDateStr(now + windowDays * 86_400_000);

  const { data: earnings = [], isLoading: loading } = useQuery<EarningsEntry[]>({
    queryKey: ['earnings-calendar', todayStr, toStr],
    queryFn: async () => {
      const result = await getEarningsCalendar({ from: todayStr, to: toStr });
      return ((result?.data || []) as EarningsEntry[]).filter(
        (e) => e.symbol && !e.symbol.includes('.')
      );
    },
    staleTime: 5 * 60_000,
  });

  const upcoming = useMemo(
    () =>
      earnings
        .filter((e) => e.date >= todayStr)
        .sort((a, b) => a.date.localeCompare(b.date)),
    [earnings, todayStr]
  );

  const grouped = useMemo(() => {
    const buckets: Record<BucketKey, EarningsEntry[]> = {
      today: [],
      tomorrow: [],
      week: [],
      next: [],
      later: [],
    };
    for (const e of upcoming) {
      buckets[bucketFor(e.date, now)].push(e);
    }
    return buckets;
  }, [upcoming, now]);

  useWidgetContextExport(instance.id, {
    full: () => {
      const sections: string[] = [];
      const counts: Record<BucketKey, number> = {
        today: grouped.today.length,
        tomorrow: grouped.tomorrow.length,
        week: grouped.week.length,
        next: grouped.next.length,
        later: grouped.later.length,
      };
      (Object.keys(grouped) as BucketKey[]).forEach((b) => {
        if (grouped[b].length === 0) return;
        sections.push(`### ${t(BUCKET_KEY[b])} (${grouped[b].length})`, '', serializeEarningsToMarkdown(grouped[b]), '');
      });
      const body = sections.length
        ? sections.join('\n').trimEnd()
        : `_no upcoming earnings in next ${windowDays}d_`;
      const text = wrapWidgetContext(
        'calendar.earnings',
        { window_days: windowDays, count: upcoming.length, from: todayStr, to: toStr },
        body,
      );
      return {
        widget_type: 'calendar.earnings',
        widget_id: instance.id,
        label: `${t('dashboard.widgets.earningsCalendar.title')} · ${upcoming.length}`,
        description: `next ${windowDays}d`,
        captured_at: new Date().toISOString(),
        text,
        data: { window_days: windowDays, from: todayStr, to: toStr, counts, upcoming },
      };
    },
    rows: (rowId: string) => {
      const item = upcoming.find((e) => `${e.symbol}-${e.date}` === rowId);
      if (!item) return null;
      const session = sessionOf(item);
      const eps = epsOf(item);
      const lines = [
        `**${item.symbol}** · ${item.date}${session ? ` · ${session}` : ''}`,
      ];
      if (item.companyName) lines.push(item.companyName);
      if (eps != null) lines.push(`EPS estimate: ${eps}`);
      const text = wrapWidgetContext(
        'calendar.earnings/row',
        { symbol: item.symbol, date: item.date },
        lines.join('\n'),
      );
      return {
        widget_type: 'calendar.earnings/row',
        widget_id: `${instance.id}/${rowId}`,
        label: `${item.symbol} · ${item.date}`,
        description: t('dashboard.widgets.earningsCalendar.title'),
        captured_at: new Date().toISOString(),
        text,
        data: { row: { symbol: item.symbol, date: item.date, companyName: item.companyName, session, epsEstimated: eps } },
      };
    },
  });

  return (
    <div className="widget-term dash-fill">
      <WidgetCardMeta>
        {!(loading && upcoming.length === 0) && <span>{upcoming.length}</span>}
        <span>{t('dashboard.widgets.earningsCalendar.windowLabel', { days: windowDays })}</span>
      </WidgetCardMeta>
      <div className="dash-scroll">
        {/* The skeleton crossfades to the list in place, holding the cell
            at its last height until the list lands. */}
        <SwapCell
          as="div"
          className="items-start"
          swapKey={loading && upcoming.length === 0 ? 'loading' : 'ready'}
        >
          {loading && upcoming.length === 0 ? (
            <table className="term-table dash-table" data-fit-hold="">
              <tbody>
                {Array.from({ length: 5 }).map((_, i) => (
                  <tr key={i}>
                    <td><SkeletonBar style={{ width: '4ch' }} /></td>
                    <td><SkeletonBar style={{ width: '4ch' }} /></td>
                    <td className="term-fill" />
                    <td className="term-num"><SkeletonBar style={{ width: '7ch' }} /></td>
                  </tr>
                ))}
              </tbody>
            </table>
          ) : upcoming.length === 0 ? (
            <div className="dash-empty">
              <span>{t('dashboard.widgets.earningsCalendar.empty')}</span>
              <span>{t('dashboard.widgets.earningsCalendar.emptyHint', { days: windowDays })}</span>
            </div>
          ) : (
            <table className="term-table dash-table">
              <tbody>
                {upcoming.map((item, i) => (
                  <EarningsRow
                    key={`${item.symbol}-${item.date}-${i}`}
                    item={item}
                    bucket={bucketFor(item.date, now)}
                    instanceId={instance.id}
                  />
                ))}
              </tbody>
            </table>
          )}
        </SwapCell>
      </div>
    </div>
  );
}

registerWidget<EarningsConfig>({
  type: 'calendar.earnings',
  titleKey: 'dashboard.widgets.earningsCalendar.title',
  cardTitleKey: 'dashboard.widgets.earningsCalendar.header',
  descriptionKey: 'dashboard.widgets.earningsCalendar.description',
  category: 'intel',
  icon: CalendarDays,
  component: EarningsCalendarWidget,
  defaultConfig: { window: '2w', tickers: 'all' },
  configSchema: EarningsConfigSchema,
  defaultSize: { w: 4, h: 16 },
  minSize: { w: 3, h: 6 },
  maxSize: { w: 12, h: 26 },
  fitToContent: true,
});

export default EarningsCalendarWidget;
