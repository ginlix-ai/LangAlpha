import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import i18n from '@/i18n';
import { useQuery } from '@tanstack/react-query';
import { CalendarDays } from 'lucide-react';
import { getEarningsCalendar } from '../../utils/api';
import { registerWidget } from '../framework/WidgetRegistry';
import { useWidgetContextExport } from '../framework/contextSnapshot';
import { serializeRowsToMarkdown, wrapWidgetContext } from '../framework/snapshotSerializers';
import { EarningsConfigSchema } from '../framework/configSchemas';
import { RowAttachButton } from '../../components/RowAttachButton';
import type { WidgetRenderProps } from '../types';

/** Local-date YYYY-MM-DD. We can't use toISOString() because that emits UTC,
 * which crosses the day boundary for users in non-UTC zones — earnings fetched
 * for "today" then get filtered out by `e.date >= todayStr` or bucketed wrong. */
function localDateStr(d: Date = new Date()): string {
  return d.toLocaleDateString('en-CA'); // en-CA → YYYY-MM-DD in local time
}

type EarningsConfig = { window?: '1w' | '2w' | '1m'; tickers?: 'all' | 'portfolio' };

interface EarningsEntry {
  symbol: string;
  date: string;
  companyName?: string;
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

function bucketFor(dateStr: string): BucketKey {
  const now = new Date();
  const startOfToday = new Date(now.getFullYear(), now.getMonth(), now.getDate()).getTime();
  const d = toDate(dateStr).getTime();
  const dayMs = 86_400_000;
  if (d >= startOfToday && d < startOfToday + dayMs) return 'today';
  if (d >= startOfToday + dayMs && d < startOfToday + 2 * dayMs) return 'tomorrow';
  if (d >= startOfToday + 2 * dayMs && d < startOfToday + 7 * dayMs) return 'week';
  if (d >= startOfToday + 7 * dayMs && d < startOfToday + 14 * dayMs) return 'next';
  return 'later';
}

function formatDateRight(dateStr: string, bucket: BucketKey): string {
  const d = toDate(dateStr);
  if (bucket === 'today') return 'Today';
  if (bucket === 'tomorrow') return 'Tomorrow';
  if (bucket === 'week') {
    return d.toLocaleDateString(i18n.language, { weekday: 'short' });
  }
  return d.toLocaleDateString(i18n.language, { month: 'short', day: 'numeric' });
}

function serializeEarningsToMarkdown(items: EarningsEntry[]): string {
  if (!items.length) return '_no upcoming earnings in window_';
  return serializeRowsToMarkdown(items, [
    { key: 'symbol', label: 'symbol' },
    { key: 'date', label: 'date' },
    { key: 'companyName', label: 'company', format: (v) => (typeof v === 'string' ? v : '') },
  ]);
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
  // formatDateRight returns 'Today'/'Tomorrow' fallthrough or the locale's
  // weekday/short-month string. Translate the today/tomorrow shortcuts so
  // they swap with locale; the others come from native Intl.DateTimeFormat.
  const raw = formatDateRight(item.date, bucket);
  const label = raw === 'Today'
    ? t('dashboard.widgets.earningsCalendar.bucket_today')
    : raw === 'Tomorrow'
      ? t('dashboard.widgets.earningsCalendar.bucket_tomorrow')
      : raw;
  return (
    <div
      className="row-attach-host group relative flex items-center gap-3 px-2 py-2 rounded-md border border-transparent transition-colors"
      onMouseEnter={(e) => {
        e.currentTarget.style.backgroundColor = 'var(--color-bg-hover)';
      }}
      onMouseLeave={(e) => {
        e.currentTarget.style.backgroundColor = 'transparent';
      }}
    >
      <div
        className="w-8 h-8 rounded-md flex items-center justify-center overflow-hidden border flex-shrink-0"
        style={{
          backgroundColor: 'var(--color-bg-subtle)',
          borderColor: 'var(--color-border-muted)',
        }}
      >
        <span
          className="font-bold text-[10px] dashboard-mono"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {(item.symbol || '??').substring(0, 2).toUpperCase()}
        </span>
      </div>
      <div className="flex-1 min-w-0 flex flex-col gap-0.5">
        <span
          className="text-[13px] font-semibold leading-tight truncate"
          style={{ color: 'var(--color-text-primary)' }}
        >
          {item.symbol}
        </span>
        <span
          className="text-[10.5px] truncate"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {item.companyName || item.symbol}
        </span>
      </div>
      <span
        className="text-[10.5px] dashboard-mono uppercase tracking-wider tabular-nums flex-shrink-0"
        style={{ color: 'var(--color-text-tertiary)' }}
      >
        {label}
      </span>
      <span className="absolute right-1 top-1/2 -translate-y-1/2">
        <RowAttachButton instanceId={instanceId} rowId={`${item.symbol}-${item.date}`} />
      </span>
    </div>
  );
}

function BucketSection({
  label,
  items,
  bucket,
  instanceId,
}: {
  label: string;
  items: EarningsEntry[];
  bucket: BucketKey;
  instanceId: string;
}) {
  if (items.length === 0) return null;
  return (
    <div className="flex flex-col">
      <div
        className="flex items-baseline gap-2 px-3 mb-0.5 sticky top-0 z-10 py-1 -mx-1"
        style={{ backgroundColor: 'var(--color-bg-card)' }}
      >
        <span
          className="text-[9.5px] font-semibold uppercase tracking-[0.16em]"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {label}
        </span>
        <span
          className="flex-1 h-px"
          style={{ backgroundColor: 'var(--color-border-muted)' }}
        />
        <span
          className="text-[10px] dashboard-mono tabular-nums"
          style={{ color: 'var(--color-text-tertiary)', opacity: 0.7 }}
        >
          {String(items.length).padStart(2, '0')}
        </span>
      </div>
      <div className="flex flex-col">
        {items.map((item, i) => (
          <EarningsRow
            key={`${item.symbol}-${item.date}-${i}`}
            item={item}
            bucket={bucket}
            instanceId={instanceId}
          />
        ))}
      </div>
    </div>
  );
}

function EarningsCalendarWidget({ instance }: WidgetRenderProps<EarningsConfig>) {
  const { t } = useTranslation();
  const windowDays = WINDOW_DAYS[instance.config.window ?? '2w'];

  // Use local dates end-to-end: the `from`/`to` we send must match the same
  // day the user's clock is on, or the >= todayStr filter below will drop
  // "today's" earnings for anyone east of UTC.
  const todayStr = localDateStr();
  const toStr = localDateStr(new Date(Date.now() + windowDays * 86_400_000));

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
      buckets[bucketFor(e.date)].push(e);
    }
    return buckets;
  }, [upcoming]);

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
      const lines = [
        `**${item.symbol}** · ${item.date}`,
      ];
      if (item.companyName) lines.push(item.companyName);
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
        data: { row: { symbol: item.symbol, date: item.date, companyName: item.companyName } },
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
          <CalendarDays
            className="h-3.5 w-3.5 flex-shrink-0 self-center"
            style={{ color: 'var(--color-text-tertiary)' }}
          />
          <span
            className="text-[10px] font-semibold uppercase tracking-[0.14em]"
            style={{ color: 'var(--color-text-secondary)' }}
          >
            {t('dashboard.widgets.earningsCalendar.header')}
          </span>
          <span
            className="title-font text-lg leading-none dashboard-mono"
            style={{ color: 'var(--color-text-primary)' }}
          >
            {upcoming.length}
          </span>
        </div>
        <span
          className="text-[10px] uppercase tracking-wider flex-shrink-0"
          style={{ color: 'var(--color-text-tertiary)' }}
        >
          {t('dashboard.widgets.earningsCalendar.windowLabel', { days: windowDays })}
        </span>
      </div>

      <div className="flex-1 min-h-0 overflow-y-auto -mx-1 px-1">
        {loading && upcoming.length === 0 ? (
          <div className="space-y-1.5 py-1">
            {Array.from({ length: 5 }).map((_, i) => (
              <div
                key={i}
                className="h-10 rounded animate-pulse"
                style={{
                  backgroundColor: 'var(--color-bg-subtle)',
                  opacity: 1 - i * 0.1,
                }}
              />
            ))}
          </div>
        ) : upcoming.length === 0 ? (
          <div className="h-full flex flex-col items-center justify-center gap-3 py-6">
            <div
              className="h-9 w-9 rounded-full flex items-center justify-center"
              style={{ backgroundColor: 'var(--color-bg-subtle)' }}
            >
              <CalendarDays className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
            </div>
            <div className="text-center">
              <div
                className="dashboard-mono text-sm mb-0.5"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {t('dashboard.widgets.earningsCalendar.empty')}
              </div>
              <div
                className="text-[11px]"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {t('dashboard.widgets.earningsCalendar.emptyHint', { days: windowDays })}
              </div>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            <BucketSection label={t(BUCKET_KEY.today)} items={grouped.today} bucket="today" instanceId={instance.id} />
            <BucketSection label={t(BUCKET_KEY.tomorrow)} items={grouped.tomorrow} bucket="tomorrow" instanceId={instance.id} />
            <BucketSection label={t(BUCKET_KEY.week)} items={grouped.week} bucket="week" instanceId={instance.id} />
            <BucketSection label={t(BUCKET_KEY.next)} items={grouped.next} bucket="next" instanceId={instance.id} />
            <BucketSection label={t(BUCKET_KEY.later)} items={grouped.later} bucket="later" instanceId={instance.id} />
          </div>
        )}
      </div>

    </div>
  );
}

registerWidget<EarningsConfig>({
  type: 'calendar.earnings',
  titleKey: 'dashboard.widgets.earningsCalendar.title',
  descriptionKey: 'dashboard.widgets.earningsCalendar.description',
  category: 'intel',
  icon: CalendarDays,
  component: EarningsCalendarWidget,
  defaultConfig: { window: '2w', tickers: 'all' },
  configSchema: EarningsConfigSchema,
  defaultSize: { w: 4, h: 26 },
  minSize: { w: 3, h: 15 },
});

export default EarningsCalendarWidget;
