import { useMemo } from 'react';
import { useQuery } from '@tanstack/react-query';
import { CalendarDays } from 'lucide-react';
import { getEarningsCalendar } from '../../utils/api';
import { registerWidget } from '../framework/WidgetRegistry';
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

const BUCKET_LABELS: Record<BucketKey, string> = {
  today: 'Today',
  tomorrow: 'Tomorrow',
  week: 'This week',
  next: 'Next week',
  later: 'Later',
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
    return d.toLocaleDateString(undefined, { weekday: 'short' });
  }
  return d.toLocaleDateString(undefined, { month: 'short', day: 'numeric' });
}

function EarningsRow({ item, bucket }: { item: EarningsEntry; bucket: BucketKey }) {
  const label = formatDateRight(item.date, bucket);
  return (
    <div
      className="group flex items-center gap-3 px-2 py-2 rounded-md border border-transparent transition-colors"
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
    </div>
  );
}

function BucketSection({
  label,
  items,
  bucket,
}: {
  label: string;
  items: EarningsEntry[];
  bucket: BucketKey;
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
          <EarningsRow key={`${item.symbol}-${item.date}-${i}`} item={item} bucket={bucket} />
        ))}
      </div>
    </div>
  );
}

function EarningsCalendarWidget({ instance }: WidgetRenderProps<EarningsConfig>) {
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
            Earnings
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
          Next {windowDays}d
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
                className="title-font italic text-sm mb-0.5"
                style={{ color: 'var(--color-text-primary)' }}
              >
                No earnings ahead
              </div>
              <div
                className="text-[11px]"
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                Nothing scheduled in the next {windowDays} days.
              </div>
            </div>
          </div>
        ) : (
          <div className="flex flex-col gap-3">
            <BucketSection label={BUCKET_LABELS.today} items={grouped.today} bucket="today" />
            <BucketSection label={BUCKET_LABELS.tomorrow} items={grouped.tomorrow} bucket="tomorrow" />
            <BucketSection label={BUCKET_LABELS.week} items={grouped.week} bucket="week" />
            <BucketSection label={BUCKET_LABELS.next} items={grouped.next} bucket="next" />
            <BucketSection label={BUCKET_LABELS.later} items={grouped.later} bucket="later" />
          </div>
        )}
      </div>

    </div>
  );
}

registerWidget<EarningsConfig>({
  type: 'calendar.earnings',
  title: 'Earnings Calendar',
  description: 'Upcoming earnings, grouped by Today / Tomorrow / This week / Next week.',
  category: 'intel',
  icon: CalendarDays,
  component: EarningsCalendarWidget,
  defaultConfig: { window: '2w', tickers: 'all' },
  defaultSize: { w: 4, h: 26 },
  minSize: { w: 3, h: 15 },
});

export default EarningsCalendarWidget;
