import { useState, useCallback } from 'react';
import { useNavigate } from 'react-router';
import { LineChart, Line, YAxis, Tooltip } from 'recharts';
import { motion, AnimatePresence, type PanInfo } from '@/lib/framer';
import { SPRING_DRAG } from '@/lib/motion';
import { SwapCell } from '@/components/ui/crossfade';
import { SkeletonBar } from '@/components/ui/skeleton';
import { useIsMobile } from '@/hooks/useIsMobile';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { grouped2 } from '@/lib/format';
import { cn, utcMsToETDate } from '@/lib/utils';
import { TermCard, TermChange } from '@/pages/ChatAgent/components/charts/TermCard';
import type { IndexData } from '@/types/market';


interface SparklineDataPoint {
  time?: string;
  val: number;
  i: number;
}

interface IndexCardProps {
  index: IndexData;
}

interface IndexMovementCardProps {
  indices?: IndexData[];
  loading?: boolean;
  /**
   * When true, forces the mobile iOS-style swipeable stack rendering even on
   * desktop viewports. Used by the widget grid when the cell is too narrow to
   * fit the tiles side by side.
   */
  forceMobile?: boolean;
  /** One row of equal cards whatever the viewport: the widget's cell is one
   *  row tall and as wide as its own grid span, not the page. */
  singleRow?: boolean;
}

const SPARK_HEIGHT = 32;

/** The session the figures belong to, M/D: the last trading day when the
 *  market is closed or not yet open, else today in ET. ET, not the browser's
 *  zone, so the fallback matches the ET-derived asOfDate on sibling cards for
 *  users outside US timezones. Only a card on the fallback reads the clock,
 *  so only it ticks. */
function useSessionDate(index: IndexData): string {
  const hasAsOfDate = /^\d{4}-\d{2}-\d{2}$/.test(index.asOfDate ?? '');
  const now = useNow(60_000, !hasAsOfDate);
  const iso = hasAsOfDate ? (index.asOfDate as string) : utcMsToETDate(now);
  const [, mo, d] = iso.split('-');
  return `${Number(mo)}/${Number(d)}`;
}

/* ── One index as a small data card: the price, the day's move, its line ── */

function IndexCardBody({ index, date }: { index: IndexData; date: string }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const hasQuote = index.quoteAvailable !== false;
  const ch = Number(index.change);
  const pct = Number(index.changePercent);
  const chartData: SparklineDataPoint[] = (index.sparklineData || []).map((pt, i) =>
    typeof pt === 'object' ? { ...pt, i } : { val: pt as unknown as number, i },
  );
  const tone = hasQuote
    ? index.isPositive ? 'var(--color-profit)' : 'var(--color-loss)'
    : 'var(--color-term-text-tertiary)';

  return (
    <>
      <div className="term-line">
        <span className="term-big">{hasQuote ? grouped2(Number(index.price), locale) : 'N/A'}</span>
      </div>
      <div className="term-line">
        {hasQuote ? (
          <>
            <TermChange value={pct} />
            <span className="term-t3">{(ch > 0 ? '+' : '') + grouped2(ch, locale)}</span>
          </>
        ) : (
          <span className="term-t3">N/A</span>
        )}
        {/* The session the line is for, at the line's end: in the edge it
            would take the room the index's name needs. */}
        <span className="term-push term-t3">{date}</span>
      </div>
      <div className="dash-spark" style={{ height: SPARK_HEIGHT }}>
        {chartData.length > 1 ? (
          <LineChart
            responsive
            width="100%"
            height={SPARK_HEIGHT}
            data={chartData}
            margin={{ top: 2, right: 0, bottom: 2, left: 0 }}
            // Not a tab stop of its own: the card's title is its button, and
            // a focusable chart would swallow the card's click.
            accessibilityLayer={false}
          >
            <Line type="monotone" dataKey="val" stroke={tone} strokeWidth={1.25} dot={false} isAnimationActive={false} />
            <Tooltip
              content={({ active, payload }) => {
                if (!active || !payload?.[0]) return null;
                const d = payload[0].payload;
                return (
                  <div className="dash-spark-tip">
                    {d.time && <div className="term-t3">{d.time}</div>}
                    <div className="term-strong">{grouped2(Number(d.val), locale)}</div>
                  </div>
                );
              }}
              cursor={{ stroke: 'var(--color-term-edge)', strokeWidth: 1 }}
            />
            <YAxis domain={['dataMin', 'dataMax']} hide />
          </LineChart>
        ) : (
          <span className="term-t3">{t('dashboard.widgets.marketsOverview.noChartData')}</span>
        )}
      </div>
    </>
  );
}

/* ── Desktop card. It has no entrance of its own: the grid it sits in crossfades
   in from the skeleton, so the cards settle as one. ── */

function IndexCard({ index }: IndexCardProps) {
  const navigate = useNavigate();
  const date = useSessionDate(index);
  return (
    <TermCard
      title={index.symbol}
      sub={index.name}
      keep="title"
      onClick={() => navigate(`/market?symbol=^${index.symbol}`)}
    >
      <IndexCardBody index={index} date={date} />
    </TermCard>
  );
}

/* ── Mobile: iOS Smart Stack-style swipeable widget ── */

const swipeVariants = {
  enter: (dir: number) => ({
    x: dir > 0 ? '80%' : '-80%',
    opacity: 0,
    scale: 0.92,
  }),
  center: {
    x: 0,
    opacity: 1,
    scale: 1,
  },
  exit: (dir: number) => ({
    x: dir > 0 ? '-80%' : '80%',
    opacity: 0,
    scale: 0.92,
  }),
};

function IndexStackWidget({ indices }: { indices: IndexData[] }) {
  const { t } = useTranslation();
  const [current, setCurrent] = useState(0);
  const [direction, setDirection] = useState(0);

  const paginate = useCallback(
    (dir: number) => {
      setCurrent((prev) => {
        const next = prev + dir;
        if (next < 0 || next >= indices.length) return prev;
        setDirection(dir);
        return next;
      });
    },
    [indices.length],
  );

  const goTo = useCallback(
    (i: number) => {
      setCurrent((prev) => {
        if (i === prev) return prev;
        setDirection(i > prev ? 1 : -1);
        return i;
      });
    },
    [],
  );

  const handleDragEnd = useCallback(
    (_: unknown, { offset, velocity }: PanInfo) => {
      if (offset.x < -40 || velocity.x < -300) paginate(1);
      else if (offset.x > 40 || velocity.x > 300) paginate(-1);
    },
    [paginate],
  );

  const index = indices[current];

  return (
    <div>
      <div className="relative overflow-hidden" style={{ touchAction: 'pan-y' }}>
        <AnimatePresence initial={false} custom={direction} mode="popLayout">
          <motion.div
            key={current}
            custom={direction}
            variants={swipeVariants}
            initial="enter"
            animate="center"
            exit="exit"
            transition={SPRING_DRAG}
            drag="x"
            dragConstraints={{ left: 0, right: 0 }}
            dragElastic={0.12}
            onDragEnd={handleDragEnd}
            style={{ touchAction: 'pan-y' }}
          >
            <IndexCard index={index} />
          </motion.div>
        </AnimatePresence>
      </div>

      {/* Dot indicators */}
      <div className="flex justify-center items-center gap-1.5 mt-2">
        {indices.map((_, i) => (
          <button
            key={i}
            onClick={() => goTo(i)}
            aria-label={t('dashboard.widgets.marketsOverview.showIndex', { name: indices[i]?.name })}
            className="rounded-full transition-all"
            style={{
              width: i === current ? 18 : 6,
              height: 6,
              backgroundColor:
                i === current
                  ? 'var(--color-term-text-secondary)'
                  : 'var(--color-term-edge)',
            }}
          />
        ))}
      </div>
    </div>
  );
}

/* ── Skeleton loader (single card on mobile, grid on desktop) ── */

function IndexSkeleton({ count }: { count: number }) {
  // The card's own lines with bars in them, so each keeps the height its text
  // will have and the cards do not grow when the quotes land.
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        <TermCard key={i} title={<SkeletonBar style={{ width: 32 }} />}>
          <div className="term-line">
            <span className="term-big"><SkeletonBar style={{ width: 72 }} /></span>
          </div>
          <div className="term-line">
            <SkeletonBar style={{ width: 56 }} />
          </div>
          <div className="dash-spark" style={{ height: SPARK_HEIGHT }} />
        </TermCard>
      ))}
    </>
  );
}

/* ── Main export ── */

function IndexMovementCard({
  indices = [],
  loading = false,
  forceMobile = false,
  singleRow = false,
}: IndexMovementCardProps) {
  const isMobileViewport = useIsMobile();
  const isMobile = forceMobile || isMobileViewport;

  if (isMobile) {
    if (loading) {
      return <IndexSkeleton count={1} />;
    }
    if (indices.length === 0) return null;
    return <IndexStackWidget indices={indices} />;
  }

  return (
    <SwapCell
      as="div"
      className="items-start"
      swapKey={loading ? 'loading' : 'ready'}
      layerClassName={cn(
        'grid gap-3',
        singleRow ? 'grid-flow-col auto-cols-fr' : 'grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5',
      )}
    >
      {loading ? (
        <IndexSkeleton count={5} />
      ) : (
        indices.map((index) => <IndexCard key={index.symbol} index={index} />)
      )}
    </SwapCell>
  );
}

export default IndexMovementCard;
