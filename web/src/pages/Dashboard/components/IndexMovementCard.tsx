import React, { useState, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { useNavigate } from 'react-router-dom';
import { LineChart, Line, ResponsiveContainer, YAxis, Tooltip } from 'recharts';
import { motion, AnimatePresence, type PanInfo } from 'framer-motion';
import { useIsMobile } from '@/hooks/useIsMobile';
import { createFormatter } from '@/lib/format';
import type { IndexData } from '@/types/market';

const fmt2 = createFormatter({ minimumFractionDigits: 2, maximumFractionDigits: 2 });

interface SparklineDataPoint {
  time?: string;
  val: number;
  i: number;
}

interface IndexCardProps {
  index: IndexData;
  delay: number;
}

interface IndexMovementCardProps {
  indices?: IndexData[];
  loading?: boolean;
  /**
   * When true, forces the mobile iOS-style swipeable stack rendering even on
   * desktop viewports. Used by the widget grid when the cell is too narrow to
   * fit the 5-column tile layout comfortably.
   */
  forceMobile?: boolean;
}

/* ── Shared card content (no animation wrapper) ── */

function IndexCardContent({ index }: { index: IndexData }) {
  useTranslation();
  const pos = index.isPositive;
  const ch = Number(index.change);
  const pct = Number(index.changePercent);
  const changeStr = fmt2(ch);
  const pctStr = '(' + (pos ? '+' : '') + fmt2(pct) + '%)';
  const chartData: SparklineDataPoint[] = (index.sparklineData || []).map((pt, i) =>
    typeof pt === 'object' ? { ...pt, i } : { val: pt as unknown as number, i },
  );

  const today = new Date();
  const dateStr = `${today.getMonth() + 1}/${today.getDate()}`;

  return (
    <>
      {/* Header: name+date | price, symbol | change */}
      <div className="p-4 pb-0">
        <div className="flex justify-between items-start">
          <div>
            <div className="flex items-baseline gap-2">
              <h3
                className="text-base font-bold tracking-tight"
                style={{ color: 'var(--color-text-primary)' }}
              >
                {index.name}
              </h3>
              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {dateStr}
              </span>
            </div>
            <div className="text-xs mt-0.5" style={{ color: 'var(--color-text-secondary)' }}>
              ^{index.symbol}
            </div>
          </div>
          <div className="text-right">
            <div
              className="text-lg font-bold tracking-tight dashboard-mono"
              style={{ color: 'var(--color-text-primary)' }}
            >
              {fmt2(Number(index.price))}
            </div>
            <div
              className="text-xs dashboard-mono"
              style={{ color: pos ? 'var(--color-profit)' : 'var(--color-loss)' }}
            >
              {changeStr} {pctStr}
            </div>
          </div>
        </div>
      </div>

      {/* Sparkline chart */}
      <div className="mt-2 px-1 pb-2 [&_*]:outline-none" style={{ height: 100 }}>
        {chartData.length > 1 ? (
          <ResponsiveContainer width="100%" height={100}>
            <LineChart data={chartData}>
              <Line
                type="monotone"
                dataKey="val"
                stroke={pos ? 'var(--color-profit)' : 'var(--color-loss)'}
                strokeWidth={1.5}
                dot={false}
                isAnimationActive={false}
              />
              <Tooltip
                content={({ active, payload }) => {
                  if (!active || !payload?.[0]) return null;
                  const d = payload[0].payload;
                  return (
                    <div
                      className="rounded-lg px-2.5 py-1.5 text-xs shadow-lg border"
                      style={{
                        backgroundColor: 'var(--color-bg-card)',
                        borderColor: 'var(--color-border-muted)',
                        color: 'var(--color-text-primary)',
                      }}
                    >
                      {d.time && (
                        <div style={{ color: 'var(--color-text-secondary)' }}>{d.time}</div>
                      )}
                      <div className="font-semibold dashboard-mono">
                        {fmt2(Number(d.val))}
                      </div>
                    </div>
                  );
                }}
                cursor={{ stroke: 'var(--color-border-default)', strokeWidth: 1 }}
              />
              <YAxis domain={['dataMin', 'dataMax']} hide />
            </LineChart>
          </ResponsiveContainer>
        ) : (
          <div className="w-full h-full flex items-center justify-center">
            <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
              No chart data
            </span>
          </div>
        )}
      </div>
    </>
  );
}

/* ── Desktop card with staggered entrance animation ── */

function IndexCard({ index, delay }: IndexCardProps) {
  const navigate = useNavigate();

  return (
    <motion.div
      initial={{ opacity: 0, y: 20 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, delay: delay * 0.1 }}
      className="overflow-hidden rounded-2xl border transition-colors group flex flex-col cursor-pointer"
      style={{
        borderColor: 'var(--color-border-muted)',
        backgroundColor: 'var(--color-bg-card)',
      }}
      onClick={() => navigate(`/market?symbol=^${index.symbol}`)}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-default)')}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = 'var(--color-border-muted)')}
    >
      <IndexCardContent index={index} />
    </motion.div>
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
  const navigate = useNavigate();
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
      {/* Card stack container */}
      <div className="relative overflow-hidden rounded-2xl" style={{ touchAction: 'pan-y' }}>
        {/* Stacked depth cards behind active card */}
        {indices.length > 1 && (
          <>
            {current < indices.length - 2 && (
              <div
                className="absolute inset-x-3 bottom-0 rounded-2xl border"
                style={{
                  borderColor: 'var(--color-border-muted)',
                  backgroundColor: 'var(--color-bg-card)',
                  opacity: 0.25,
                  height: 'calc(100% - 10px)',
                  top: 10,
                }}
              />
            )}
            {current < indices.length - 1 && (
              <div
                className="absolute inset-x-1.5 bottom-0 rounded-2xl border"
                style={{
                  borderColor: 'var(--color-border-muted)',
                  backgroundColor: 'var(--color-bg-card)',
                  opacity: 0.45,
                  height: 'calc(100% - 5px)',
                  top: 5,
                }}
              />
            )}
          </>
        )}

        {/* Active card */}
        <AnimatePresence initial={false} custom={direction} mode="popLayout">
          <motion.div
            key={current}
            custom={direction}
            variants={swipeVariants}
            initial="enter"
            animate="center"
            exit="exit"
            transition={{ type: 'spring', stiffness: 350, damping: 32, mass: 0.8 }}
            drag="x"
            dragConstraints={{ left: 0, right: 0 }}
            dragElastic={0.12}
            onDragEnd={handleDragEnd}
            className="relative overflow-hidden rounded-2xl border cursor-pointer"
            style={{
              borderColor: 'var(--color-border-muted)',
              backgroundColor: 'var(--color-bg-card)',
              touchAction: 'pan-y',
            }}
            onClick={() => navigate(`/market?symbol=^${index.symbol}`)}
          >
            <IndexCardContent index={index} />
          </motion.div>
        </AnimatePresence>
      </div>

      {/* Dot indicators */}
      <div className="flex justify-center items-center gap-1.5 mt-3">
        {indices.map((_, i) => (
          <button
            key={i}
            onClick={() => goTo(i)}
            aria-label={`Show ${indices[i]?.name}`}
            className="rounded-full transition-all duration-200"
            style={{
              width: i === current ? 18 : 6,
              height: 6,
              backgroundColor:
                i === current
                  ? 'var(--color-accent-primary)'
                  : 'var(--color-border-default)',
            }}
          />
        ))}
      </div>
    </div>
  );
}

/* ── Skeleton loader (single card on mobile, grid on desktop) ── */

function IndexSkeleton({ count }: { count: number }) {
  return (
    <>
      {Array.from({ length: count }).map((_, i) => (
        <div
          key={i}
          className="flex flex-col rounded-2xl animate-pulse border"
          style={{
            backgroundColor: 'var(--color-bg-card)',
            borderColor: 'var(--color-border-muted)',
          }}
        >
          <div className="p-4 pb-0">
            <div className="flex justify-between">
              <div>
                <div
                  className="h-4 rounded mb-1"
                  style={{ backgroundColor: 'var(--color-border-default)', width: 80 }}
                />
                <div
                  className="h-3 rounded"
                  style={{ backgroundColor: 'var(--color-border-default)', width: 40 }}
                />
              </div>
              <div className="text-right">
                <div
                  className="h-5 rounded mb-1"
                  style={{ backgroundColor: 'var(--color-border-default)', width: 80 }}
                />
                <div
                  className="h-3 rounded"
                  style={{ backgroundColor: 'var(--color-border-default)', width: 60 }}
                />
              </div>
            </div>
          </div>
          <div className="mt-2 px-1 pb-2 [&_*]:outline-none" style={{ height: 100 }}>
            <div
              className="w-full h-full rounded"
              style={{ backgroundColor: 'var(--color-border-default)', opacity: 0.3 }}
            />
          </div>
        </div>
      ))}
    </>
  );
}

/* ── Main export ── */

function IndexMovementCard({
  indices = [],
  loading = false,
  forceMobile = false,
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
    <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-5 gap-4">
      {loading ? (
        <IndexSkeleton count={5} />
      ) : (
        indices.map((index, i) => (
          <IndexCard key={index.symbol} index={index} delay={i} />
        ))
      )}
    </div>
  );
}

export default IndexMovementCard;
