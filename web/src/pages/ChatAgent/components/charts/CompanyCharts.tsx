/**
 * The company charts Market View's company page draws. The detail panel shares
 * the quarterly series and the revenue breakdown, which draw in its frames and
 * type there (`useChartSkin`); performance and ratings it draws as bars of its
 * own (CompanyOverviewDetail).
 */
import React, { memo, useMemo } from 'react';
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip,
  PieChart, Pie, Cell, Legend,
  LineChart, Line, ReferenceLine,
} from 'recharts';
import { useTranslation } from 'react-i18next';
import { formatMoney } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { ratingCounts } from './cardReaders';
import { formatPct, performancePoints } from './cardWords';
import { ChartFrame } from './TermPanel';
import { useChartSkin, useTermPanel } from './termContext';

// CSS-variable colors for recharts (SVG) and DOM elements
const TEXT_COLOR = 'var(--color-text-secondary)';
const GREEN = 'var(--color-profit)';
const RED = 'var(--color-loss)';
const AMBER = '#f59e0b';

const ANALYST_COLORS: Record<string, string> = {
  strongBuy: 'var(--color-profit)',
  buy: '#34d399',
  hold: '#f59e0b',
  sell: '#f87171',
  strongSell: 'var(--color-loss)',
};

// Custom tooltip for dark theme
interface DarkTooltipProps {
  active?: boolean;
  payload?: Array<{ color?: string; value: number }>;
  label?: string;
  formatter?: (value: number) => string;
}

const DarkTooltip = ({ active, payload, label, formatter }: DarkTooltipProps): React.ReactElement | null => {
  const term = useTermPanel();
  if (!active || !payload?.length) return null;
  if (term) {
    return (
      <div className="term-tooltip">
        <p>{label}</p>
        {payload.map((entry, i) => (
          <p key={i} style={{ color: entry.color || 'var(--color-term-text)' }}>
            {formatter ? formatter(entry.value) : entry.value}
          </p>
        ))}
      </div>
    );
  }
  return (
    <div style={{ background: 'var(--color-bg-card)', border: '1px solid var(--color-border-muted)', borderRadius: 6, padding: '8px 12px' }}>
      <p style={{ color: TEXT_COLOR, fontSize: '0.75rem', margin: 0 }}>{label}</p>
      {payload.map((entry, i) => (
        <p key={i} style={{ color: entry.color || 'var(--color-text-primary)', fontSize: '0.75rem', margin: '2px 0 0' }}>
          {formatter ? formatter(entry.value) : entry.value}
        </p>
      ))}
    </div>
  );
};

// ─── PerformanceBarChart ────────────────────────────────────────────

export const PerformanceBarChart = memo(function PerformanceBarChart({ performance }: { performance: Record<string, number> | undefined }): React.ReactElement | null {
  const { t } = useTranslation();
  const skin = useChartSkin();
  const chartData = useMemo(
    () => performancePoints(performance).map((d) => ({ ...d, fill: d.value >= 0 ? GREEN : RED })),
    [performance],
  );

  if (chartData.length === 0) return null;

  return (
    <ChartFrame title={t('toolArtifact.pricePerformance')}>
      <BarChart responsive width="100%" height={180} data={chartData} margin={{ left: 0, right: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={skin.grid} vertical={false} />
        <XAxis
          dataKey="name"
          tick={skin.tick}
          axisLine={{ stroke: skin.axis }}
        />
        <YAxis
          width="auto"
          tick={skin.tick}
          axisLine={{ stroke: skin.axis }}
          tickFormatter={(v: number) => `${v.toFixed(0)}%`}
        />
        <Tooltip content={<DarkTooltip formatter={(v: number) => formatPct(v)} />} />
        <Bar dataKey="value" radius={skin.bar}>
          {chartData.map((entry, i) => (
            <Cell key={i} fill={entry.fill} />
          ))}
        </Bar>
      </BarChart>
    </ChartFrame>
  );
});

// ─── AnalystRatingsChart ────────────────────────────────────────────

export const AnalystRatingsChart = memo(function AnalystRatingsChart({ ratings }: { ratings: Record<string, unknown> | undefined }): React.ReactElement | null {
  const { t } = useTranslation();
  const skin = useChartSkin();

  const { chartData, total } = useMemo(() => {
    if (!ratings) return { chartData: [], total: 0 };
    const cd = ratingCounts(ratings)
      .filter((d) => d.value > 0)
      .map((d) => ({ ...d, name: t(`toolArtifact.ratings.${d.key}`) }));
    return { chartData: cd, total: cd.reduce((s, d) => s + d.value, 0) };
  }, [ratings, t]);

  if (!ratings || chartData.length === 0) return null;

  return (
    <ChartFrame title={t('toolArtifact.analystRatings')}>
      <div style={{ position: 'relative' }}>
        <PieChart responsive width="100%" height={200}>
          <Pie
            data={chartData}
            cx="50%"
            cy="50%"
            innerRadius={50}
            outerRadius={75}
            dataKey="value"
            stroke="none"
            strokeWidth={2}
          >
            {chartData.map((entry) => (
              <Cell key={entry.key} fill={ANALYST_COLORS[entry.key] || 'var(--color-icon-muted)'} />
            ))}
          </Pie>
          <Legend wrapperStyle={{ fontSize: 11 }} formatter={skin.legend} />
          <Tooltip content={<DarkTooltip formatter={(v: number) => `${v} (${((v / total) * 100).toFixed(0)}%)`} />} />
        </PieChart>
        {/* Center consensus label */}
        <div
          style={{
            position: 'absolute',
            top: '38%',
            left: '50%',
            transform: 'translate(-50%, -50%)',
            textAlign: 'center',
          }}
        >
          <div style={{ fontSize: '0.875rem', fontWeight: 700, color: 'var(--color-text-primary)', textTransform: 'uppercase' }}>
            {(ratings.consensus as string) || ''}
          </div>
          <div style={{ fontSize: '0.6875rem', color: TEXT_COLOR }}>{t('toolArtifact.nRatings', { count: total })}</div>
        </div>
      </div>
    </ChartFrame>
  );
});

// ─── RevenueBreakdownChart ──────────────────────────────────────────

interface RevenueBreakdownChartProps {
  revenueByProduct: Record<string, number> | undefined;
  revenueByGeo: Record<string, number> | undefined;
  /** ISO code of the slice values. */
  currency: string;
}

const buildPieData = (obj: Record<string, number>) => {
  return Object.entries(obj)
    .map(([name, value]) => ({ name, value }))
    .sort((a, b) => b.value - a.value);
};

export const RevenueBreakdownChart = memo(function RevenueBreakdownChart({ revenueByProduct, revenueByGeo, currency }: RevenueBreakdownChartProps): React.ReactElement | null {
  const locale = useLocale();
  const { t } = useTranslation();
  const skin = useChartSkin();
  const hasProduct = revenueByProduct && Object.keys(revenueByProduct).length > 0;
  const hasGeo = revenueByGeo && Object.keys(revenueByGeo).length > 0;

  if (!hasProduct && !hasGeo) return null;

  const renderPie = (data: Array<{ name: string; value: number }>, title: string) => {
    const total = data.reduce((s, d) => s + d.value, 0);
    return (
      <div style={{ flex: 1, minWidth: 200 }}>
        <h5 className="term-word" style={{ color: TEXT_COLOR, fontSize: '0.75rem', fontWeight: 500, marginBottom: 4 }}>
          {title}
        </h5>
        <PieChart responsive width="100%" height={180}>
          <Pie
            data={data}
            cx="50%"
            cy="50%"
            innerRadius={35}
            outerRadius={55}
            dataKey="value"
            stroke="none"
          >
            {data.map((_, i) => (
              <Cell key={i} fill={skin.slices[i % skin.slices.length]} />
            ))}
          </Pie>
          <Legend wrapperStyle={{ fontSize: 10 }} formatter={skin.legend} />
          <Tooltip
            content={<DarkTooltip formatter={(v: number) => `${formatMoney(v, currency, locale, { compact: true })} (${((v / total) * 100).toFixed(1)}%)`} />}
          />
        </PieChart>
      </div>
    );
  };

  return (
    <ChartFrame title={t('toolArtifact.revenueBreakdown')}>
      <div style={{ display: 'flex', gap: 16, flexWrap: 'wrap' }}>
        {hasProduct && renderPie(buildPieData(revenueByProduct), t('toolArtifact.byProduct'))}
        {hasGeo && renderPie(buildPieData(revenueByGeo), t('toolArtifact.byGeography'))}
      </div>
    </ChartFrame>
  );
});

// ─── QuarterlyRevenueChart ───────────────────────────────────────────

interface ChartArrayDataProps {
  data: Record<string, unknown>[] | undefined;
  /** ISO code of money-valued axes and tooltips; required so a forgotten
   *  prop cannot silently read as USD. */
  currency: string;
}

export const QuarterlyRevenueChart = memo(function QuarterlyRevenueChart({ data, currency }: ChartArrayDataProps): React.ReactElement | null {
  const locale = useLocale();
  const { t } = useTranslation();
  const skin = useChartSkin();
  if (!data?.length) return null;

  return (
    <ChartFrame title={t('toolArtifact.quarterlyRevenue')}>
      <BarChart responsive width="100%" height={220} data={data} margin={{ left: 0, right: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={skin.grid} vertical={false} />
        <XAxis dataKey="period" tick={{ ...skin.tick, fontSize: 10 }} axisLine={{ stroke: skin.axis }} />
        <YAxis width="auto" tick={skin.tick} axisLine={{ stroke: skin.axis }} tickFormatter={(v: number) => formatMoney(v, currency, locale, { compact: true })} />
        <Tooltip content={<DarkTooltip formatter={(v: number) => formatMoney(v, currency, locale, { compact: true })} />} />
        <Legend wrapperStyle={{ fontSize: 11 }} formatter={skin.legend} />
        <Bar dataKey="revenue" name={t('toolArtifact.revenue')} fill={skin.primary} radius={skin.bar} />
        <Bar dataKey="netIncome" name={t('toolArtifact.netIncome')} fill={GREEN} radius={skin.bar} />
      </BarChart>
    </ChartFrame>
  );
});

// ─── MarginsChart ───────────────────────────────────────────────────

export const MarginsChart = memo(function MarginsChart({ data }: Pick<ChartArrayDataProps, 'data'>): React.ReactElement | null {
  const { t } = useTranslation();
  const skin = useChartSkin();

  const chartData = useMemo(() => {
    if (!data?.length) return [];
    return data.map((d) => ({
      period: d.period as string,
      grossMargin: (d.grossMargin as number | undefined) != null ? (d.grossMargin as number) * 100 : null,
      operatingMargin: (d.operatingMargin as number | undefined) != null ? (d.operatingMargin as number) * 100 : null,
      netMargin: (d.netMargin as number | undefined) != null ? (d.netMargin as number) * 100 : null,
    }));
  }, [data]);

  if (!data?.length) return null;

  return (
    <ChartFrame title={t('toolArtifact.profitMargins')}>
      <LineChart responsive width="100%" height={220} data={chartData} margin={{ left: 0, right: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={skin.grid} vertical={false} />
        <XAxis dataKey="period" tick={{ ...skin.tick, fontSize: 10 }} axisLine={{ stroke: skin.axis }} />
        <YAxis width="auto" tick={skin.tick} axisLine={{ stroke: skin.axis }} tickFormatter={(v: number) => `${v.toFixed(0)}%`} />
        <Tooltip content={<DarkTooltip formatter={(v: number) => `${v?.toFixed(1)}%`} />} />
        <Legend wrapperStyle={{ fontSize: 11 }} formatter={skin.legend} />
        <Line type="monotone" dataKey="grossMargin" name={t('toolArtifact.grossMargin')} stroke={skin.primary} strokeWidth={2} dot={{ r: 3 }} connectNulls />
        <Line type="monotone" dataKey="operatingMargin" name={t('toolArtifact.operatingMargin')} stroke={AMBER} strokeWidth={2} dot={{ r: 3 }} connectNulls />
        <Line type="monotone" dataKey="netMargin" name={t('toolArtifact.netMargin')} stroke={GREEN} strokeWidth={2} dot={{ r: 3 }} connectNulls />
      </LineChart>
    </ChartFrame>
  );
});

// ─── EarningsSurpriseChart ──────────────────────────────────────────

export const EarningsSurpriseChart = memo(function EarningsSurpriseChart({ data, currency }: ChartArrayDataProps): React.ReactElement | null {
  const locale = useLocale();
  const { t } = useTranslation();
  const skin = useChartSkin();
  if (!data?.length) return null;

  return (
    <ChartFrame title={t('toolArtifact.epsActualVsEstimate')}>
      <BarChart responsive width="100%" height={220} data={data} margin={{ left: 0, right: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={skin.grid} vertical={false} />
        <XAxis dataKey="period" tick={{ ...skin.tick, fontSize: 10 }} axisLine={{ stroke: skin.axis }} />
        <YAxis width="auto" tick={skin.tick} axisLine={{ stroke: skin.axis }} tickFormatter={(v: number) => formatMoney(v, currency, locale)} />
        <Tooltip content={<DarkTooltip formatter={(v: number) => formatMoney(v, currency, locale)} />} />
        <Legend wrapperStyle={{ fontSize: 11 }} formatter={skin.legend} />
        <Bar dataKey="epsActual" name={t('toolArtifact.epsActual')} fill={GREEN} radius={skin.bar} />
        <Bar dataKey="epsEstimate" name={t('toolArtifact.epsEstimate')} fill="var(--color-icon-muted)" radius={skin.bar} />
      </BarChart>
    </ChartFrame>
  );
});

// ─── CashFlowChart ──────────────────────────────────────────────────

export const CashFlowChart = memo(function CashFlowChart({ data, currency }: ChartArrayDataProps): React.ReactElement | null {
  const locale = useLocale();
  const { t } = useTranslation();
  const skin = useChartSkin();
  if (!data?.length) return null;

  return (
    <ChartFrame title={t('toolArtifact.cashFlowQuarterly')}>
      <BarChart responsive width="100%" height={220} data={data} margin={{ left: 0, right: 10 }}>
        <CartesianGrid strokeDasharray="3 3" stroke={skin.grid} vertical={false} />
        <XAxis dataKey="period" tick={{ ...skin.tick, fontSize: 10 }} axisLine={{ stroke: skin.axis }} />
        <YAxis width="auto" tick={skin.tick} axisLine={{ stroke: skin.axis }} tickFormatter={(v: number) => formatMoney(v, currency, locale, { compact: true })} />
        <Tooltip content={<DarkTooltip formatter={(v: number) => formatMoney(v, currency, locale, { compact: true })} />} />
        <Legend wrapperStyle={{ fontSize: 11 }} formatter={skin.legend} />
        <ReferenceLine y={0} stroke={skin.axis} />
        <Bar dataKey="operatingCashFlow" name={t('toolArtifact.operatingCF')} fill={skin.primary} radius={skin.bar} />
        <Bar dataKey="capitalExpenditure" name={t('toolArtifact.capEx')} fill={RED} radius={skin.bar} />
        <Bar dataKey="freeCashFlow" name={t('toolArtifact.freeCF')} fill={GREEN} radius={skin.bar} />
      </BarChart>
    </ChartFrame>
  );
});
