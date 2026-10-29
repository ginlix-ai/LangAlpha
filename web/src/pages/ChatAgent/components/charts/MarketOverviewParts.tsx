import React, { useEffect, useMemo, useRef } from 'react';
import { CandlestickSeries } from 'lightweight-charts';
import { useTranslation } from 'react-i18next';
import type { ChartBar } from '@/lib/bars';
import { grouped2 } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { canvasChartOptions, candleData, candleOptions, mountCanvasChart, useCanvasTheme } from './canvasChart';
import { indexRows, sectorRows, type PanelProps } from './cardReaders';
import { FreshnessBadge } from './FreshnessBadge';
import { IndexTable } from './InlineMarketCards';
import { MarketLink } from './marketLinks';
import { TermBars, TermCard, TermStats } from './TermCard';
import { commonFreshness } from './termFreshness';

// The legacy `market_indices` and `sector_performance` panels, which the
// composite market overview draws under one head.

export function SectorPerformanceChart({ data }: PanelProps): React.ReactElement {
  const { t } = useTranslation();
  const rows = sectorRows(data);
  return rows.length ? (
    <TermCard title={t('toolArtifact.sectorPerformance')}>
      <TermBars rows={rows} />
    </TermCard>
  ) : (
    <div className="term-t3" style={{ padding: 16 }}>{t('toolArtifact.noSectorData')}</div>
  );
}

/**
 * The indices card a size up: its table, how current it is said once on the
 * edge, then each index's bars in a frame of its own.
 */
export function MarketIndicesChart({ data }: PanelProps): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  // Held across renders, so each index's chart is not drawn again.
  const rows = useMemo(() => indexRows(data), [data]);
  if (!rows.length) {
    return <div className="term-t3" style={{ padding: 16 }}>{t('toolArtifact.noIndexData')}</div>;
  }

  const { common } = commonFreshness(rows);

  return (
    <>
      <TermCard
        title={t('toolArtifact.marketIndices')}
        meta={common ? <FreshnessBadge tier={common.tier} freshness={common.freshness} source={common.source} /> : undefined}
      >
        <IndexTable rows={rows} />
      </TermCard>

      {rows.map((row) => {
        const num = (key: string) => row.stats?.[key] as number | null | undefined;
        const ma20 = num('ma_20');
        const ma50 = num('ma_50');
        const volatility = num('volatility');
        return (
          <TermCard key={row.sym} title={row.name} sub={row.name !== row.sym ? row.sym : undefined}>
            <MiniCandlestick bars={row.bars} intraday={row.intraday} height={160} />
            {/* The link sits under the bars, so a long name keeps the edge. */}
            <div className="term-line" style={{ marginTop: 8 }}>
              <TermStats
                stats={[
                  ma20 != null && ['MA20', grouped2(ma20, locale)],
                  ma50 != null && ['MA50', grouped2(ma50, locale)],
                  volatility != null && [t('toolArtifact.volatility'), `${(volatility * 100).toFixed(1)}%`],
                ]}
              />
              <span style={{ marginLeft: 'auto' }}>
                <MarketLink symbol={row.sym} />
              </span>
            </div>
          </TermCard>
        );
      })}
    </>
  );
}

function MiniCandlestick({ bars, intraday, height }: { bars: ChartBar[]; intraday: boolean; height: number }): React.ReactElement | null {
  const ct = useCanvasTheme();
  const containerRef = useRef<HTMLDivElement>(null);

  useEffect(() => {
    const el = containerRef.current;
    if (!el || !bars.length) return;
    const { chart, dispose } = mountCanvasChart(el, height, canvasChartOptions(ct, intraday));
    chart.addSeries(CandlestickSeries, candleOptions(ct)).setData(candleData(bars));
    chart.timeScale().fitContent();
    return dispose;
  }, [bars, intraday, height, ct]);

  if (!bars.length) return null;
  return <div ref={containerRef} style={{ width: '100%', height }} />;
}
