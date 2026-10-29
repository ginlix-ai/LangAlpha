import React from 'react';
import { useTranslation } from 'react-i18next';
import { useLocale } from '@/hooks/useLocale';
import { fixed2, signedFixed2 } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { MeterReading } from '../utils/price';
import './PriceMeter.css';

function formatValue(reading: MeterReading, v: number, locale: string): string {
  return reading.unit === 'pct' ? `${signedFixed2(v, locale)}%` : fixed2(v, locale);
}

/**
 * Where the watched value sits relative to the line that fires the run. The
 * scale is fitted to the two points with room either side, so the distance
 * reads the same whether the stock trades at 8 or 800; the numbers underneath
 * carry the absolute values.
 */
export function PriceMeter({ reading, symbol }: { reading: MeterReading; symbol: string }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { current, threshold } = reading;
  const span = Math.abs(threshold - current);
  const floor = reading.unit === 'pct' ? 0.5 : Math.abs(current) * 0.01;
  const pad = Math.max(span * 0.4, floor);
  const lo = Math.min(current, threshold) - pad;
  const hi = Math.max(current, threshold) + pad;
  const pos = (v: number) => ((v - lo) / (hi - lo)) * 100;
  const cur = pos(current);
  const thr = pos(threshold);
  const met = reading.remaining <= 0;

  return (
    <div className="automation-meter">
      <div className="automation-meter-track" aria-hidden="true">
        {!met && (
          <span
            className="automation-meter-gap"
            style={{ left: `${Math.min(cur, thr)}%`, width: `${Math.abs(thr - cur)}%` }}
          />
        )}
        <span className="automation-meter-threshold" style={{ left: `${thr}%` }} />
        <span className="automation-meter-dot" style={{ left: `${cur}%` }} />
      </div>
      {/* Each number sits at the end of the track its mark is on. */}
      <div className={cn('automation-mono automation-meter-scale', cur > thr && 'flex-row-reverse')}>
        <span>
          <span style={{ color: 'var(--color-text-secondary)' }}>{symbol}</span> {formatValue(reading, current, locale)}
        </span>
        <span>{t('automation.firesAt', { value: formatValue(reading, threshold, locale) })}</span>
      </div>
    </div>
  );
}
