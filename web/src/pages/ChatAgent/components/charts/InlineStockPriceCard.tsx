import React from 'react';
import { useTranslation } from 'react-i18next';
import { formatMoney } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { compactNumber, formatCalendarDayRange, formatCalendarMonthDay, grouped2 } from '@/lib/format';
import { readStockPrices, type InlineCardProps } from './cardReaders';
import { FreshnessBadge } from './FreshnessBadge';
import { TermCard, TermChange, TermPriceLine, TermStats } from './TermCard';
import { changeDirection } from './termChange';
import { useCardNarrow } from './termContext';

function downsample<T>(arr: readonly T[], maxPoints = 60): readonly T[] {
  if (arr.length <= maxPoints) return arr;
  const step = arr.length / maxPoints;
  const result: T[] = [];
  for (let i = 0; i < maxPoints; i++) {
    result.push(arr[Math.floor(i * step)]);
  }
  // Always include last point
  if (result[result.length - 1] !== arr[arr.length - 1]) {
    result.push(arr[arr.length - 1]);
  }
  return result;
}

const LINE_COLOR = {
  up: 'var(--color-profit)',
  down: 'var(--color-loss)',
  flat: 'var(--color-term-text-secondary)',
} as const;

export function InlineStockPriceCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const locale = useLocale();
  const narrow = useCardNarrow();
  const prices = readStockPrices(artifact);
  const { symbol, days, sent } = prices;
  if (!sent) return null;

  const closes = downsample(days).map((bar) => bar.close);
  const money = (n: number): string => formatMoney(n, prices.currency, locale);
  const { first, mid, last } = sent;

  return (
    <TermCard title={symbol} meta={first && last ? formatCalendarDayRange(first, last, locale) : undefined} onClick={onClick}>
      <div className="term-line term-plot-lead">
        {sent.lastClose != null && <span className="term-big">{money(sent.lastClose)}</span>}
        <TermChange value={prices.changePct} />
        {/* Publisher and lag of the plotted series live behind this tag. */}
        {prices.freshness && <FreshnessBadge freshness={prices.freshness} source={prices.source} chartSymbol={symbol ?? ''} />}
      </div>
      {closes.length > 1 && (
        <TermPriceLine
          values={closes}
          color={LINE_COLOR[changeDirection(prices.changePct)]}
          height={narrow ? 44 : 60}
          axis={[grouped2(Math.max(...closes), locale), grouped2(Math.min(...closes), locale)]}
          dates={[first, mid, last].map((day) => formatCalendarMonthDay(day, locale))}
        />
      )}
      <div className="term-plot-foot">
        <TermStats
          stats={[
            prices.high != null && [t('toolArtifact.high'), money(prices.high)],
            prices.low != null && [t('toolArtifact.low'), money(prices.low)],
            prices.avgVolume != null && [t('toolArtifact.vol'), compactNumber(prices.avgVolume, locale)],
          ]}
        />
      </div>
    </TermCard>
  );
}
