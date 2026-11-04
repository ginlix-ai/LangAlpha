/**
 * The extended-hours print beside a headline: session icon, price, change
 * and percent in the session's color. StockHeader and the legend lead both
 * show it, in their own type size, so the pair is composed once here and
 * the two cannot drift on which figures they print.
 */

import React from 'react';
import { Sunrise, Sunset } from 'lucide-react';
import { signedFixed2 } from '@/lib/format';
import { fixedUpTo } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import type { StockQuoteModel } from '../hooks/useStockQuoteModel';

interface ExtendedHoursPairProps {
  ext: NonNullable<StockQuoteModel['ext']>;
  places: number;
  iconSize: number;
  className?: string;
  style?: React.CSSProperties;
}

export function ExtendedHoursPair({ ext, places, iconSize, className, style }: ExtendedHoursPairProps): React.ReactElement {
  const locale = useLocale();
  return (
    <span className={className} style={{ ...style, color: ext.type === 'pre' ? 'var(--color-ext-pre)' : 'var(--color-ext-post)' }}>
      {ext.type === 'pre' ? <Sunrise size={iconSize} /> : <Sunset size={iconSize} />}
      {fixedUpTo(ext.price, places, locale)}
      {ext.change != null && <span>{fixedUpTo(ext.change, places, locale, { signed: true })}</span>}
      <span>({signedFixed2(ext.pct, locale)}%)</span>
    </span>
  );
}
