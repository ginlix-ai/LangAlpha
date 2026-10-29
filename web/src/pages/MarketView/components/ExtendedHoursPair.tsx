/**
 * The extended-hours print beside a headline: session icon, price, change
 * and percent in the session's color. StockHeader and the legend lead both
 * show it, in their own type size, so the pair is composed once here and
 * the two cannot drift on which figures they print.
 */

import React from 'react';
import { Sunrise, Sunset } from 'lucide-react';
import { fixed2, signedFixed2 } from '@/lib/format';
import { useLocale } from '@/hooks/useLocale';
import { EXT_COLOR_PRE, EXT_COLOR_POST } from '../utils/chartConstants';
import type { StockQuoteModel } from '../hooks/useStockQuoteModel';

interface ExtendedHoursPairProps {
  ext: NonNullable<StockQuoteModel['ext']>;
  iconSize: number;
  className?: string;
  style?: React.CSSProperties;
}

export function ExtendedHoursPair({ ext, iconSize, className, style }: ExtendedHoursPairProps): React.ReactElement {
  const locale = useLocale();
  return (
    <span className={className} style={{ ...style, color: ext.type === 'pre' ? EXT_COLOR_PRE : EXT_COLOR_POST }}>
      {ext.type === 'pre' ? <Sunrise size={iconSize} /> : <Sunset size={iconSize} />}
      {fixed2(ext.price, locale)}
      {ext.change != null && <span>{signedFixed2(ext.change, locale)}</span>}
      <span>({signedFixed2(ext.pct, locale)}%)</span>
    </span>
  );
}
