import React, { useState } from 'react';
import { List } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { cn } from '@/lib/utils';
import { ChartToolButton } from '../ChartToolButton';
import { QuoteList } from './QuoteList';
import { ADD_PANEL_ATTR } from './QuoteListAdd';
import { sameListing } from './quoteRows';

interface QuoteListDrawerProps {
  /** The chart's symbol, marked in the list. */
  symbol: string;
  onPick: (symbol: string) => void;
}

/**
 * The watchlist and holdings over a chart, on the market page and in a
 * thread's chart tab alike: a toolbar button opens the list over the chart's
 * right side, so the chart keeps its whole width, and a pick moves the chart
 * and puts the list away. It mounts the list only while
 * open, so a closed drawer costs the chart no quotes and no socket symbols.
 * It must render inside the chart's `MarketDataWSProvider`, as a toolbar
 * action does.
 */
export function QuoteListDrawer({ symbol, onPick }: QuoteListDrawerProps): React.ReactElement {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const label = t('marketView:quoteList.openList');

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <ChartToolButton title={label} className={cn(open && 'chart-tool-btn-active')}>
          <List size={14} />
        </ChartToolButton>
      </PopoverTrigger>
      <PopoverContent
        align="end"
        sideOffset={6}
        collisionPadding={8}
        // A short list still opens at a settled height, a long one scrolls
        // past 480px, and neither runs past the room under the button.
        className="flex w-[280px] flex-col overflow-hidden p-0"
        style={{
          minHeight: 'min(340px, var(--radix-popover-content-available-height))',
          maxHeight: 'min(480px, var(--radix-popover-content-available-height))',
        }}
        aria-label={label}
        // Escape inside the add search backs out of the search, not the list.
        onEscapeKeyDown={(e) => {
          if (e.target instanceof Element && e.target.closest(`[${ADD_PANEL_ATTR}]`)) e.preventDefault();
        }}
      >
        <QuoteList
          activeSymbol={symbol}
          onPick={(next) => {
            setOpen(false);
            if (!sameListing(next, symbol)) onPick(next);
          }}
        />
      </PopoverContent>
    </Popover>
  );
}
