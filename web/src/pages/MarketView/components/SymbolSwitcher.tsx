import React, { useRef, useState } from 'react';
import { ChevronDown } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { SymbolSearch } from '@/components/SymbolSearch';
import type { StockSearchHit } from '@/lib/marketUtils';
import './StockHeader.css';

interface SymbolSwitcherProps {
  symbol: string;
  onPick: (symbol: string, hit?: StockSearchHit) => void;
}

/** The ticker as a control: a button in the header's type, opening the search in a popover. */
export function SymbolSwitcher({ symbol, onPick }: SymbolSwitcherProps): React.ReactElement {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  // A pick closes the popover and the chart moves on to the new symbol; the
  // ticker button should not take the focus back and light up under the new
  // name. Escape and an outside click still return it, as a dismissal should.
  const pickedRef = useRef(false);

  return (
    <Popover open={open} onOpenChange={setOpen}>
      <PopoverTrigger asChild>
        <button
          type="button"
          className="stock-symbol stock-symbol-btn"
          title={t('marketView.symbolSearch.changeSymbol')}
          aria-label={t('marketView.symbolSearch.switchSymbol', { symbol })}
        >
          {symbol}
          <ChevronDown size={14} aria-hidden="true" />
        </button>
      </PopoverTrigger>
      <PopoverContent
        align="start"
        sideOffset={6}
        className="w-72 p-2"
        onCloseAutoFocus={(e) => { if (pickedRef.current) { pickedRef.current = false; e.preventDefault(); } }}
      >
        <SymbolSearch onPick={(next, hit) => { pickedRef.current = true; setOpen(false); onPick(next, hit); }} />
      </PopoverContent>
    </Popover>
  );
}
