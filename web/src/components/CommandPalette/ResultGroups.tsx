import { useMemo } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronRight, Pin } from 'lucide-react';
import { quoteKey, useQuotes } from '@/lib/quotes';
import { cn } from '@/lib/utils';
import { PaletteRow, QuoteFigures } from './PaletteRow';
import type { Lens, PaletteGroup, PaletteItem } from './types';

interface ResultGroupsProps {
  groups: PaletteGroup[];
  /** Every row in order, which the selection indexes. */
  flat: PaletteItem[];
  index: number;
  optionId: (index: number) => string;
  /** Opens a group alone from its header. */
  onOpen: (lens: Lens) => void;
  onHover: (item: PaletteItem) => void;
  onPick: (item: PaletteItem) => void;
  onMore: (item: PaletteItem) => void;
}

/** The results, in their groups, each ticker with its price and change, and each pinned thread with its pin. */
export function ResultGroups({ groups, flat, index, optionId, onOpen, onHover, onPick, onMore }: ResultGroupsProps) {
  const { t } = useTranslation();
  const symbols = useMemo(() => flat.flatMap((item) => (item.symbol ? [item.symbol] : [])), [flat]);
  const { quotes } = useQuotes(symbols, { refetchInterval: false });

  return groups.map((group) => {
    const open = group.open;
    return (
      <div key={group.id} role="group" aria-label={group.label}>
        {/* For a pointer, the header opens its group alone; the keyboard has
            the group's Show all row. */}
        <div
          className={cn('command-palette-group', open && 'is-drillable')}
          aria-hidden
          onMouseDown={(e) => e.preventDefault()}
          onClick={open ? () => onOpen(open) : undefined}
        >
          {group.label}
          {open && <ChevronRight aria-hidden />}
        </div>
        {group.items.map((item) => {
          const i = flat.indexOf(item);
          const quote = item.symbol ? quotes[quoteKey(item.symbol)] : undefined;
          return (
            <PaletteRow
              key={item.id}
              id={optionId(i)}
              index={i}
              active={i === index}
              icon={item.icon}
              accent={item.accent}
              symbol={item.symbol}
              title={item.title || (quote?.name ?? '')}
              spans={item.spans}
              meta={item.meta}
              detail={item.detail}
              trailing={
                item.symbol ? (
                  <QuoteFigures symbol={item.symbol} quote={quote} className="command-palette-trailing command-palette-quote" />
                ) : (
                  item.pinned && (
                    // The sidebar's resting pinned marker.
                    <span className="command-palette-trailing command-palette-pin" role="img" aria-label={t('palette.pinned')}>
                      <Pin aria-hidden fill="currentColor" />
                    </span>
                  )
                )
              }
              moreLabel={t('palette.do.actions')}
              onMore={item.actions.length > 1 ? () => onMore(item) : undefined}
              onHover={() => onHover(item)}
              onPick={() => onPick(item)}
            />
          );
        })}
      </div>
    );
  });
}
