/**
 * The ordered lists the Desktop app section edits: the items beside the menu
 * bar icon and the panel's tiles as chips, the panel's sections as rows, each
 * dragged into order (`SortableArea`).
 */
import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { GripVertical, Plus, X } from 'lucide-react';
import { arrayMove } from '@dnd-kit/sortable';
import type { UniqueIdentifier } from '@dnd-kit/core';
import { Popover, PopoverContent, PopoverTrigger } from '@/components/ui/popover';
import { CheckboxChoice } from '@/components/ui/choice';
import { SortableArea, useSortableItem } from '@/components/ui/sortable';
import { SymbolSearch } from '@/components/SymbolSearch';
import type { MenubarItem, PanelSectionId } from '@/lib/desktop';
import { PORTFOLIO_ITEM, hasItem, itemKey, itemLabel, quoteRef, withItem } from '@/lib/desktopSettings';
import { cn } from '@/lib/utils';

/**
 * Chips in the order they are shown, each removable, with an add chip that
 * opens the app's symbol search (and offers the portfolio when it may be
 * added and is not there yet). A full list says so in place of the add chip.
 */
export function ItemChips<T extends MenubarItem>({
  items,
  onChange,
  max,
  allowPortfolio = false,
  addLabel,
  labelledBy,
}: {
  items: readonly T[];
  onChange: (items: T[]) => void;
  max: number;
  /** The portfolio may be one of the items (the menu bar), not only symbols (the tiles). */
  allowPortfolio?: boolean;
  addLabel: string;
  labelledBy: string;
}) {
  const { t } = useTranslation();
  const [adding, setAdding] = useState(false);
  const ids = items.map(itemKey);
  const nameOf = (id: UniqueIdentifier) => {
    const item = items.find((it) => itemKey(it) === String(id));
    return item ? itemLabel(item, t) : '';
  };
  const add = (item: T) => {
    setAdding(false);
    const next = withItem(items, item, max);
    if (next.length !== items.length) onChange(next);
  };
  const offerPortfolio = allowPortfolio && !hasItem(items, PORTFOLIO_ITEM);

  return (
    <SortableArea ids={ids} nameOf={nameOf} onMove={(from, to) => onChange(arrayMove([...items], from, to))}>
      <ul aria-labelledby={labelledBy} className="flex flex-wrap items-center justify-end gap-1.5">
        {items.map((item, index) => (
          <ItemChip
            key={ids[index]}
            id={ids[index]}
            label={itemLabel(item, t)}
            onRemove={() => onChange(items.filter((_, i) => i !== index))}
          />
        ))}
        <li>
          {items.length >= max ? (
            <span className="px-1 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('settings.desktop.itemsFull', { count: max })}
            </span>
          ) : (
            <Popover open={adding} onOpenChange={setAdding}>
              <PopoverTrigger asChild>
                <button
                  type="button"
                  data-search-focus=""
                  className="rings-within inline-flex h-7 items-center gap-1 rounded-full border border-dashed border-(--color-border-default) px-2.5 text-[0.8125rem] text-(--color-text-secondary) hover:text-(--color-text-primary) press"
                >
                  <Plus className="h-3.5 w-3.5" aria-hidden="true" />
                  {addLabel}
                </button>
              </PopoverTrigger>
              <PopoverContent align="end" sideOffset={6} className="w-72 p-2">
                {offerPortfolio && (
                  <button
                    type="button"
                    onClick={() => add(PORTFOLIO_ITEM as T)}
                    className="mb-1.5 flex w-full items-center rounded-md px-2 py-1.5 text-left text-[0.8125rem] text-(--color-text-primary) hover:bg-(--color-bg-hover)"
                  >
                    {t('settings.desktop.addPortfolio')}
                  </button>
                )}
                <SymbolSearch onPick={(symbol) => add(quoteRef(symbol) as T)} />
              </PopoverContent>
            </Popover>
          )}
        </li>
      </ul>
    </SortableArea>
  );
}

function ItemChip({ id, label, onRemove }: { id: string; label: string; onRemove: () => void }) {
  const { t } = useTranslation();
  const { isDragging, item, handle } = useSortableItem(id, label);
  return (
    <li
      {...item}
      data-testid={`desktop-chip-${id}`}
      className={cn(
        'inline-flex h-7 items-center rounded-full border border-(--color-border-muted) bg-(--color-bg-card) text-[0.8125rem] text-(--color-text-primary)',
        isDragging && 'relative z-10 shadow-md',
      )}
    >
      <span
        {...handle}
        className="rings-within cursor-grab touch-none rounded-full py-1 pr-1 pl-2.5 whitespace-nowrap active:cursor-grabbing"
      >
        {label}
      </span>
      <button
        type="button"
        onClick={onRemove}
        aria-label={t('settings.desktop.removeItem', { name: label })}
        className="tap-target mr-1 flex h-5 w-5 items-center justify-center rounded-full text-(--color-text-tertiary) hover:bg-(--color-bg-hover) hover:text-(--color-text-primary)"
      >
        <X className="h-3 w-3" aria-hidden="true" />
      </button>
    </li>
  );
}

/** The panel's sections in order, each with a box that shows or hides it. */
export function SectionList({
  sections,
  onChange,
  labelledBy,
}: {
  sections: ReadonlyArray<{ id: PanelSectionId; visible: boolean }>;
  onChange: (sections: Array<{ id: PanelSectionId; visible: boolean }>) => void;
  labelledBy: string;
}) {
  const { t } = useTranslation();
  const ids = sections.map((s) => s.id);
  const nameOf = (id: UniqueIdentifier) => t(`settings.desktop.section.${String(id)}`);
  return (
    <SortableArea ids={ids} nameOf={nameOf} column onMove={(from, to) => onChange(arrayMove([...sections], from, to))}>
      <ul aria-labelledby={labelledBy} className="flex min-w-40 flex-col gap-0.5">
        {sections.map((section, index) => (
          <SectionRow
            key={section.id}
            id={section.id}
            name={nameOf(section.id)}
            visible={section.visible}
            onVisible={(visible) => onChange(sections.map((s, i) => (i === index ? { ...s, visible } : s)))}
          />
        ))}
      </ul>
    </SortableArea>
  );
}

function SectionRow({ id, name, visible, onVisible }: {
  id: PanelSectionId;
  name: string;
  visible: boolean;
  onVisible: (visible: boolean) => void;
}) {
  const { isDragging, item, handle } = useSortableItem(id, name);
  return (
    <li
      {...item}
      data-testid={`desktop-section-${id}`}
      className={cn(
        'flex items-center gap-1 rounded-md bg-(--color-bg-card) py-0.5 pr-2',
        isDragging && 'relative z-10 shadow-md',
      )}
    >
      <span
        {...handle}
        className="rings-within flex h-7 w-6 shrink-0 cursor-grab touch-none items-center justify-center rounded-md text-(--color-text-quaternary) hover:text-(--color-text-secondary) active:cursor-grabbing"
      >
        <GripVertical className="h-4 w-4" aria-hidden="true" />
      </span>
      <CheckboxChoice checked={visible} onCheckedChange={onVisible} size="md" className="flex-1 items-center gap-2 rounded-md py-1 pl-0.5 text-[0.8125rem]">
        <span style={{ color: 'var(--color-text-primary)' }}>{name}</span>
      </CheckboxChoice>
    </li>
  );
}
