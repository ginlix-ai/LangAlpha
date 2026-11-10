/**
 * The chart's list of agent layers: a stack of small chips (colour dot, title,
 * as-of) laid in the chart's top-left overlay, each opening a popover with the
 * agent's summary and what the viewer can do with the layer: hide it, remove
 * it, or ask the agent about it. Past `COLLAPSE_AFTER` layers the stack folds to
 * one "N layers" chip whose popover lists them.
 *
 * Hiding is the viewer's own and lasts the session (`layerViewStore`). Removing
 * deletes the stored layer, shown at once and put back if the server refuses
 * (`useRemoveChartLayer`).
 */

import React, { useCallback, useEffect, useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronDown, Eye, EyeOff, Layers, MessageSquare, Trash2 } from 'lucide-react';

import { Popover, PopoverAnchor, PopoverContent } from '@/components/ui/popover';
import { useToast } from '@/components/ui/use-toast';
import { INTERVAL_LABEL } from '@/lib/bars';
import { useLocale } from '@/hooks/useLocale';
import { buttonSlot } from '@/components/ui/button-class';
import { cn } from '@/lib/utils';

import { useChartComposer } from '@/components/chart/chartComposer';

import { useChartLayerRows, useRemoveChartLayer } from '../hooks/useChartLayerQuery';
import { setLayerHidden, useHiddenLayers } from '../stores/layerViewStore';
import type { ChartLayer, StoredChartLayer } from '../types/chartLayer';
import type { ChartKey } from '../utils/chartInstanceKey';
import { formatLayerAsOf, formatLayerAsOfShort, layerAccent } from '../utils/layerDisplay';

/** More layers than this and the stack folds to one chip. */
const COLLAPSE_AFTER = 3;
const HOVER_OPEN_MS = 120;
const HOVER_CLOSE_MS = 180;

/** A layer as the legend shows it: the stored row and whether the viewer hid it. */
interface ChartLayerEntry {
  layer: StoredChartLayer;
  hidden: boolean;
}

const MONO = { fontFamily: 'var(--font-mono)', fontVariantNumeric: 'tabular-nums' } as const;

/**
 * The agent's thesis, scrolling past a few paragraphs. While more is below, the
 * last line fades out, so a cut-off sentence reads as "scroll", not as the end.
 */
function Summary({ text }: { text: string }): React.ReactElement {
  const ref = useRef<HTMLParagraphElement>(null);
  const [more, setMore] = useState(false);
  const measure = useCallback(() => {
    const el = ref.current;
    if (el) setMore(el.scrollHeight - el.scrollTop - el.clientHeight > 2);
  }, []);
  useEffect(measure, [measure, text]);
  return (
    <p
      ref={ref}
      onScroll={measure}
      className={cn(
        'max-h-56 overflow-y-auto overscroll-contain pr-1 whitespace-pre-line text-xs leading-[1.55] text-[var(--color-text-secondary)]',
        more && '[mask-image:linear-gradient(to_bottom,black_calc(100%-2rem),transparent)]',
      )}
    >
      {text}
    </p>
  );
}

function Dot({ layer, hidden }: { layer: ChartLayer; hidden: boolean }): React.ReactElement {
  const color = layerAccent(layer);
  return (
    <span
      aria-hidden="true"
      className="inline-block size-2 shrink-0 rounded-full"
      style={hidden
        ? { boxShadow: 'inset 0 0 0 1.5px var(--color-text-tertiary)' }
        : { backgroundColor: color ?? 'var(--color-text-tertiary)' }}
    />
  );
}

interface LayerActions {
  toggleHidden: (entry: ChartLayerEntry) => void;
  ask: (entry: ChartLayerEntry) => void;
  remove: (entry: ChartLayerEntry) => void;
}

function ActionButton({
  label, onClick, danger, className, children,
}: {
  label: string;
  onClick: () => void;
  danger?: boolean;
  className?: string;
  children: React.ReactNode;
}): React.ReactElement {
  return (
    <button
      type="button"
      onClick={onClick}
      title={label}
      aria-label={label}
      {...buttonSlot('ghost', danger ? 'danger' : undefined)}
      className={cn(
        'press tap-target inline-flex size-7 items-center justify-center rounded-md text-[var(--color-text-secondary)] hover:bg-[var(--color-bg-surface)]',
        danger ? 'hover:text-[var(--color-loss)]' : 'hover:text-[var(--color-text-primary)]',
        className,
      )}
    >
      {children}
    </button>
  );
}

function LayerDetail({ entry, actions }: { entry: ChartLayerEntry; actions: LayerActions }): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const { layer, hidden } = entry;
  const asOf = formatLayerAsOf(layer.layer.as_of, locale);
  const interval = INTERVAL_LABEL[layer.timeframe] ?? layer.timeframe;
  return (
    <div className="flex flex-col gap-2">
      <div className="flex flex-col gap-0.5">
        <span className="text-[0.8125rem] font-medium leading-snug text-[var(--color-text-primary)]">
          {layer.layer.title}
        </span>
        <span className="text-[0.6875rem] text-[var(--color-text-tertiary)]" style={MONO}>
          {layer.symbol} · {interval}{asOf && ` · ${t('marketView.layers.asOf', { date: asOf })}`}
        </span>
      </div>
      {layer.layer.summary && <Summary text={layer.layer.summary} />}
      {/* Hide and ask sit 2px apart, so their touch boxes meet in that gap: hide's
          reaches left into the popover's padding (8px past it), ask's right over
          the spacer. Remove has 8px of padding to its right and room to its left. */}
      <div className="-mx-1 flex items-center gap-0.5">
        <ActionButton
          label={hidden ? t('marketView.layers.show') : t('marketView.layers.hide')}
          onClick={() => actions.toggleHidden(entry)}
          className="[--tap-target-w:36px] [--tap-target-x:-3px]"
        >
          {hidden ? <Eye size={14} /> : <EyeOff size={14} />}
        </ActionButton>
        <ActionButton
          label={t('marketView.layers.ask')}
          onClick={() => actions.ask(entry)}
          className="[--tap-target-x:7px]"
        >
          <MessageSquare size={14} />
        </ActionButton>
        <span className="flex-1" />
        <ActionButton label={t('marketView.layers.remove')} onClick={() => actions.remove(entry)} danger>
          <Trash2 size={14} />
        </ActionButton>
      </div>
    </div>
  );
}

/** One row of the folded list: opens to its detail in place. */
function ListRow({ entry, actions }: { entry: ChartLayerEntry; actions: LayerActions }): React.ReactElement {
  const { t } = useTranslation();
  const locale = useLocale();
  const [open, setOpen] = useState(false);
  const { layer, hidden } = entry;
  const asOf = formatLayerAsOfShort(layer.layer.as_of, locale);
  return (
    <li className="rule-b py-1.5 first:pt-0 last:border-b-0 last:pb-0">
      <button
        type="button"
        aria-expanded={open}
        onClick={() => setOpen((v) => !v)}
        // Rows sit 13px apart: the box takes the row's own 6px above and below.
        className={cn(
          'tap-target flex w-full items-center gap-2 text-left [--tap-target-h:calc(100%+12px)]',
          hidden && 'opacity-60',
        )}
      >
        <Dot layer={layer.layer} hidden={hidden} />
        <span className="min-w-0 flex-1 truncate text-xs text-[var(--color-text-primary)]">{layer.layer.title}</span>
        {hidden && (
          <span className="shrink-0 text-[0.6875rem] text-[var(--color-text-tertiary)]">{t('marketView.layers.hidden')}</span>
        )}
        {asOf && <span className="shrink-0 text-[0.6875rem] text-[var(--color-text-tertiary)]" style={MONO}>{asOf}</span>}
        <ChevronDown size={12} className={cn('shrink-0 text-[var(--color-text-tertiary)] transition-transform', open && 'rotate-180')} />
      </button>
      {open && <div className="mt-2 pl-4"><LayerDetail entry={entry} actions={actions} /></div>}
    </li>
  );
}

/**
 * A chip that opens its popover on a mouse hover (after a short intent delay),
 * and on a click or a key, which also pins it open until dismissed. A touch has
 * no hover, so a tap is the click.
 */
function ChipPopover({
  chipClassName, chipContent, children, contentClassName, layerId,
}: {
  chipClassName: string;
  chipContent: React.ReactNode;
  children: React.ReactNode;
  contentClassName?: string;
  /** The layer whose title the chip shows; marks the chip for the snapshot's render report. */
  layerId?: string;
}): React.ReactElement {
  const [open, setOpen] = useState(false);
  const [pinned, setPinned] = useState(false);
  const timer = useRef<ReturnType<typeof setTimeout> | null>(null);
  const chip = useRef<HTMLButtonElement | null>(null);
  useEffect(() => () => {
    if (timer.current) clearTimeout(timer.current);
  }, []);
  const later = (next: boolean, ms: number) => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = setTimeout(() => setOpen(next), ms);
  };
  const stopTimer = () => {
    if (timer.current) clearTimeout(timer.current);
    timer.current = null;
  };
  return (
    <Popover
      open={open}
      onOpenChange={(next) => {
        if (!next) {
          stopTimer();
          setOpen(false);
          setPinned(false);
        }
      }}
    >
      <PopoverAnchor asChild>
        <button
          ref={chip}
          type="button"
          className={chipClassName}
          data-layer-chip={layerId ?? ''}
          aria-expanded={open}
          onPointerEnter={(e) => { if (e.pointerType === 'mouse') later(true, HOVER_OPEN_MS); }}
          onPointerLeave={() => { if (!pinned) later(false, HOVER_CLOSE_MS); }}
          onClick={() => {
            stopTimer();
            if (open && pinned) {
              setOpen(false);
              setPinned(false);
            } else {
              setOpen(true);
              setPinned(true);
            }
          }}
        >
          {chipContent}
        </button>
      </PopoverAnchor>
      <PopoverContent
        align="start"
        side="bottom"
        sideOffset={6}
        collisionPadding={8}
        onOpenAutoFocus={(e) => { if (!pinned) e.preventDefault(); }}
        // The chip is an anchor, not a trigger, so a press on it reads as outside:
        // left to dismiss, it would close the popover before the chip's own click
        // reopens it. The chip's click decides (pin, or close when pinned).
        onInteractOutside={(e) => { if (chip.current?.contains(e.target as Node)) e.preventDefault(); }}
        onPointerEnter={stopTimer}
        onPointerLeave={() => { if (!pinned) later(false, HOVER_CLOSE_MS); }}
        className={cn('w-80 border-[var(--color-shell-edge,var(--color-border-muted))] p-3', contentClassName)}
      >
        {children}
      </PopoverContent>
    </Popover>
  );
}

/** Chips stack 4px apart, so a chip's touch box takes half that gap each side. */
const CHIP_CLASS =
  'press tap-target [--tap-target-h:calc(100%+4px)] pointer-events-auto inline-flex h-6 max-w-[16rem] items-center gap-1.5 rounded-full border border-[var(--color-shell-edge,var(--color-border-muted))] bg-[var(--color-shell-float,var(--color-bg-elevated))] px-2.5 text-[0.6875rem] text-[var(--color-text-secondary)] hover:text-[var(--color-text-primary)]';

interface ChartLayerLegendProps {
  chartKey: ChartKey | null;
  className?: string;
}

export function ChartLayerLegend({ chartKey, className }: ChartLayerLegendProps): React.ReactElement | null {
  const { t } = useTranslation();
  const { toast } = useToast();
  const locale = useLocale();
  const composer = useChartComposer();
  const rows = useChartLayerRows(chartKey);
  const hidden = useHiddenLayers(chartKey);
  const entries: ChartLayerEntry[] = rows.map((layer) => ({ layer, hidden: hidden.has(layer.layer_id) }));
  const removeLayer = useRemoveChartLayer(() => toast({ variant: 'destructive', title: t('marketView.layers.removeFailed') }));

  const actions: LayerActions = {
    toggleHidden: (entry) => {
      if (chartKey) setLayerHidden(chartKey, entry.layer.layer_id);
    },
    ask: ({ layer }) => {
      const handled = composer.ask({
        symbol: layer.symbol,
        timeframe: layer.timeframe,
        layerId: layer.layer_id,
        layerTitle: layer.layer.title,
        summary: layer.layer.summary,
      });
      if (!handled) toast({ title: t('marketView.layers.askUnavailable') });
    },
    remove: (entry) => removeLayer.mutate(entry.layer),
  };

  if (entries.length === 0) return null;

  const folded = entries.length > COLLAPSE_AFTER;
  return (
    <div
      role="group"
      aria-label={t('marketView.layers.legendLabel')}
      className={cn('pointer-events-none flex flex-col items-start gap-1', className)}
    >
      {folded ? (
        <ChipPopover
          contentClassName="w-80"
          chipClassName={CHIP_CLASS}
          chipContent={(
            <>
              <Layers size={12} aria-hidden="true" className="shrink-0" />
              {t('marketView.layers.count', { count: entries.length })}
            </>
          )}
        >
          <ul>
            {entries.map((entry) => (
              <ListRow key={entry.layer.layer_id} entry={entry} actions={actions} />
            ))}
          </ul>
        </ChipPopover>
      ) : (
        entries.map((entry) => (
          <ChipPopover
            key={entry.layer.layer_id}
            chipClassName={cn(CHIP_CLASS, entry.hidden && 'opacity-60')}
            layerId={entry.layer.layer.id}
            chipContent={(
              <>
                <Dot layer={entry.layer.layer} hidden={entry.hidden} />
                <span data-layer-title="" className="min-w-0 truncate text-[var(--color-text-primary)]">{entry.layer.layer.title}</span>
                {entry.layer.layer.as_of && (
                  <span className="shrink-0 text-[0.6875rem] text-[var(--color-text-tertiary)]" style={MONO}>
                    {formatLayerAsOfShort(entry.layer.layer.as_of, locale)}
                  </span>
                )}
              </>
            )}
          >
            <LayerDetail entry={entry} actions={actions} />
          </ChipPopover>
        ))
      )}
    </div>
  );
}

export default ChartLayerLegend;
