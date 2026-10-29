import { ChevronDown, ChevronUp, GripVertical } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  DndContext,
  MouseSensor,
  TouchSensor,
  closestCenter,
  useSensor,
  useSensors,
  type DragEndEvent,
} from '@dnd-kit/core';
import { SortableContext, arrayMove, useSortable, verticalListSortingStrategy } from '@dnd-kit/sortable';
import { CSS } from '@dnd-kit/utilities';
import { cn } from '@/lib/utils';
import { OUTPUT_FORMATS, type OutputFormat } from '@/lib/outputFormat';

interface OutputFormatRankingProps {
  /** The saved order, or null while the agent decides. */
  ranking: OutputFormat[] | null;
  /** A new order, or null to leave it to the agent again. */
  onChange: (ranking: OutputFormat[] | null) => void;
  labelledBy: string;
}

/**
 * The output format as an order the user sets: dragged, or moved a step with
 * the buttons beside each form, which are also the keyboard's way. Unset, it
 * shows the forms in the server's order with faded ranks, and the first move
 * saves it; a form a partial ranking leaves out is faded the same way, since
 * the agent is given no rank for it.
 */
export function OutputFormatRanking({ ranking, onChange, labelledBy }: OutputFormatRankingProps) {
  const { t } = useTranslation();
  // A ranking the agent wrote may leave a form out: it is drawn last.
  const shown = ranking ? [...ranking, ...OUTPUT_FORMATS.filter((format) => !ranking.includes(format))] : [...OUTPUT_FORMATS];
  const sensors = useSensors(
    useSensor(MouseSensor, { activationConstraint: { distance: 4 } }),
    useSensor(TouchSensor, { activationConstraint: { delay: 150, tolerance: 5 } }),
  );
  const move = (from: number, to: number) => {
    if (from === to || to < 0 || to >= shown.length) return;
    onChange(arrayMove(shown, from, to));
  };
  const onDragEnd = ({ active, over }: DragEndEvent) => {
    if (over) move(shown.indexOf(active.id as OutputFormat), shown.indexOf(over.id as OutputFormat));
  };

  return (
    <div className="flex flex-col gap-2">
      <DndContext sensors={sensors} collisionDetection={closestCenter} onDragEnd={onDragEnd}>
        <SortableContext items={shown} strategy={verticalListSortingStrategy}>
          <ol aria-labelledby={labelledBy} className="flex flex-col gap-1.5">
            {shown.map((format, index) => (
              <RankedFormat
                key={format}
                format={format}
                rank={index + 1}
                ranked={ranking !== null && index < ranking.length}
                onUp={index > 0 ? () => move(index, index - 1) : undefined}
                onDown={index < shown.length - 1 ? () => move(index, index + 1) : undefined}
              />
            ))}
          </ol>
        </SortableContext>
      </DndContext>
      <p className="settings-row-desc">
        {ranking ? (
          <button
            type="button"
            onClick={() => onChange(null)}
            className="underline underline-offset-2 hover:text-(--color-text-primary)"
          >
            {t('settings.outputFormatReset')}
          </button>
        ) : (
          t('settings.outputFormatDecides')
        )}
      </p>
    </div>
  );
}

function RankedFormat({ format, rank, ranked, onUp, onDown }: {
  format: OutputFormat;
  rank: number;
  ranked: boolean;
  onUp?: () => void;
  onDown?: () => void;
}) {
  const { t } = useTranslation();
  const { listeners, setNodeRef, setActivatorNodeRef, transform, transition, isDragging } = useSortable({ id: format });
  const name = t(`settings.outputFormatOption.${format}.title`);
  const step = 'flex h-7 w-7 items-center justify-center rounded-md text-(--color-text-tertiary) hover:bg-(--color-bg-hover) hover:text-(--color-text-primary) disabled:pointer-events-none disabled:opacity-30';
  return (
    <li
      ref={setNodeRef}
      data-testid={`output-format-${format}`}
      data-ranked={ranked || undefined}
      className={cn(
        'flex items-center gap-2 rounded-lg bg-(--color-bg-card) py-2 pr-1.5 pl-1',
        isDragging && 'relative z-10 shadow-md',
      )}
      style={{ transform: CSS.Translate.toString(transform), transition, border: '1px solid var(--color-border-muted)' }}
    >
      <span
        ref={setActivatorNodeRef}
        {...listeners}
        aria-hidden
        className="flex h-7 w-5 shrink-0 cursor-grab touch-none items-center justify-center text-(--color-text-quaternary) active:cursor-grabbing"
        title={t('settings.outputFormatDrag', { name })}
      >
        <GripVertical className="h-4 w-4" />
      </span>
      <span
        className={cn('w-4 shrink-0 text-center text-xs font-medium tabular-nums', !ranked && 'opacity-40')}
        style={{ color: 'var(--color-text-secondary)' }}
      >
        {rank}
      </span>
      <span className="min-w-0 flex-1">
        <span className="block text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{name}</span>
        <span className="mt-0.5 block text-xs leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>
          {t(`settings.outputFormatOption.${format}.desc`)}
        </span>
      </span>
      <span className="flex shrink-0 flex-col sm:flex-row">
        <button type="button" className={step} disabled={!onUp} onClick={onUp} aria-label={t('settings.outputFormatMoveUp', { name })}>
          <ChevronUp className="h-4 w-4" />
        </button>
        <button
          type="button"
          className={step}
          disabled={!onDown}
          onClick={onDown}
          aria-label={t('settings.outputFormatMoveDown', { name })}
          data-search-focus={rank === 1 ? '' : undefined}
        >
          <ChevronDown className="h-4 w-4" />
        </button>
      </span>
    </li>
  );
}
