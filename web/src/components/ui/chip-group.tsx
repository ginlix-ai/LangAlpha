/**
 * Pick any number from a short list, drawn as pills that take a check when
 * picked. A pill is as wide picked as not: the check grows in inside the
 * pill's own padding and the label slides over to make room, so a pick never
 * rewraps the row and sends the chips after it onto another line.
 *
 * Native checkboxes underneath, as `CheckboxChoice` does: Space toggles the
 * focused chip and each takes its name from its own label. A row of checkboxes
 * gives Tab one stop per chip; the arrow keys walk the row as well, since a
 * wrapped row of pills reads as one control. Enter is left alone, so a page
 * can take it to mean "continue".
 */
import type { KeyboardEvent, ReactNode } from 'react';
import { Check } from 'lucide-react';
import { cn } from '@/lib/utils';

export interface ChipOption<V extends string> {
  value: V;
  label: ReactNode;
}

interface ChipGroupProps<V extends string> {
  /** Picked values, in the order they were picked. */
  values: readonly V[];
  /** A pick appends, so the order of `values` stays the order of picking. */
  onChange: (values: V[]) => void;
  options: ReadonlyArray<ChipOption<V>>;
  /** id of the visible question naming the group. */
  labelledBy: string;
}

const STEP: Record<string, number> = { ArrowRight: 1, ArrowDown: 1, ArrowLeft: -1, ArrowUp: -1 };

function moveFocus(event: KeyboardEvent<HTMLDivElement>) {
  const step = STEP[event.key];
  if (!step) return;
  const inputs = [...event.currentTarget.querySelectorAll<HTMLInputElement>('input[type="checkbox"]:not(:disabled)')];
  const at = inputs.indexOf(event.target as HTMLInputElement);
  if (at === -1) return;
  event.preventDefault();
  inputs[(at + step + inputs.length) % inputs.length]?.focus();
}

export function ChipGroup<V extends string>({ values, onChange, options, labelledBy }: ChipGroupProps<V>) {
  return (
    <div role="group" aria-labelledby={labelledBy} onKeyDown={moveFocus} className="flex flex-wrap gap-2">
      {options.map((option) => {
        const checked = values.includes(option.value);
        return (
          <label
            key={option.value}
            data-state={checked ? 'checked' : 'unchecked'}
            className={cn(
              'rings-within relative inline-flex h-9 cursor-pointer items-center rounded-full border px-[19px] text-sm press',
              checked
                ? 'border-(--color-text-primary) bg-(--color-bg-tag) text-(--color-text-primary)'
                : 'border-(--color-border-muted) bg-(--color-bg-card) text-(--color-text-secondary) hover:border-(--color-border-default) hover:text-(--color-text-primary)',
            )}
          >
            <input
              type="checkbox"
              checked={checked}
              onChange={(e) =>
                onChange(
                  e.target.checked
                    ? [...values, option.value]
                    : values.filter((v) => v !== option.value),
                )
              }
              className="sr-only"
            />
            {/* 19px either side of the label; picked, the check takes 8 to
                22 and the label moves 7 over, leaving a 4px gap and 12 on
                the right. Transforms, so the pill itself never changes. */}
            <Check
              aria-hidden="true"
              className={cn(
                'absolute left-2 top-1/2 h-3.5 w-3.5 -translate-y-1/2 transition-[opacity,scale]',
                checked ? 'scale-100 opacity-100' : 'opacity-0 motion-safe:scale-50',
              )}
              strokeWidth={2.5}
              style={{ color: 'var(--color-text-primary)' }}
            />
            <span
              className={cn(
                'whitespace-nowrap transition-[translate] motion-reduce:transition-none',
                checked ? 'translate-x-[7px]' : 'translate-x-0',
              )}
            >
              {option.label}
            </span>
          </label>
        );
      })}
    </div>
  );
}
