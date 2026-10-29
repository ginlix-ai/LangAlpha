/**
 * A labelled radiogroup of bordered options: a title, a short description or
 * a line of facts, and an optional note (why it is locked, which one is
 * current).
 *
 * The computer pickers (tier and provider) share it, so a locked option reads
 * and behaves the same in both: faded, unselectable, skipped by the arrows.
 * Each option is a `RadioChoice`, whose native input owns the keyboard.
 */
import React, { useId } from 'react';

import { RadioChoice } from '@/components/ui/choice';
import { cn } from '@/lib/utils';

export interface RadioCardOption<V extends string> {
  value: V;
  title: string;
  /** A short line under the title. */
  description?: string;
  /** Short facts in place of a sentence, such as a spec's vCPU, RAM and disk, read as one dotted line. */
  facts?: string[];
  /** Beside the title on a card, at the far end of a row. */
  note?: string | null;
  disabled: boolean;
}

interface RadioCardGroupProps<V extends string> {
  options: RadioCardOption<V>[];
  value: V | null;
  onChange: (value: V) => void;
  /** Shown above the options, and the group's accessible name. */
  label: string;
  /**
   * Cards side by side once the group is wide enough for that many, stacked
   * below it. Read off the group's own width, not the window's: a dialog is
   * narrower than the screen it sits on.
   */
  columns?: 1 | 2 | 3;
  /**
   * `rows` sets each option on one compact row, its facts on a single line
   * after the title and its note at the far end, for options told apart by a
   * few facts (the sizes). The facts drop under the title, whole, on a row
   * too narrow for them.
   */
  layout?: 'cards' | 'rows';
}

// Literal class names, so Tailwind's scanner sees them.
const GRID_COLUMNS = { 1: '', 2: '@sm:grid-cols-2', 3: '@md:grid-cols-3' } as const;

export function RadioCardGroup<V extends string>({
  options,
  value,
  onChange,
  label,
  columns = 1,
  layout = 'cards',
}: RadioCardGroupProps<V>) {
  // One id names the native group and prefixes every element id below it.
  const name = useId();
  const labelId = `${name}-label`;
  const rows = layout === 'rows';
  // Three across leaves a card little room: its padding tightens there, and
  // its note moves under the text.
  const narrow = !rows && columns === 3;

  return (
    <div className="flex flex-col gap-1.5">
      <span id={labelId} className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
        {label}
      </span>
      <div className="@container">
        <div
          className={cn('grid', rows ? 'gap-1.5' : cn('gap-2', GRID_COLUMNS[columns]))}
          role="radiogroup"
          aria-labelledby={labelId}
        >
          {options.map(({ value: id, title, description, facts, note, disabled }, index) => {
            const selected = value === id;
            const hasFacts = !!facts?.length;
            const hasDetail = hasFacts || !!description;
            const titleId = `${name}-${index}-title`;
            const noteId = `${name}-${index}-note`;
            const detailId = `${name}-${index}-detail`;
            // Named by its title alone, so the note and the detail are read as
            // its description rather than as part of the name.
            const describedBy = [note && noteId, hasDetail && detailId].filter(Boolean).join(' ') || undefined;
            const titleEl = (
              <span
                id={titleId}
                className={cn('text-sm font-medium', rows && 'w-24 shrink-0')}
                style={{ color: 'var(--color-text-primary)' }}
              >
                {title}
              </span>
            );
            const noteEl = note && (
              <span
                id={noteId}
                className={cn(
                  'text-xs whitespace-nowrap',
                  rows && 'ms-auto',
                  // Three across, a note beside a long title would break the
                  // title, so it closes the card instead.
                  narrow && '@md:order-last @md:mt-0.5 @md:basis-full',
                )}
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {note}
              </span>
            );
            const detailEl = hasDetail && (
              <span
                id={detailId}
                className={cn(
                  'text-xs',
                  hasFacts && 'tabular-nums',
                  // Whole, so a fact never breaks away from the line it is on.
                  rows ? 'whitespace-nowrap' : 'mt-0.5 basis-full',
                )}
                style={{ color: 'var(--color-text-tertiary)' }}
              >
                {hasFacts
                  ? facts?.map((fact, i) => (
                      <React.Fragment key={i}>
                        {i > 0 && ' · '}
                        {fact}
                      </React.Fragment>
                    ))
                  : description}
              </span>
            );
            return (
              <RadioChoice
                key={id}
                name={name}
                value={id}
                checked={selected}
                disabled={disabled}
                onSelect={onChange}
                size="md"
                aria-labelledby={titleId}
                aria-describedby={describedBy}
                markClassName={rows ? undefined : 'mt-0.5'}
                className={cn(
                  'gap-3 rounded-lg border transition-colors',
                  rows ? 'items-center px-3 py-2.5' : 'items-start p-3',
                  narrow && '@md:p-2.5',
                  !selected && !disabled && 'hover:bg-(--color-bg-subtle)',
                )}
                // Selection is carried by neutrals that hold on any fill, the
                // dialog's elevated one included: a stronger edge, a tint, and
                // the filled mark. Never the accent (DESIGN.md).
                style={{
                  borderColor: selected ? 'var(--color-border-strong)' : 'var(--color-border-muted)',
                  backgroundColor: selected ? 'var(--color-bg-hover)' : undefined,
                }}
              >
                {rows ? (
                  <span className="flex min-w-0 flex-1 flex-wrap items-baseline gap-x-3 gap-y-0.5">
                    {titleEl}
                    {detailEl}
                    {noteEl}
                  </span>
                ) : (
                  <span className="flex min-w-0 flex-1 flex-wrap items-baseline justify-between gap-x-2">
                    {titleEl}
                    {noteEl}
                    {detailEl}
                  </span>
                )}
              </RadioChoice>
            );
          })}
        </div>
      </div>
    </div>
  );
}
