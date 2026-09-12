import React from 'react';

export interface SegmentedOption<T extends string> {
  value: T;
  label: React.ReactNode;
  /** Offered but refused. It greys out in place, so the row still says which
   *  answers exist rather than quietly shrinking to the ones that are left. */
  disabled?: boolean;
}

interface SegmentedControlProps<T extends string> {
  /** null = no segment is pressed, which is a selection spanning more than one value. */
  value: T | null;
  onChange: (value: T) => void;
  options: ReadonlyArray<SegmentedOption<T>>;
  /** id of the visible row label, so the group is named by the text a sighted reader sees. */
  labelledBy?: string;
  /** Names the group where no visible label stands for it. */
  label?: string;
  /** Nothing may be picked while a write is in flight. The values keep their
   *  colours: the row is saving, not refusing. */
  disabled?: boolean;
  /** `compact` is the row-height variant, for a control at the edge of a list row. */
  size?: 'default' | 'compact';
}

/**
 * Exclusive-choice row of pressed buttons, the Settings shape for theme, font
 * size and turn-end landing, and the shape a tool row uses for its binding. A
 * plain button group rather than a Radix ToggleGroup: it keeps the markup and
 * theme-var styling the rows already shipped, and the pressed state is read by
 * screen readers as a toggle.
 *
 * One click is the whole change, which is what a list of eighty-eight rows
 * needs: a select made every change open a menu, pick, and close, with the
 * value the user wanted already on screen.
 */
export function SegmentedControl<T extends string>({
  value,
  onChange,
  options,
  labelledBy,
  label,
  disabled = false,
  size = 'default',
}: SegmentedControlProps<T>): React.ReactElement {
  const compact = size === 'compact';
  return (
    <div
      role="group"
      aria-labelledby={labelledBy}
      aria-label={label}
      className={
        compact
          ? 'inline-flex rounded-md overflow-hidden clips-focus-ring flex-shrink-0'
          : 'inline-flex rounded-lg overflow-hidden clips-focus-ring'
      }
      style={{ border: '1px solid var(--color-border-muted)' }}
    >
      {options.map((option) => {
        const selected = option.value === value;
        return (
          <button
            key={option.value}
            type="button"
            aria-pressed={selected}
            disabled={disabled || option.disabled}
            onClick={() => onChange(option.value)}
            className={`flex items-center gap-1.5 font-medium transition-colors disabled:cursor-default ${
              compact ? 'px-2 py-0.5 text-[0.6875rem]' : 'px-2.5 py-1 text-[0.8125rem]'
            }`}
            style={{
              backgroundColor: selected ? 'var(--color-accent-soft)' : 'transparent',
              // Secondary, not tertiary: the tertiary grey reads under 2.5:1
              // on the light surface, below AA for 13 px text.
              color: option.disabled
                ? 'var(--color-text-quaternary)'
                : selected
                  ? 'var(--color-accent-primary)'
                  : 'var(--color-text-secondary)',
            }}
          >
            {option.label}
          </button>
        );
      })}
    </div>
  );
}
