import { useId } from 'react';
import { RadioChoice } from '@/components/ui/choice';
import { cn } from '@/lib/utils';

/** One way to run it, drawn as Settings draws a trading level. Named by its
 *  label alone, so its detail is read as a description. */
export function ModeCard<V extends string>({ name, mode, checked, label, detail, onSelect, onReselect }: {
  name: string;
  mode: V;
  checked: boolean;
  label: string;
  detail: string;
  onSelect: (mode: V) => void;
  onReselect?: (mode: V) => void;
}) {
  const labelId = useId();
  const detailId = useId();
  return (
    <RadioChoice
      name={name}
      value={mode}
      checked={checked}
      onSelect={onSelect}
      onReselect={onReselect}
      aria-labelledby={labelId}
      aria-describedby={detailId}
      markClassName="mt-0.5"
      className={cn(
        'items-start gap-3 rounded-lg px-3 py-2.5 transition-colors',
        checked ? 'bg-(--color-bg-tag)' : 'bg-(--color-bg-card) hover:bg-(--color-bg-hover)',
      )}
      style={{ border: `1px solid ${checked ? 'var(--color-text-primary)' : 'var(--color-border-muted)'}` }}
    >
      <span className="min-w-0 flex-1">
        <span id={labelId} className="block text-[0.8125rem] font-medium" style={{ color: 'var(--color-text-primary)' }}>{label}</span>
        <span id={detailId} className="mt-0.5 block text-xs leading-relaxed" style={{ color: 'var(--color-text-tertiary)' }}>{detail}</span>
      </span>
    </RadioChoice>
  );
}
