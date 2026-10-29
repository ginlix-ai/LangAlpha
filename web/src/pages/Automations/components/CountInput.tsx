import { useState } from 'react';
import { Input } from '@/components/ui/input';

/** A count held as a number. What is typed lands only once it reads as a
 *  whole number in range; leaving the field puts back the count it holds. */
export default function CountInput({
  id,
  value,
  min,
  max,
  onChange,
  className,
  'aria-label': ariaLabel,
}: {
  id?: string;
  value: number;
  min: number;
  max: number;
  onChange: (n: number) => void;
  className?: string;
  'aria-label'?: string;
}) {
  const [draft, setDraft] = useState<string | null>(null);
  return (
    <Input
      id={id}
      type="number"
      min={min}
      max={max}
      aria-label={ariaLabel}
      value={draft ?? String(value)}
      onChange={(e) => {
        const text = e.target.value;
        setDraft(text);
        const n = Number(text);
        if (text.trim() && Number.isInteger(n) && n >= min && n <= max) onChange(n);
      }}
      onBlur={() => setDraft(null)}
      className={className}
    />
  );
}
