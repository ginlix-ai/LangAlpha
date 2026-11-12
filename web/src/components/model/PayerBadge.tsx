import type { ReactNode } from 'react';
import { cn } from '@/lib/utils';
import type { Payer } from '@/lib/modelPayer';
import { usePayerLabel } from '@/hooks/usePayerLabel';

/** The small tag set beside a model or a setting's name. */
export function NameTag({ children, className }: { children: ReactNode; className?: string }) {
  return (
    <span
      className={cn('flex-none rounded px-[5px] py-px text-[0.625rem] font-medium leading-normal', className)}
      style={{ backgroundColor: 'var(--color-bg-tag)', color: 'var(--color-text-tertiary)' }}
    >
      {children}
    </span>
  );
}

/** Who pays for a turn on a model, set beside its name. Nothing for a model
 *  with no payer, so a caller passes whatever its map holds. */
export function PayerBadge({ payer, className, markLegacy }: {
  payer: Payer | undefined;
  className?: string;
  /** False keeps a legacy sign-in's tag to its name, where a row is too
   *  narrow for more; its brand's tab in the same picker says legacy. */
  markLegacy?: boolean;
}) {
  const label = usePayerLabel();
  if (!payer) return null;
  return <NameTag className={className}>{label(payer, { markLegacy })}</NameTag>;
}
