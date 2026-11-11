import type { ButtonHTMLAttributes } from 'react';
import { Search } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useCommandPalette, type PaletteStart } from '../CommandPalette/CommandPaletteProvider';
import { cn } from '@/lib/utils';

export type PaletteTriggerProps = Omit<ButtonHTMLAttributes<HTMLButtonElement>, 'type' | 'onClick' | 'onPointerDown'> & {
  /** Where the palette opens; everything when absent. */
  start?: PaletteStart;
};

/**
 * A button that opens the command palette, and nothing where there is no
 * palette to open. The palette's code is fetched as the finger lands, ahead
 * of the click.
 */
export function PaletteTrigger({ start, ...props }: PaletteTriggerProps) {
  const palette = useCommandPalette();
  if (!palette) return null;
  return <button type="button" onClick={() => palette.open(start)} onPointerDown={palette.preload} aria-haspopup="dialog" {...props} />;
}

/**
 * The command palette's tap target on a phone, where no sidebar shows its
 * search row and ⌘K needs a hardware keyboard. Sized as the icon buttons of
 * the header it sits in.
 */
export function PaletteSearchButton({ className, iconClassName = 'h-5 w-5' }: { className?: string; iconClassName?: string }) {
  const { t } = useTranslation();
  return (
    <PaletteTrigger
      aria-label={t('palette.search')}
      title={t('palette.search')}
      className={cn('tap-target p-2 rounded-md shrink-0 transition-colors hover:bg-(--color-border-muted)', className)}
      style={{ color: 'var(--color-text-primary)' }}
    >
      <Search className={iconClassName} aria-hidden="true" />
    </PaletteTrigger>
  );
}
