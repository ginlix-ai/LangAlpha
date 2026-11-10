import { Search } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useCommandPalette } from '../CommandPalette/CommandPaletteProvider';
import { cn } from '@/lib/utils';

/**
 * The command palette's tap target on a phone, where no sidebar shows its
 * search row and ⌘K needs a hardware keyboard. Sized as the icon buttons of
 * the header it sits in. The palette's code is fetched as the finger lands,
 * ahead of the click.
 */
export function PaletteSearchButton({ className, iconClassName = 'h-5 w-5' }: { className?: string; iconClassName?: string }) {
  const { t } = useTranslation();
  const palette = useCommandPalette();
  if (!palette) return null;
  return (
    <button
      type="button"
      onClick={palette.open}
      onPointerDown={palette.preload}
      aria-haspopup="dialog"
      aria-label={t('palette.search')}
      title={t('palette.search')}
      className={cn('tap-target p-2 rounded-md shrink-0 transition-colors hover:bg-(--color-border-muted)', className)}
      style={{ color: 'var(--color-text-primary)' }}
    >
      <Search className={iconClassName} aria-hidden="true" />
    </button>
  );
}
