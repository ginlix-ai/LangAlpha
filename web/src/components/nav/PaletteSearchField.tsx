import { useCommandPalette, type PaletteStart } from '../CommandPalette/CommandPaletteProvider';
import { useSlashKey } from '@/hooks/useSlashKey';
import { PaletteTrigger, type PaletteTriggerProps } from './PaletteSearchButton';

/**
 * A page's search field that the command palette serves, opening at `start`
 * on a tap or on `/`. A button the caller draws as its field (its children,
 * class and style), so a phone's keyboard opens once, for the palette's own
 * field, rather than under a header it crowds out.
 */
export function PaletteSearchField({ start, ...props }: PaletteTriggerProps & { start: PaletteStart }) {
  const palette = useCommandPalette();
  useSlashKey(() => palette?.open(start), !!palette);
  return <PaletteTrigger start={start} {...props} />;
}
