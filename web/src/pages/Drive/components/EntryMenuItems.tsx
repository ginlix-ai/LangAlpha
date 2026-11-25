import { useTranslation } from 'react-i18next';
import { Download, FolderInput, Pencil, Trash2 } from 'lucide-react';
import type { ContextMenuItem, ContextMenuSeparator } from '@/components/ui/context-menu';
import type { DropdownMenuItem, DropdownMenuSeparator } from '@/components/ui/dropdown-menu';
import type { DriveEntry } from '@/types/drive';
import { useDriveContext } from '../driveContext';

/** The item and separator of the menu the entries render in: a dropdown or a context menu. */
export interface MenuParts {
  Item: typeof DropdownMenuItem | typeof ContextMenuItem;
  Separator: typeof DropdownMenuSeparator | typeof ContextMenuSeparator;
}

/** What can be done to an entry, the same in a row's menu, its context menu and the file's header. */
export function EntryMenuItems({ entry, parts: { Item, Separator } }: { entry: DriveEntry; parts: MenuParts }) {
  const { t } = useTranslation();
  const { actions, ask, trash } = useDriveContext();
  return (
    <>
      <Item onSelect={() => ask.rename(entry)}>
        <Pencil className="h-3.5 w-3.5" aria-hidden="true" />
        {t('drive:actions.rename')}
      </Item>
      <Item onSelect={() => ask.move(entry)}>
        <FolderInput className="h-3.5 w-3.5" aria-hidden="true" />
        {t('drive:actions.move')}
      </Item>
      {entry.kind === 'file' && (
        <Item onSelect={() => void actions.download(entry)}>
          <Download className="h-3.5 w-3.5" aria-hidden="true" />
          {t('drive:actions.download')}
        </Item>
      )}
      <Separator />
      <Item variant="destructive" onSelect={() => trash(entry)}>
        <Trash2 className="h-3.5 w-3.5" aria-hidden="true" />
        {t('drive:actions.trash')}
      </Item>
    </>
  );
}
