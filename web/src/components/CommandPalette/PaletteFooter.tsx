import { useTranslation } from 'react-i18next';
import { clientOs } from '@/lib/clientOs';

export interface KeysOffered {
  /** What Enter does to the selected row. */
  enter?: string;
  /** ↑↓ move the selection; not while the field edits a row. */
  move: boolean;
  /** → opens the selected row's other actions. */
  actions: boolean;
  /** ⌘↵ asks the agent. */
  ask: boolean;
  /** ← and Esc step back (a row's actions, a drilled-in view) rather than close. */
  back: boolean;
  /** Tab searches within the selected row (a workspace or thread). */
  scope: boolean;
  /** The scope Backspace (in the empty field), Shift+Tab and Esc leave, at its own results. */
  leave: string | null;
  /** The field edits a row: Esc, and Backspace in the empty field, go back to its actions (← moves the caret). */
  edit: boolean;
}

/**
 * The keys the selected row takes, said twice: drawn in the footer for the
 * eye (hidden from the accessibility tree, whose key caps read as symbols),
 * and as a sentence the field names as its description. A screen reader
 * reads the description when the field takes focus and on request, not on
 * every arrow, so moving the selection announces the row alone.
 */
export function PaletteFooter({ keys, describedId }: { keys: KeysOffered; describedId: string }) {
  const { t } = useTranslation();
  const mac = clientOs() === 'mac';
  const said = [
    keys.enter && t('palette.keys.enter', { action: keys.enter }),
    keys.actions && t('palette.keys.actions'),
    keys.ask && (mac ? t('palette.keys.askMac') : t('palette.keys.askOther')),
    keys.scope && t('palette.keys.scope'),
    keys.back && t('palette.keys.back'),
    keys.leave && t('palette.keys.leave', { scope: keys.leave }),
    keys.edit && t('palette.keys.editBack'),
  ].filter(Boolean);
  return (
    <>
      <span id={describedId} className="sr-only">
        {said.join(' ')}
      </span>
      <div className="command-palette-footer" aria-hidden>
        {keys.move && <span><kbd>↑↓</kbd>{t('palette.do.move')}</span>}
        {keys.enter && <span><kbd>↵</kbd>{keys.enter}</span>}
        {keys.ask && <span><kbd>{mac ? '⌘↵' : 'Ctrl ↵'}</kbd>{t('palette.do.ask')}</span>}
        {keys.actions && <span><kbd>→</kbd>{t('palette.do.actions')}</span>}
        {keys.scope && <span><kbd>tab</kbd>{t('palette.do.searchIn')}</span>}
        {keys.back && <span><kbd>←</kbd>{t('palette.do.back')}</span>}
        <span className="command-palette-footer-end">
          <kbd>esc</kbd>
          {keys.back || keys.leave || keys.edit ? t('palette.do.back') : t('palette.do.close')}
        </span>
      </div>
    </>
  );
}
