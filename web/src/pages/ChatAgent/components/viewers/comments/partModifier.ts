import { clientOs } from '@/lib/clientOs';

const ON_META = clientOs() === 'mac';

/**
 * The key that makes a click add a part to the comment being written: Cmd on
 * a Mac, Ctrl elsewhere, never both. A Ctrl-click on a Mac is a right-click
 * that opens the context menu, so taking Ctrl there would promise an add
 * that never happens.
 */
export const ADD_KEY: 'Meta' | 'Control' = ON_META ? 'Meta' : 'Control';

/** The add key as the hint names it. */
export const ADD_KEY_LABEL = ON_META ? '⌘' : 'Ctrl';

export function addKeyDown(e: { metaKey: boolean; ctrlKey: boolean }): boolean {
  return ON_META ? e.metaKey : e.ctrlKey;
}

/** A Ctrl-press on a Mac, which opens the context menu rather than pressing. */
export function contextPress(e: { ctrlKey: boolean }): boolean {
  return ON_META && e.ctrlKey;
}
