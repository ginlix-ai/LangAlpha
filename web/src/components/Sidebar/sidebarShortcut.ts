import { useEffect } from 'react';
import { clientOs } from '@/lib/clientOs';

/** Cmd+B on a Mac, Ctrl+B elsewhere: the key most apps with a sidebar use. On
 *  a Mac only Cmd counts, since Ctrl+B there moves the caret back a character. */
function isToggleShortcut(e: KeyboardEvent): boolean {
  if (e.altKey || e.shiftKey || e.key.toLowerCase() !== 'b') return false;
  return clientOs() === 'mac' ? e.metaKey && !e.ctrlKey : e.ctrlKey && !e.metaKey;
}

export const toggleShortcutLabel = () => (clientOs() === 'mac' ? '⌘B' : 'Ctrl B');
export const toggleKeyShortcuts = () => (clientOs() === 'mac' ? 'Meta+B' : 'Control+B');

/**
 * The shortcut, wherever the toggle is drawn. A key an editor already took
 * (bold, in one that has it) is left to it, as is a rich-text field, where the
 * browser's own Cmd+B is bold. A held key is taken but toggles once, so the
 * desktop app's menu, which answers the key the page leaves, does not toggle
 * on each repeat.
 */
export function useSidebarShortcut(onToggle: (() => void) | undefined) {
  useEffect(() => {
    if (!onToggle) return;
    const onKeyDown = (e: KeyboardEvent) => {
      if (e.defaultPrevented || !isToggleShortcut(e)) return;
      if (e.target instanceof HTMLElement && e.target.isContentEditable) return;
      e.preventDefault();
      if (!e.repeat) onToggle();
    };
    window.addEventListener('keydown', onKeyDown);
    return () => window.removeEventListener('keydown', onKeyDown);
  }, [onToggle]);
}
