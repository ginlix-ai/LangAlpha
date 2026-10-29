import { useLayoutEffect, useRef, type ChangeEvent, type RefObject } from 'react';
import { normalizeSecretName } from '@/lib/secretNames';

/**
 * The change handler of a field a vault name is typed into. Each keystroke
 * takes the vault's spelling at once, so the field shows what Save stores.
 * The value then changes under the caret, and a controlled input whose value
 * changes sends the caret to the end, so a letter typed in the middle of a
 * name would push every later keystroke to the end of it; the caret is put
 * back where the keystroke left it.
 */
export function useSecretNameInput(
  inputRef: RefObject<HTMLInputElement | null>,
  name: string,
  onName: (name: string) => void,
): (e: ChangeEvent<HTMLInputElement>) => void {
  const caret = useRef<number | null>(null);
  useLayoutEffect(() => {
    const input = inputRef.current;
    if (caret.current === null || !input || document.activeElement !== input) return;
    input.setSelectionRange(caret.current, caret.current);
    caret.current = null;
  }, [inputRef, name]);
  return (e) => {
    const typed = e.target.value;
    const spelled = normalizeSecretName(typed);
    const at = e.target.selectionStart;
    // What the spelling drops is a leading digit, before the caret.
    caret.current = at === null ? null : Math.max(0, at - (typed.length - spelled.length));
    onName(spelled);
  };
}
