import { useEffect, useEffectEvent, useRef, type RefObject } from 'react';
import { isQuoteSurface } from './quoteAnchor';

/** How long a selection made without a mouse has to rest before it counts as finished. */
const SETTLE_MS = 250;

/**
 * Reports a finished text selection inside `surfaceRef`. Listens on the document
 * because a drag often ends past the surface it started in, overshooting the
 * last line onto the composer. Shift+arrows and a touch selection's handles have
 * no such end, so those are read once the selection stops changing.
 */
export function useTextSelectionCapture({ surfaceRef, enabled, onSelect, onClear }: {
  surfaceRef: RefObject<HTMLElement | null>;
  /** Off while a reply is being written, so a stray drag cannot replace its quote. */
  enabled: boolean;
  onSelect: (text: string, range: Range, backward: boolean) => void;
  onClear: () => void;
}) {
  const readRange = useRef<Range | null>(null);
  const alreadyRead = () => {
    const was = readRange.current;
    const selection = window.getSelection();
    if (!was || !selection?.rangeCount) return false;
    const range = selection.getRangeAt(0);
    return range.startContainer === was.startContainer && range.startOffset === was.startOffset
      && range.endContainer === was.endContainer && range.endOffset === was.endOffset;
  };

  const read = useEffectEvent(() => {
    readRange.current = null;
    const surface = surfaceRef.current;
    const selection = window.getSelection();
    const text = selection?.toString() ?? '';
    if (!surface || !selection || selection.rangeCount === 0 || !text.trim()) {
      onClear();
      return;
    }
    const range = selection.getRangeAt(0);
    const active = document.activeElement;
    // Text chosen in a field here (a message being edited) is that field's.
    const inField = !!active?.closest('input, textarea, [contenteditable="true"]') && surface.contains(active);
    if (inField || !surface.contains(range.commonAncestorContainer)) {
      onClear();
      return;
    }
    // A clone: the live range moves with the selection, which focus takes away.
    // Selecting a whole line takes its line break too, which is not part of the
    // quote and would count as a second line. Leading indent can be meaningful.
    readRange.current = range.cloneRange();
    const backward = selection.focusNode === range.startContainer && selection.focusOffset === range.startOffset && !selection.isCollapsed;
    onSelect(text.replace(/^(?:[ \t]*\r?\n)+/, '').trimEnd(), range.cloneRange(), backward);
  });

  useEffect(() => {
    if (!enabled) return;
    let timer: ReturnType<typeof setTimeout> | undefined;
    let settling: ReturnType<typeof setTimeout> | undefined;
    let pressed = false;
    const onMouseDown = () => {
      pressed = true;
    };
    const onMouseUp = (event: MouseEvent) => {
      pressed = false;
      if (isQuoteSurface(event.target)) return;
      clearTimeout(timer);
      // The browser finalizes the selection after mouseup.
      timer = setTimeout(read, 10);
    };
    const onSelectionChange = () => {
      clearTimeout(settling);
      if (pressed) return;
      // Read again, the same selection would reopen a menu someone is using.
      settling = setTimeout(() => {
        if (!alreadyRead()) read();
      }, SETTLE_MS);
    };
    document.addEventListener('mousedown', onMouseDown);
    document.addEventListener('mouseup', onMouseUp);
    document.addEventListener('selectionchange', onSelectionChange);
    return () => {
      clearTimeout(timer);
      clearTimeout(settling);
      document.removeEventListener('mousedown', onMouseDown);
      document.removeEventListener('mouseup', onMouseUp);
      document.removeEventListener('selectionchange', onSelectionChange);
    };
  }, [enabled]);
}
