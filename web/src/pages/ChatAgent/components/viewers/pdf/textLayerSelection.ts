import { useEffect } from 'react';

const SELECTING = 'selecting';
/** On the root while a press selects a page's words: `PdfViewer.css` lets nothing else select then. */
const ROOT_MARK = 'pdfSelecting';

/**
 * A press selecting a page's words, held to the words. The page's text layer
 * is marked `selecting`, which stretches its `.endOfContent` over the page, so
 * the blank between lines holds the selection where it is: without it Chrome
 * extends the selection to the end of the page and back as the pointer crosses
 * each gap, and at some presses selects nothing at all. react-pdf marks it
 * from React's `onMouseDown`, which a capture listener that stops the press
 * (the comment mode's, `ViewerPage`) keeps from running, so the column marks
 * it from a capture listener of its own, on a mouse's or a pen's press. As in pdf.js's viewer, a layer the
 * selection runs into while the press lasts is marked too. The root is marked
 * for the press as well, so nothing but words selects until the release.
 */
export function holdTextLayers(column: HTMLElement): () => void {
  const listening = new AbortController();
  const { signal } = listening;
  let pressed = false;
  const root = document.documentElement;
  const release = () => {
    if (!pressed) return;
    pressed = false;
    delete root.dataset[ROOT_MARK];
    for (const layer of column.querySelectorAll('.textLayer')) layer.classList.remove(SELECTING);
  };

  // From the pointerdown, a mouse's or a pen's: it is the first event of
  // every press and says what pressed. A touch selects nothing by dragging,
  // and fires its mousedown only after its pointerup, which would leave the
  // page marked, and nothing else selectable, until the next press ended.
  column.addEventListener('pointerdown', (e) => {
    if (e.button !== 0 || e.pointerType === 'touch' || !(e.target instanceof Element)) return;
    const layer = e.target.closest('.textLayer');
    if (!layer || !column.contains(layer)) return;
    pressed = true;
    root.dataset[ROOT_MARK] = '';
    layer.classList.add(SELECTING);
  }, { capture: true, signal });
  document.addEventListener('selectionchange', () => {
    if (!pressed) return;
    const sel = document.getSelection();
    if (!sel?.rangeCount) return;
    const range = sel.getRangeAt(0);
    for (const layer of column.querySelectorAll('.textLayer')) {
      if (range.intersectsNode(layer)) layer.classList.add(SELECTING);
    }
  }, { signal });
  window.addEventListener('pointerup', release, { capture: true, signal });
  window.addEventListener('pointercancel', release, { capture: true, signal });
  // The window's own blur, not captured: the press itself blurs whatever had
  // the focus, and that blur reaches a capturing window listener too.
  window.addEventListener('blur', release, { signal });
  return () => {
    release();
    listening.abort();
  };
}

/** `holdTextLayers` over the pages column while it is drawn. */
export function useTextLayerSelection(column: HTMLElement | null): void {
  useEffect(() => (column ? holdTextLayers(column) : undefined), [column]);
}
