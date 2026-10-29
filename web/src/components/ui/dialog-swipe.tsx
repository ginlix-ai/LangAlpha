import { useEffect } from 'react';
import { useSwipeToDismiss } from '@/hooks/useSwipeToDismiss';

interface DialogSwipeProps {
  container: HTMLDivElement | null;
  content: HTMLDivElement | null;
  handle: HTMLDivElement | null;
  onDismiss: () => void;
}

/**
 * Swipe to dismiss for DialogContent's mobile sheet, loaded the first time a
 * sheet opens. The gesture runs on framer motion values, and the dialogs the
 * app shell mounts closed on first paint would otherwise put framer on the
 * entry chunk. The sheet renders without this, so it opens at once and only
 * the gesture waits for the chunk.
 */
export default function DialogSwipe({ container, content, handle, onDismiss }: DialogSwipeProps) {
  const { contentRef, handleRef, dragY } = useSwipeToDismiss({ onDismiss });

  // The hook takes its nodes through callback refs, and the sheet owns them.
  useEffect(() => {
    contentRef(content);
    handleRef(handle);
  }, [content, handle, contentRef, handleRef]);

  // The CSS `translate` property, so a drag composes with the `transform` the
  // sheet-in keyframes animate instead of fighting it.
  useEffect(() => {
    if (!container) return;
    return dragY.on('change', (v) => {
      container.style.translate = `0 ${v}px`;
    });
  }, [dragY, container]);

  return null;
}
