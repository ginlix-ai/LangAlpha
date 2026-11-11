import { useEffect, useEffectEvent, useRef, type RefObject } from 'react';
import { messageOrigin } from '../../ChatAgent/components/viewers/frameHost';
import { parsePageMessage } from '../../ChatAgent/components/viewers/html/pageProtocol';

/** A window scroll offset in CSS pixels from the document's top left. */
export interface ScrollPlace {
  x: number;
  y: number;
}

export const TOP: ScrollPlace = { x: 0, y: 0 };

interface UsePageScrollOptions {
  /** The element the app lays out: the frame, or the frame host relaying for it. */
  frameRef: RefObject<HTMLIFrameElement | null>;
  src: string;
  sandbox: string;
  /** Where the document should open, asked at its hello and again by `restore`. */
  place: () => ScrollPlace;
  /** Where the reader has scrolled the document to. */
  onScroll: (at: ScrollPlace) => void;
}

/**
 * The scroll half of the page bridge (`bridge/core.js`) for a kept page's
 * frame. The bridge reports nothing until the frame's parent speaks, so the
 * document's hello is answered with where it should open, which also turns its
 * reports on. Only the first document the frame loads is answered: one a link
 * took the frame to is not the page whose place is kept.
 */
export function usePageScroll({ frameRef, src, sandbox, place, onScroll }: UsePageScrollOptions): { restore: () => void } {
  const greeted = useRef(false);

  const restore = () => {
    const win = frameRef.current?.contentWindow;
    if (!win || !greeted.current) return;
    const origin = messageOrigin(src, sandbox);
    const at = place();
    win.postMessage({ type: 'page:restore-scroll', x: at.x, y: at.y }, origin === 'null' ? '*' : origin);
  };

  const onMessage = useEffectEvent((event: MessageEvent) => {
    // Its window and its origin, as the page bridge's other hosts check: an
    // opaque document posts as 'null', so the window is what names it.
    const win = frameRef.current?.contentWindow;
    if (!win || event.source !== win || event.origin !== messageOrigin(src, sandbox)) return;
    const message = parsePageMessage(event.data);
    if (message?.type === 'page:ready') {
      if (greeted.current) return;
      greeted.current = true;
      restore();
    } else if (message?.type === 'page:scroll' && greeted.current) {
      onScroll({ x: message.x, y: message.y });
    }
  });

  useEffect(() => {
    const listener = (event: MessageEvent) => onMessage(event);
    window.addEventListener('message', listener);
    return () => window.removeEventListener('message', listener);
  }, []);

  return { restore };
}
