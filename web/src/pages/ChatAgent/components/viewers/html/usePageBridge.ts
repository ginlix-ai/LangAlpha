import { useCallback, useEffect, useEffectEvent, useLayoutEffect, useMemo, useRef, useState } from 'react';
import { resolveThemeVars } from './buildHtmlSrcDoc';
import { parsePageMessage, type PageToViewer, type ViewerToPage } from './pageProtocol';

/** The viewer's end of the bridge, handed to whatever draws over the frame. */
export interface PageBridge {
  /** The page's bridge has said hello since the frame last loaded, so a post reaches it. */
  ready: boolean;
  /**
   * Counts the hellos. A page that links to another of its files loads a new
   * document in the same frame, which knows nothing it was told before, so
   * whatever the viewer keeps the page told is sent again when this moves.
   */
  session: number;
  /**
   * The workspace path of the document the frame holds, as its hello said:
   * a link inside a report loads another of its files in the same frame.
   * Null when the hello named none.
   */
  path: string | null;
  /** Send to the page. Dropped until it is ready: before then the frame may hold another document. */
  post: (message: ViewerToPage) => void;
  /**
   * Hear every message the page sends that parses (`parsePageMessage`). For
   * what is drawn over the frame, which comes and goes with its own state;
   * returns the unsubscribe.
   */
  subscribe: (listener: PageBridgeListener) => () => void;
}

export type PageBridgeListener = (message: PageToViewer, bridge: PageBridge) => void;

interface UsePageBridgeOptions {
  frameRef: React.RefObject<HTMLIFrameElement | null>;
  /** The origin the frame posts from: the page's own, or `'null'` for an opaque fallback frame. */
  origin: string | null;
  /** Changes whenever the frame loads another document, which has to say hello again. */
  frameKey: string | null;
}

/**
 * The parent end of a framed document's postMessage bridge. The page says
 * `page:ready` as its first script runs; the answer is the app's theme, and
 * a theme switch is sent again for as long as the page is up.
 *
 * A message counts only when it comes from this frame's window and, for a
 * page on its own origin, from that origin. The opaque fallback cannot be
 * named, so it is told apart by its window alone, and posts to it go to `'*'`.
 */
export function usePageBridge({ frameRef, origin, frameKey }: UsePageBridgeOptions): PageBridge {
  const [hello, setHello] = useState<{ key: string | null; count: number; path: string | null }>({ key: null, count: 0, path: null });
  const ready = frameKey !== null && hello.key === frameKey;
  // `ready` for posts, which run outside render: the key whose document said
  // hello against the key framed now. Both are facts, not a flag an effect
  // clears, so an effect run again without a reload (a hot update, a key
  // that comes back to a frame kept on screen) cannot leave `ready` true
  // and every post dropped. Set in a layout effect, before a new frame can
  // say hello.
  const heardKey = useRef<string | null>(null);
  const framedKey = useRef(frameKey);
  useLayoutEffect(() => {
    framedKey.current = frameKey;
  }, [frameKey]);

  const post = useCallback((message: ViewerToPage) => {
    const win = frameRef.current?.contentWindow;
    if (!win || !origin || heardKey.current === null || heardKey.current !== framedKey.current) return;
    win.postMessage(message, origin === 'null' ? '*' : origin);
  }, [frameRef, origin]);

  const listeners = useRef(new Set<PageBridgeListener>());
  const subscribe = useCallback((listener: PageBridgeListener) => {
    const all = listeners.current;
    all.add(listener);
    return () => { all.delete(listener); };
  }, []);

  const session = hello.count;
  const path = ready ? hello.path : null;
  const bridge = useMemo<PageBridge>(() => ({ ready, session, path, post, subscribe }), [ready, session, path, post, subscribe]);

  const pushTheme = useCallback(() => post({ type: 'page:theme', css: resolveThemeVars() }), [post]);

  const onMessage = useEffectEvent((e: MessageEvent) => {
    const win = frameRef.current?.contentWindow;
    if (!win || e.source !== win || !origin || e.origin !== origin) return;
    const message = parsePageMessage(e.data);
    if (!message) return;
    if (message.type === 'page:ready') {
      heardKey.current = frameKey;
      setHello((prev) => ({ key: frameKey, count: prev.count + 1, path: message.path }));
      pushTheme();
    }
    for (const listener of listeners.current) listener(message, bridge);
  });

  useEffect(() => {
    const listener = (e: MessageEvent) => onMessage(e);
    window.addEventListener('message', listener);
    return () => window.removeEventListener('message', listener);
  }, []);

  // The page holds the theme it was sent, so a switch in the app is sent too.
  const onThemeToggle = useEffectEvent(pushTheme);
  useEffect(() => {
    const observer = new MutationObserver(() => onThemeToggle());
    observer.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => observer.disconnect();
  }, []);

  return bridge;
}
