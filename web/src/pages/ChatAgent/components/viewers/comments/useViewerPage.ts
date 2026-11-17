import { useCallback, useEffect, useMemo, useState } from 'react';
import type { FrameView } from '../html/BridgedFrame';
import type { PageSelection } from '../html/pageProtocol';
import type { PageBridge, PageBridgeListener } from '../html/usePageBridge';
import type { SpotStrategy } from './spotStrategy';
import { ViewerPage, type ViewerPageOptions } from './viewerPage';

interface Args {
  /** The viewer's scroll viewport, which the comment overlay covers. */
  scroller: HTMLElement | null;
  /** What it draws the file in: where spots are picked. */
  content: HTMLElement | null;
  strategy: SpotStrategy | null;
  path: string;
  title?: ViewerPageOptions['title'];
}

/**
 * A `PageBridge` over a viewer drawn in the app, for `CommentLayer`, with the
 * `FrameView` it reads beside it. The file is one document for as long as the
 * viewer shows it: an edit to it changes what the page finds, not the page.
 *
 * The page is made as soon as there is a viewer, and listens from the effect
 * after: the layer, drawn under this hook, posts its anchors from its own
 * effects, and the page answers them on the next frame.
 */
export function useViewerPage({ scroller, content, strategy, path, title }: Args): {
  bridge: PageBridge;
  view: FrameView;
  /** Words in the viewer as the pick a comment opens on (`ViewerPage.wordsPick`); left out where it takes none. */
  pickWords?: (range: Range) => PageSelection | null;
  /** Whether words are in the viewer at all (`ViewerPage.holds`). */
  holdsWords?: (range: Range) => boolean;
} {
  const [listeners] = useState(() => new Set<PageBridgeListener>());
  const subscribe = useCallback((listener: PageBridgeListener) => {
    listeners.add(listener);
    return () => { listeners.delete(listener); };
  }, [listeners]);
  // Made together, so what the page reports always names the bridge the layer holds.
  const live = useMemo(() => {
    if (!scroller || !content || !strategy) return null;
    const page: ViewerPage = new ViewerPage(scroller, content, strategy, {
      path,
      title,
      emit: (message) => {
        for (const listener of listeners) listener(message, bridge);
      },
    });
    const bridge: PageBridge = { ready: true, session: 1, path, post: (message) => page.handle(message), subscribe };
    return { page, bridge };
  }, [scroller, content, strategy, path, title, listeners, subscribe]);
  const idle = useMemo<PageBridge>(() => ({ ready: false, session: 0, path: null, post: () => {}, subscribe }), [subscribe]);
  const page = live?.page ?? null;
  useEffect(() => page?.attach(), [page]);

  const ready = page !== null;
  const view = useMemo<FrameView>(() => ({ key: path, shown: ready }), [path, ready]);
  const pickWords = useCallback((range: Range) => page?.wordsPick(range) ?? null, [page]);
  const holdsWords = useCallback((range: Range) => page?.holds(range) ?? false, [page]);
  const words = strategy?.words ? { pickWords, holdsWords } : {};
  return { bridge: live?.bridge ?? idle, view, ...words };
}
