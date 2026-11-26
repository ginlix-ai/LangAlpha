import { useCallback, useEffect, useEffectEvent, useLayoutEffect, useRef, useState } from 'react';
import { retryAfterRefusal, userActedInFrame, watchFrameActivation } from './frameActivation';
import { paletteCss, readThemePalette } from './frameTheme';
import { postToWidget, readWidgetMessage, type FramePrompts, type WidgetStorageWrite } from './widgetProtocol';
import { subscribeWidgetStorage, writeWidgetStorage, type WidgetStorageListener } from './widgetStorage';

interface UseHtmlSandboxOptions {
  iframeRef: React.RefObject<HTMLIFrameElement | null>;
  /** When true, listen for widget:resize and expose the reported height. */
  autoHeight: boolean;
  /**
   * What each prompt call does: `sendPrompt(text, data)` sends `text` as the
   * user's next message, `draftPrompt` leaves it in the message box, unsent,
   * `startThread` opens a new thread whose first message is `text`.
   */
  prompts?: FramePrompts;
  /**
   * Where the frame's localStorage writes are kept (`widgetStorage`), and
   * whose writes from the widget's other open frames it is handed.
   */
  storageId?: string;
  /** A `data-zoom` click: an http(s) or data:image URL to show in the app's lightbox. */
  onZoom?: (src: string, alt?: string) => void;
}

interface UseHtmlSandboxResult {
  /** Reported iframe height (null until first resize), only set when autoHeight. */
  height: number | null;
  /** Push current theme vars into the iframe (call after a fresh srcdoc loads). */
  pushTheme: () => void;
  /**
   * The frame's `onLoad`. Writes another frame makes while this one's
   * document is still loading would reach no listener, so they wait for it.
   */
  onFrameLoad: () => void;
  /**
   * When a prompt call or zoom the user just tried will count, after
   * `userActedInFrame` refused it inside the activation window
   * (`FrameActionHint` shows it); null once one is accepted.
   */
  retryAt: number | null;
}

/**
 * Owns the parent-side postMessage bridge for a srcdoc widget frame (a served
 * document speaks the page bridge instead, `usePageBridge`):
 * resize → height (when autoHeight), the prompt calls and zoom → callbacks,
 * storage → `widgetStorage` and the widget's other open frames,
 * and a theme MutationObserver that re-pushes widget:themeUpdate on data-theme
 * changes. Diagnostics (`widget:diag`) and inspect answers are for the
 * headless render; the chat ignores them.
 *
 * The prompt calls and zoom act as the user, so they are answered only when
 * the user just acted in this frame (`userActedInFrame`): the frame checks
 * too, but its check runs where the widget's code can go around it.
 */
export function useHtmlSandbox({
  iframeRef,
  autoHeight,
  prompts,
  storageId,
  onZoom,
}: UseHtmlSandboxOptions): UseHtmlSandboxResult {
  const [height, setHeight] = useState<number | null>(null);
  const [retryAt, setRetryAt] = useState<number | null>(null);
  const sync = useRef<{ loaded: boolean; queue: WidgetStorageWrite[]; listener: WidgetStorageListener | null }>({
    loaded: false,
    queue: [],
    listener: null,
  });

  useEffect(() => watchFrameActivation(), []);

  // A layout effect, so the frame is listed before any message can arrive
  // after the render that read its seed.
  useLayoutEffect(() => {
    if (!storageId) return;
    const state = sync.current;
    const listener: WidgetStorageListener = (write) => {
      if (!state.loaded) state.queue.push(write);
      else postToWidget(iframeRef.current?.contentWindow, { type: 'widget:storageSync', write });
    };
    state.listener = listener;
    const off = subscribeWidgetStorage(storageId, listener);
    return () => {
      off();
      state.listener = null;
      state.queue = [];
    };
  }, [storageId, iframeRef]);

  const onFrameLoad = useCallback(() => {
    // The blank document a frame holds before its srcdoc is set loads too.
    const frame = iframeRef.current;
    if (!frame?.getAttribute('srcdoc')) return;
    const state = sync.current;
    state.loaded = true;
    const queued = state.queue;
    state.queue = [];
    for (const write of queued) postToWidget(frame.contentWindow, { type: 'widget:storageSync', write });
  }, [iframeRef]);

  const pushTheme = useCallback(() => {
    postToWidget(iframeRef.current?.contentWindow, {
      type: 'widget:themeUpdate',
      css: paletteCss(readThemePalette()),
    });
  }, [iframeRef]);

  // An event, so the listener is added once rather than each time a host
  // hands over a fresh `prompts` object.
  // Whether the user acted in the frame, keeping the hint in step: an
  // accepted call clears it, a refused one inside the window sets it.
  const acted = (frame: HTMLIFrameElement) => {
    if (userActedInFrame(frame)) {
      setRetryAt(null);
      return true;
    }
    const at = retryAfterRefusal(frame);
    if (at != null) setRetryAt(at);
    return false;
  };

  const onMessage = useEffectEvent((e: MessageEvent) => {
    // Only this frame's own document: every widget on the page posts the
    // same shapes, and a window with no tie to the frame must not reach these.
    const frame = iframeRef.current;
    if (!frame || e.source !== frame.contentWindow) return;
    const msg = readWidgetMessage(e.data);
    if (!msg) return;

    switch (msg.type) {
      case 'widget:resize':
        if (autoHeight) {
          const next = Math.ceil(msg.height);
          setHeight((prev) => (prev === next ? prev : next));
        }
        return;
      case 'widget:sendPrompt':
      case 'widget:draftPrompt':
      case 'widget:startThread': {
        const handler = prompts?.[msg.call];
        if (handler && acted(frame)) handler(msg.text, msg.data);
        return;
      }
      case 'widget:storage':
        if (storageId) writeWidgetStorage(storageId, msg.write, sync.current.listener ?? undefined);
        return;
      case 'widget:zoom':
        if (onZoom && acted(frame)) onZoom(msg.src, msg.alt);
        return;
      default:
        return;
    }
  });

  useEffect(() => {
    const listener = (e: MessageEvent) => onMessage(e);
    window.addEventListener('message', listener);
    return () => window.removeEventListener('message', listener);
  }, []);

  // Re-push theme vars whenever the app toggles data-theme.
  const onThemeToggle = useEffectEvent(pushTheme);
  useEffect(() => {
    const observer = new MutationObserver(() => onThemeToggle());
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ['data-theme'],
    });
    return () => observer.disconnect();
  }, []);

  return { height, pushTheme, onFrameLoad, retryAt };
}
