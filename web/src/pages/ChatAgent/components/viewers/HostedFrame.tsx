import { useCallback, useEffect, useEffectEvent, useRef, type IframeHTMLAttributes, type ReactElement, type Ref } from 'react';
import { hostedFrameSrc, originOf } from '@/lib/framePolicy';
import { FRAME_HOST_PATH, framedDirectly } from './frameHost';

type FrameAttributes = Omit<IframeHTMLAttributes<HTMLIFrameElement>, 'src' | 'srcDoc' | 'sandbox' | 'title' | 'onLoad'>;

interface HostedFrameProps extends FrameAttributes {
  src: string;
  sandbox: string;
  title: string;
  /** The framed document loaded, as the frame's own load event would say. */
  onLoad?: () => void;
  /** Pass messages both ways through the host, for a document that talks to the app (a page's bridge). */
  relay?: boolean;
  /** The element the app lays out: the host when there is one. */
  ref?: Ref<HTMLIFrameElement>;
}

/**
 * A sandboxed frame on any origin, kept to that origin.
 *
 * index.html's frame-src names only origins known at build, and a meta policy
 * cannot be widened once parsed. A frame on one known only at runtime (a
 * running app, a page on the preview base) is put inside a frame host of its
 * own instead, `public/frame-host.html` on the app's origin, whose policy names
 * that one origin: the frame can move about its own origin and nowhere else.
 */
export function HostedFrame({ relay = false, ...props }: HostedFrameProps): ReactElement {
  if (framedDirectly(props.src)) {
    const { onLoad, ...rest } = props;
    return <iframe {...rest} onLoad={onLoad ? () => onLoad() : undefined} />;
  }
  // A host's policy is fixed once written, so another address gets a new host.
  return <FrameHost key={props.src} relay={relay} {...props} />;
}

function FrameHost({ src, sandbox, title, onLoad, relay, ref, tabIndex, ...rest }: HostedFrameProps & { relay: boolean }) {
  const hostRef = useRef<HTMLIFrameElement | null>(null);
  const innerRef = useRef<HTMLIFrameElement | null>(null);
  const setHost = useCallback((el: HTMLIFrameElement | null) => {
    hostRef.current = el;
    if (typeof ref === 'function') ref(el);
    else if (ref) ref.current = el;
    return () => {
      hostRef.current = null;
      if (typeof ref === 'function') ref(null);
      else if (ref) ref.current = null;
    };
  }, [ref]);

  const loaded = useEffectEvent(() => onLoad?.());
  const describe = useEffectEvent((frame: HTMLIFrameElement) => {
    frame.title = title;
    if (tabIndex === undefined) frame.removeAttribute('tabindex');
    else frame.tabIndex = tabIndex;
  });

  useEffect(() => {
    const host = hostRef.current;
    if (!host) return;
    // Before the host loads, the element holds its initial about:blank, which
    // carries no marker. Checked rather than assumed after load too: a server
    // that answered the path with something else gets nothing written into it.
    const hostDocument = () => {
      const doc = host.contentDocument;
      return doc?.body?.hasAttribute('data-frame-host') ? doc : null;
    };
    // A document whose color-scheme differs from its frame's gets an opaque
    // backdrop, so the host follows the app's and the frame inside it meets
    // the scheme it would have met framed by the app directly.
    const matchScheme = () => {
      const doc = hostDocument();
      if (doc) doc.documentElement.style.colorScheme = getComputedStyle(host).colorScheme;
    };
    const fill = () => {
      const doc = hostDocument();
      const policy = hostedFrameSrc(src);
      const origin = originOf(src);
      if (!doc || !policy || !origin || doc.querySelector('iframe')) return;
      matchScheme();
      // The policy first: the frame must never exist under the host without it.
      const meta = doc.createElement('meta');
      meta.httpEquiv = 'Content-Security-Policy';
      meta.content = policy;
      doc.head.appendChild(meta);
      const frame = doc.createElement('iframe');
      frame.setAttribute('sandbox', sandbox);
      if (relay) frame.setAttribute('data-relay-origin', origin);
      describe(frame);
      frame.addEventListener('load', () => loaded());
      frame.src = src;
      doc.body.appendChild(frame);
      innerRef.current = frame;
    };
    host.addEventListener('load', fill);
    fill();
    const themes = new MutationObserver(matchScheme);
    themes.observe(document.documentElement, { attributes: true, attributeFilter: ['data-theme'] });
    return () => {
      host.removeEventListener('load', fill);
      themes.disconnect();
    };
  }, [src, sandbox, relay]);

  useEffect(() => {
    if (innerRef.current) describe(innerRef.current);
  }, [title, tabIndex]);

  return <iframe {...rest} ref={setHost} src={FRAME_HOST_PATH} title={title} tabIndex={tabIndex} />;
}
