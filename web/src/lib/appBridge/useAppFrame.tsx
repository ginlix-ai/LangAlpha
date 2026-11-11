import { useRef, type ReactElement, type RefObject } from 'react';
import { APP_PREVIEW_SANDBOX } from '@/pages/ChatAgent/components/viewers/html/sandbox';
import { AppBridge, type BridgeOptions } from './AppBridge';

export interface AppFrame {
  /** Spread on the app's `HostedFrame`. */
  frame: { ref: RefObject<HTMLIFrameElement | null>; src: string; sandbox: string; relay: boolean };
  /** The bridge and its consent bar, rendered where the bar should sit. */
  bridge: ReactElement | null;
}

/**
 * A frame and the bridge answering it, wired in one place: the bridge trusts
 * only the origin the frame's sandbox and host give its messages, so the two
 * must never disagree. With `bridge` null the frame is shown with nothing to
 * answer it. `sandbox` is a running app's unless the frame is a kept page's.
 */
export function useAppFrame(src: string, bridge: BridgeOptions | null, sandbox: string = APP_PREVIEW_SANDBOX): AppFrame {
  const ref = useRef<HTMLIFrameElement>(null);
  return {
    frame: { ref, src, sandbox, relay: bridge !== null },
    bridge: bridge && src ? <AppBridge frameRef={ref} src={src} sandbox={sandbox} {...bridge} /> : null,
  };
}
