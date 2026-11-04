import { originOf } from '@/lib/framePolicy';

/**
 * On the app's own origin whatever `VITE_CDN_BASE` says, since the app reaches
 * into it; served as is from `public/`.
 */
export const FRAME_HOST_PATH = '/frame-host.html';

const API_ORIGIN = originOf(import.meta.env.VITE_API_BASE_URL);

/**
 * Whether index.html's frame-src already lets a frame load `src`: the app's
 * own origin, or an absolute API base's.
 */
export function framedDirectly(src: string): boolean {
  const origin = originOf(src, window.location.href);
  return origin !== null && (origin === window.location.origin || origin === API_ORIGIN);
}

/**
 * Moves a directly framed document to `url` in place of the one it holds,
 * with no step of its own in the browser's history.
 */
export function sendFrameTo(frame: HTMLIFrameElement, url: string): void {
  frame.contentWindow?.location.replace(url);
}

/**
 * The origin a frame's messages arrive from: the app's when a frame host
 * relays them, else the document's own, which a sandbox without
 * `allow-same-origin` makes opaque (`'null'`).
 */
export function messageOrigin(src: string, sandbox: string): string {
  if (!framedDirectly(src)) return window.location.origin;
  if (!sandbox.split(/\s+/).includes('allow-same-origin')) return 'null';
  return originOf(src, window.location.href) ?? 'null';
}
