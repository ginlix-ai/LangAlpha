import { useEffect, useEffectEvent, useMemo, useState } from 'react';
import { createPortal } from 'react-dom';
import { BridgedFrame } from './BridgedFrame';
import { useLiveFrame } from './liveFrame';
import { SETTLE_CAP_MS } from './pageProtocol';
import { SCOUT_SANDBOX } from './sandbox';
import { useBridgeMessage } from './usePageBridge';

/**
 * How long the review waits on the page once it has loaded. The bridge's own
 * cap runs from `load` too, so this outlasts it: released first, Publish would
 * open on every page that never goes still just before its report arrived.
 */
const SCOUT_CAP_MS = SETTLE_CAP_MS + 2000;
/**
 * A page that never finishes loading (an image from a host that never answers)
 * still lets the review go. The scout stays until the dialog closes, so what
 * the page reports later still reads the list again before a publish.
 */
const SCOUT_START_CAP_MS = 30_000;

/** It runs as it would in the viewer, and only while the owner is reviewing it. */
export function PageScout({ workspaceId, path, onSettled, onFailed }: {
  workspaceId: string;
  path: string;
  /** Its first load is done, or not coming: what it found is on the list. */
  onSettled: () => void;
  /** Its grant never came, so the page never ran: the list lacks what its code builds. */
  onFailed: () => void;
}) {
  // Under a grant of its own, its report standing for the page: a frame the
  // panel showed earlier may hold files the agent has changed since, in its
  // frame or in the browser's cache.
  const live = useLiveFrame({ workspaceId, filePath: path, inject: 'view', ownGrant: true, scout: true });
  const source = useMemo(() => live.source && { ...live.source, sandbox: SCOUT_SANDBOX }, [live.source]);
  useBridgeMessage(live.bridge, 'page:settled', () => onSettled());
  const settled = useEffectEvent(onSettled);
  // From the document's `load`, not from here or its hello: a slow grant or
  // a slow image would otherwise spend the wait before the page settled.
  const [loaded, setLoaded] = useState(false);
  useEffect(() => {
    if (!loaded) return;
    const cap = setTimeout(() => settled(), SCOUT_CAP_MS);
    return () => clearTimeout(cap);
  }, [loaded]);
  useEffect(() => {
    const cap = setTimeout(() => settled(), SCOUT_START_CAP_MS);
    return () => clearTimeout(cap);
  }, []);
  const failed = useEffectEvent(onFailed);
  useEffect(() => {
    if (live.failed) failed();
  }, [live.failed]);
  // Clear rather than hidden, or the browser paints nothing and the page never
  // says it settled. On the body, since the dialog's transform would hold it.
  return createPortal(
    <div aria-hidden inert className="fixed left-0 top-0 h-[800px] w-[1280px] opacity-0 pointer-events-none">
      <BridgedFrame
        source={source}
        title=""
        groundKey={`scout:${workspaceId}:${path}`}
        onBridge={live.onBridge}
        onUnbridged={onSettled}
        onLoad={() => setLoaded(true)}
      />
    </div>,
    document.body,
  );
}
