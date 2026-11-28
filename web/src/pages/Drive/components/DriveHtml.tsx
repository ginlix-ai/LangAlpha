import React, { Suspense, useEffect, useEffectEvent, useRef, useState } from 'react';
import { useDrivePage } from '@/hooks/useDriveComments';
import { cn } from '@/lib/utils';
import type { ViewerCommentsTarget } from '@/pages/ChatAgent/components/viewers/comments/ViewerComments';
import { CodeFile } from '@/pages/ChatAgent/components/viewers/comments/code/CodeFile';
import { LazyCommentLayer } from '@/pages/ChatAgent/components/viewers/html/comments/lazyCommentLayer';
import { SERVED_HTML_PERMISSIONS, SERVED_HTML_SANDBOX } from '@/pages/ChatAgent/components/viewers/html/sandbox';
import { useBridgeMessage, usePageBridge } from '@/pages/ChatAgent/components/viewers/html/usePageBridge';
import { useDriveContext } from '../driveContext';
import { DriveLoading } from './DriveLoading';
import '@/pages/ChatAgent/components/viewers/html/BridgedFrame.css';

/**
 * The longest the frame stays out of sight waiting for its `load`, which waits
 * on every script and font the page pulls: past it the page shows as it draws.
 */
const REVEAL_CAP_MS = 800;

/** The file and version a page is drawn from: its head, or one version looked back at. */
export interface DrivePage {
  entryId: string;
  /** The bytes' hash, which keys the text read with its bridge. */
  sha: string;
  seq: number | null;
  /**
   * The version those bytes are read at: `seq`, or for the head the version
   * the entry named with `sha`, so a save made since never lands under it.
   */
  at: number | null;
}

/**
 * An HTML file in the Drive. A workspace's HTML is served from the file
 * server's origin; a Drive file has no such origin, so the page frames its
 * markup in an opaque origin under the same sandbox tokens served HTML gets:
 * its scripts run, and it reads nothing of the app. A file that loads its
 * neighbours by relative path finds none here.
 *
 * The page is read with the bridge first in its head (`comments` where its
 * current content takes comments, `view` otherwise), so the comment layer
 * draws over it as it does over a workspace page. Should that read fail (a
 * page past what the server reads whole), the markup is framed as it is, with
 * no bridge and no comments, and `onUnbridged` says so, so nothing offers a
 * comment the page cannot take.
 */
export function DriveHtml({ content, name, source, page, comments, onUnbridged }: {
  content: string;
  name: string;
  source: boolean;
  page: DrivePage | null;
  comments: ViewerCommentsTarget | null;
  /** The page is framed with no bridge, or has one again. */
  onUnbridged?: (unbridged: boolean) => void;
}) {
  const { space } = useDriveContext();
  const read = useDrivePage(source ? null : space, page && { ...page, inject: comments ? 'comments' : 'view' });
  // Its source takes comments on its lines, bridge or none.
  const unbridged = !source && (!page || (!read.isPending && !read.data));
  const report = useEffectEvent((now: boolean) => onUnbridged?.(now));
  // Said again for each content: what hears it keeps it by the bytes it was said of.
  useEffect(() => report(unbridged), [unbridged, content]);
  if (source) {
    return (
      <div className="p-4">
        <CodeFile content={content} language="html" lineRange={null} comments={comments} />
      </div>
    );
  }
  // Pending covers a read still waiting to be asked (`useDrivePage`), which is not loading yet.
  if (page && read.isPending) {
    return <div className="drive-html"><DriveLoading /></div>;
  }
  const shown = read.data;
  // A new text is a new page, so it waits to be shown again.
  if (!page || !shown) return <DriveFrame key={content} docKey={null} doc={content} title={name} comments={null} />;
  const docKey = `${page.entryId}:${shown.sha}`;
  return (
    <DriveFrame
      // A new document is a new frame, which says hello again.
      key={`${docKey}:${comments ? 'c' : 'v'}`}
      docKey={docKey}
      doc={shown.doc}
      title={name}
      // A comment is made on the page on screen, which trails the head while
      // a new save of it is read.
      comments={comments && { ...comments, fileSha: shown.sha }}
    />
  );
}

/**
 * The page as a `srcDoc` frame. With a `docKey` it speaks the page bridge,
 * with the comment layer over it: its origin is opaque, so the bridge knows it
 * by its window alone (`usePageBridge` with origin `'null'`), and
 * `allow-same-origin` never joins its sandbox, since a `srcDoc` frame with it
 * would be the app. With none it is the markup as it is, on paper, no bridge:
 * what a page shows when its bridged read failed.
 */
export function DriveFrame({ docKey, doc, title, comments }: {
  /** Which document this is, one file at one content; null for the markup with no bridge. */
  docKey: string | null;
  doc: string;
  title: string;
  comments: ViewerCommentsTarget | null;
}) {
  const frameRef = useRef<HTMLIFrameElement>(null);
  // Out of sight until its document has loaded, or for a short while at
  // most: before that it is the blank paper a frame starts on, which flashed
  // white over a dark page before the report painted its own ground.
  const [shown, setShown] = useState(false);
  useEffect(() => {
    const cap = setTimeout(() => setShown(true), REVEAL_CAP_MS);
    return () => clearTimeout(cap);
  }, []);
  const bridge = usePageBridge({ frameRef, origin: docKey ? 'null' : null, frameKey: docKey });
  // The ground the page paints, drawn behind the frame and the layer.
  const [ground, setGround] = useState<string | null>(null);
  useBridgeMessage(bridge, 'page:ground', (message) => setGround(message.color));

  return (
    <div
      className={cn('drive-html', docKey && 'bridged-frame drive-html-page', shown && 'is-shown')}
      style={ground ? ({ '--page-ground': ground } as React.CSSProperties) : undefined}
    >
      {!shown && <DriveLoading />}
      <iframe
        ref={frameRef}
        className={docKey ? 'bridged-frame-doc' : cn('drive-html-frame', shown && 'is-shown')}
        data-state={docKey ? (shown ? 'shown' : 'pending') : undefined}
        title={title}
        srcDoc={doc}
        sandbox={SERVED_HTML_SANDBOX}
        allow={SERVED_HTML_PERMISSIONS}
        // A crafted link's query must not reach a page that reads its referrer.
        referrerPolicy="no-referrer"
        onLoad={() => setShown(true)}
      />
      {docKey && comments && (
        <div className="bridged-frame-overlay">
          {/* Its own boundary: the layer's chunk loading must not hide the page. */}
          <Suspense fallback={null}>
            <LazyCommentLayer
              tabId={comments.tabId}
              workspaceId={comments.workspaceId}
              target={comments}
              bridge={bridge}
              view={{ key: docKey, shown }}
              active={comments.active}
              ui={comments.ui}
              fileSha={comments.fileSha}
            />
          </Suspense>
        </div>
      )}
    </div>
  );
}
