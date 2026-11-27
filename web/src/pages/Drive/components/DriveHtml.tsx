import { useEffect, useState } from 'react';
import { cn } from '@/lib/utils';
import { SERVED_HTML_PERMISSIONS, SERVED_HTML_SANDBOX } from '@/pages/ChatAgent/components/viewers/html/sandbox';
import { CodeFile } from '@/pages/ChatAgent/components/viewers/comments/code/CodeFile';
import { DriveLoading } from './DriveLoading';

/**
 * The longest the frame stays out of sight waiting for its `load`, which waits
 * on every script and font the page pulls: past it the page shows as it draws.
 */
const REVEAL_CAP_MS = 800;

/**
 * An HTML file in the Drive, drawn from its own text. A workspace's HTML is
 * served from the file server's origin; a Drive file has no such origin, so
 * the page frames its markup in an opaque origin under the same sandbox
 * tokens served HTML gets: its scripts run, and it reads nothing of the app.
 * A file that loads its neighbours by relative path finds none here.
 */
export function DriveHtml({ content, name, source }: { content: string; name: string; source: boolean }) {
  if (source) {
    return (
      <div className="p-4">
        <CodeFile content={content} language="html" lineRange={null} comments={null} />
      </div>
    );
  }
  // A new text is a new page, so it waits to be shown again.
  return <HtmlFrame key={content} content={content} name={name} />;
}

/**
 * The frame stays out of sight until its document has loaded, or for a short
 * while at most: before that it is the blank paper a frame starts on, which
 * flashed white over a dark page before the report painted its own ground.
 */
function HtmlFrame({ content, name }: { content: string; name: string }) {
  const [shown, setShown] = useState(false);
  useEffect(() => {
    const cap = setTimeout(() => setShown(true), REVEAL_CAP_MS);
    return () => clearTimeout(cap);
  }, []);
  return (
    <div className="drive-html">
      {!shown && <DriveLoading />}
      <iframe
        className={cn('drive-html-frame', shown && 'is-shown')}
        title={name}
        srcDoc={content}
        sandbox={SERVED_HTML_SANDBOX}
        allow={SERVED_HTML_PERMISSIONS}
        referrerPolicy="no-referrer"
        onLoad={() => setShown(true)}
      />
    </div>
  );
}
