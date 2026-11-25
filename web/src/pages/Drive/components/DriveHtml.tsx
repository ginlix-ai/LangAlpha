import { SERVED_HTML_PERMISSIONS, SERVED_HTML_SANDBOX } from '@/pages/ChatAgent/components/viewers/html/sandbox';
import { CodeFile } from '@/pages/ChatAgent/components/viewers/comments/code/CodeFile';

/**
 * An HTML file in the Drive, drawn from its own text. A workspace's HTML is
 * served from its computer under a grant; a Drive file has no such route, so
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
  return (
    <iframe
      className="drive-html-frame"
      title={name}
      srcDoc={content}
      sandbox={SERVED_HTML_SANDBOX}
      allow={SERVED_HTML_PERMISSIONS}
      referrerPolicy="no-referrer"
    />
  );
}
