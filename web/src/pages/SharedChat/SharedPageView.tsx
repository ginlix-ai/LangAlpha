import React, { Suspense, useMemo, useState } from 'react';
import { Download } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { bodyMode } from '../ChatAgent/components/filePanel/fileBody';
import { BridgedFrame, type FrameSource } from '../ChatAgent/components/viewers/html/BridgedFrame';
import { ISOLATED_PAGE_SANDBOX, SERVED_HTML_SANDBOX } from '../ChatAgent/components/viewers/html/sandbox';
import { buildServeUrl, versionPrefix } from '../ChatAgent/components/viewers/html/wsfilesUrl';
import { ShareTopBar, ShareTopBarAction } from './ShareTopBar';
import { downloadServedFile, type SharedPublicFileMetadata } from './api';
import './SharePage.css';

// A visitor to an HTML version never loads the file viewers.
const FileBody = React.lazy(() => import('./SharedFileView').then((m) => ({ default: m.FileBody })));

interface SharedPageViewProps {
  code: string;
  metadata: SharedPublicFileMetadata;
  /** Read the link again: the version on screen has stopped answering, or may have. */
  onStale: () => void;
}

/**
 * The version in a frame, its bridge giving it the app's theme and taking no
 * comments. A load with no hello is a document that is not the version's,
 * most likely the 404 its files answer once an Update or an unshare ended it,
 * and the link is read again for what replaced it.
 */
function VersionFrame({ metadata, title, onStale }: {
  metadata: SharedPublicFileMetadata;
  title: string;
  onStale: () => void;
}) {
  const { artifact_id: artifactId, seq, path, frame_base: base, isolated } = metadata;
  const doc = `${seq}\u0000${path}`;
  // Keyed by the version: a renewed grant names the same one under a new
  // base, and the document on screen keeps the address it loaded from. That
  // address lapses with its grant, so a frame that lost its document on one
  // since renewed loads the renewed one, once: then it is on the current base.
  const [loaded, setLoaded] = useState({ doc, base, reloads: 0 });
  if (loaded.doc !== doc) setLoaded({ doc, base, reloads: loaded.reloads });
  const source = useMemo<FrameSource>(() => ({
    key: `${doc}\u0000${loaded.reloads}`,
    src: buildServeUrl(versionPrefix(base, seq), path),
    sandbox: isolated ? ISOLATED_PAGE_SANDBOX : SERVED_HTML_SANDBOX,
  }), [doc, loaded.reloads, seq, path, base, isolated]);
  const lost = () => {
    onStale();
    if (loaded.base !== base) setLoaded({ doc, base, reloads: loaded.reloads + 1 });
  };

  return <BridgedFrame source={source} title={title} groundKey={artifactId} onUnbridged={lost} />;
}

/**
 * A file behind its `/a/` link, for anyone but its owner: the version that
 * was latest when the link was read, read only. No comments and no other
 * versions: those are the owner's. The page reads the link again when the
 * tab comes back and before the grant lapses; a new version then replaces
 * this one, and a stopped link takes the page away.
 */
export default function SharedPageView({ code, metadata, onStale }: SharedPageViewProps): React.ReactElement {
  const { t } = useTranslation();
  const { name, path, seq, frame_base: base } = metadata;
  const label = metadata.title || name;
  const isHtml = bodyMode(path) === 'html';
  const prefix = versionPrefix(base, seq);
  const [busy, setBusy] = useState(false);

  const download = () => {
    setBusy(true);
    downloadServedFile(buildServeUrl(prefix, path), name)
      .catch((err: unknown) => {
        console.error('[SharedPageView] Download failed:', err);
        toast({ description: t('filePanel.downloadFailed'), variant: 'destructive' });
        // Most likely the version ended under the reader.
        onStale();
      })
      .finally(() => setBusy(false));
  };

  return (
    <div className="share-page">
      <ShareTopBar
        name={label}
        actions={isHtml
          ? undefined
          : <ShareTopBarAction icon={Download} label={t('filePanel.download')} onClick={download} disabled={busy} />}
      />
      <div className="share-content">
        {isHtml
          ? <VersionFrame metadata={metadata} title={label} onStale={onStale} />
          : (
            <Suspense fallback={null}>
              {/* Scoped by the version too: an Update keeps the path and changes the bytes. */}
              <FileBody scope={`share:${code}:v${seq}`} path={path} frameBase={prefix} onDownload={download} downloading={busy} onGone={onStale} />
            </Suspense>
          )}
      </div>
    </div>
  );
}
