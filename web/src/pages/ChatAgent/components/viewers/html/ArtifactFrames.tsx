import React, { Suspense, useMemo, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, AlertTriangle } from 'lucide-react';
import { Button } from '@/components/ui/button';
import { useArtifactGrant } from '@/hooks/useArtifact';
import type { ArtifactVersionView } from '../../filePanel/useArtifactVersionView';
import { BridgedFrame, type FrameSource } from './BridgedFrame';
import { LazyCommentLayer as CommentLayer } from './comments/lazyCommentLayer';
import type { CommentUi } from './comments/useCommentUi';
import type { PageBridge } from './usePageBridge';
import { ISOLATED_PAGE_SANDBOX, SERVED_HTML_SANDBOX } from './sandbox';
import { useFragmentScroll, useLiveFrame } from './liveFrame';
import { buildServeUrl, versionPrefix } from './wsfilesUrl';
import './ArtifactFrames.css';

/**
 * The owner's view of an HTML file: the live file with the comment layer, or
 * a published version, read only. Absent on a share and in a read-only panel,
 * which show the file alone.
 */
export interface HtmlArtifactView {
  /** The tab the comment controls are held for. */
  tabId: string;
  ui: CommentUi;
  /** The tab is in front of an active panel: keys and asks are its. */
  active: boolean;
  /** The record, its versions and which one is on screen (`useArtifactVersionView`). */
  versionView: ArtifactVersionView;
}

interface ArtifactFramesProps {
  /**
   * Which bytes of the live file to show. A new one loads the file again,
   * over the old one with the place kept; the same one keeps the browser's
   * copy through a remount.
   */
  revision: string;
  fileName: string;
  workspaceId: string;
  /** Workspace-relative, or as the panel's tab holds it. */
  filePath: string;
  view: HtmlArtifactView;
  /** The frames are on screen: not under Source, where they wait unseen. */
  shown?: boolean;
  /**
   * The serve prefix to frame the live file under: the standalone view's
   * `frame_base`. Omitted, the workspace's file grant.
   */
  prefix?: string | null;
  /** Element id a reference pointed at, and the count that makes a repeat ask a new one. */
  anchor?: string | null;
  anchorSeq?: number | null;
}

/** The owner's grant never arrived, so nothing will load until a retry. */
export function ServeFailed({ onRetry }: { onRetry: () => void }): React.ReactElement {
  const { t } = useTranslation();
  return (
    <div className="flex flex-col items-center justify-center gap-3 py-12 text-sm" role="alert">
      <span className="flex items-center gap-2" style={{ color: 'var(--color-text-secondary)' }}>
        <AlertCircle className="h-4 w-4 shrink-0" style={{ color: 'var(--color-icon-danger)' }} />
        {t('filePanel.htmlPreviewFailed')}
      </span>
      <Button variant="outline" size="sm" onClick={onRetry}>
        {t('common.retry')}
      </Button>
    </div>
  );
}

function VersionNotice({ title, hint, children }: { title: string; hint: string; children?: React.ReactNode }): React.ReactElement {
  return (
    <div className="flex flex-col items-center justify-center gap-3 px-6 py-12" role="alert">
      <AlertTriangle className="h-6 w-6" style={{ color: 'var(--color-text-tertiary)' }} />
      <p className="text-sm font-medium" style={{ color: 'var(--color-text-secondary)' }}>{title}</p>
      <p className="text-xs text-center max-w-xs" style={{ color: 'var(--color-text-tertiary)' }}>{hint}</p>
      {children && <div className="mt-1 flex gap-2">{children}</div>}
    </div>
  );
}

/**
 * The live file under the owner's grant with the comment bridge, and over it,
 * when one is picked, a published version from the artifact's grant, read
 * only. The live frame stays mounted, laid out and unseen, under a version,
 * so a comment being written and the place read survive a look at an older
 * version.
 */
export function ArtifactFrames({
  revision, fileName, workspaceId, filePath, view, shown = true, prefix: givenPrefix = null, anchor = null, anchorSeq = null,
}: ArtifactFramesProps): React.ReactElement {
  const { t } = useTranslation();
  const live = useLiveFrame({ workspaceId, filePath, prefix: givenPrefix, inject: 'comments', revision, anchor });
  const [liveBridge, setLiveBridge] = useState<PageBridge | null>(null);
  useFragmentScroll(liveBridge, anchor, anchorSeq);

  // A published version, framed from the artifact's own grant.
  const { record, version, entry } = view.versionView;
  const artifactId = record?.artifact_id ?? null;
  const versionGrant = useArtifactGrant(version !== null ? artifactId : null);
  const versionSource = useMemo<FrameSource | null>(() => {
    const g = versionGrant.data;
    if (!entry || !g || !artifactId) return null;
    return {
      key: `v:${artifactId}:${entry.seq}`,
      src: buildServeUrl(versionPrefix(g.base, entry.seq), entry.entry_path),
      sandbox: g.isolated ? ISOLATED_PAGE_SANDBOX : SERVED_HTML_SANDBOX,
    };
  }, [entry, versionGrant.data, artifactId]);

  let versionNotice: React.ReactNode = null;
  if (version !== null) {
    // A failed renewal leaves the frame on the grant it has, as the live view does.
    if ((versionGrant.error && !versionGrant.data) || view.versionView.failed) {
      versionNotice = (
        <VersionNotice title={t('pageViewer.failed')} hint={t('pageViewer.failedHint')}>
          <Button variant="outline" size="sm" onClick={() => { void versionGrant.refetch(); view.versionView.retry(); }}>
            {t('common.retry')}
          </Button>
        </VersionNotice>
      );
    } else if (view.versionView.missing) {
      versionNotice = (
        <VersionNotice title={t('pageViewer.versionNotFound', { seq: version })} hint={t('artifact.versionNotFoundHint')}>
          <Button variant="outline" size="sm" onClick={() => view.versionView.pick(null)}>{t('artifact.showLive')}</Button>
        </VersionNotice>
      );
    }
  }

  return (
    <div className="artifact-frames">
      <div className="artifact-frames-layer" data-shown={version === null}>
        {live.failed ? (
          <ServeFailed onRetry={live.retry} />
        ) : (
          <BridgedFrame
            source={live.source}
            title={fileName || 'HTML Preview'}
            groundKey={filePath}
            onBridge={setLiveBridge}
            overlay={(bridge, frameView) => (
              // Its own boundary: the layer's chunk loading must not hide the frame.
              <Suspense fallback={null}>
                <CommentLayer
                  tabId={view.tabId}
                  workspaceId={workspaceId}
                  path={filePath}
                  bridge={bridge}
                  view={frameView}
                  active={view.active && shown && version === null}
                  ui={view.ui}
                />
              </Suspense>
            )}
          />
        )}
      </div>
      {version !== null && (
        <div className="artifact-frames-layer">
          <BridgedFrame
            source={versionSource}
            title={fileName || 'HTML Preview'}
            groundKey={filePath}
            notice={versionNotice}
          />
        </div>
      )}
    </div>
  );
}
