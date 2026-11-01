import React, { useState } from 'react';
import { FileDown, Link2, RefreshCw } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useSearchParams } from 'react-router';
import { ThreadLifecycleFeed } from '@/lib/threadLifecycle/ThreadLifecycleFeed';
import PublishDialog from '../ChatAgent/components/PublishDialog';
import {
  useArtifactVersionView, versionReducer, type VersionAction, type VersionStore,
} from '../ChatAgent/components/filePanel/useArtifactVersionView';
import { ArtifactFrames } from '../ChatAgent/components/viewers/html/ArtifactFrames';
import { ArtifactVersionPicker } from '../ChatAgent/components/viewers/html/ArtifactVersionPicker';
import { CommentTools } from '../ChatAgent/components/viewers/html/comments/CommentTools';
import { useCommentUi } from '../ChatAgent/components/viewers/html/comments/useCommentUi';
import { exportServedPdf } from '../ChatAgent/components/viewers/html/useHtmlActions';
import { buildServeUrl } from '../ChatAgent/components/viewers/html/wsfilesUrl';
import { shareLinkHref } from '../ChatAgent/utils/api/shareLinks';
import { ShareTopBar, ShareTopBarAction } from './ShareTopBar';
import type { SharedOwnerFileMetadata } from './api';
import './SharePage.css';

/** The one file this route shows stands where a panel's tab id would. */
const VIEW_ID = 'artifact-view';

/** `?v=` as a version, or null for the live file when it names none. */
function versionParam(raw: string | null): number | null {
  const v = Number(raw);
  return raw && Number.isInteger(v) && v >= 1 ? v : null;
}

/**
 * The page's version in its address, `?v=`. A move replaces the entry rather
 * than adding a step, so Back leaves the page instead of walking its versions.
 */
function useAddressVersionStore(): VersionStore {
  const [params, setParams] = useSearchParams();
  const [updatedTo, setUpdatedTo] = useState<number | null>(null);
  const state = { version: versionParam(params.get('v')), updatedTo };
  return {
    state,
    dispatch: (action: VersionAction) => {
      const next = versionReducer(state, action);
      if (next.version !== state.version) {
        setParams(next.version == null ? {} : { v: String(next.version) }, { replace: true });
      }
      setUpdatedTo(next.updatedTo);
    },
  };
}

/**
 * The owner's own HTML file at its `/a/` link: the live file, with comments,
 * in a tab of its own. `?v=` opens a published version, read only.
 *
 * No chat sits beside it, so a comment sent from here starts a turn on its
 * destination (the bound thread, else the one whose agent wrote the file,
 * else a new one) and the card links there.
 */
export default function OwnerArtifactView({ code, metadata }: {
  code: string;
  metadata: SharedOwnerFileMetadata;
}): React.ReactElement {
  const { t } = useTranslation();
  const { path, name, workspace_id: workspaceId, frame_base: frameBase } = metadata;
  // The record moves with a publish made here or anywhere, so the view reads
  // it rather than the link's answer, which is only where the page started.
  const versionStore = useAddressVersionStore();
  const versionView = useArtifactVersionView(workspaceId, path, versionStore);
  const [reloads, setReloads] = useState(0);
  const [publishOpen, setPublishOpen] = useState(false);
  const [pdfBusy, setPdfBusy] = useState(false);
  const ui = useCommentUi();

  const savePdf = async () => {
    if (pdfBusy) return;
    setPdfBusy(true);
    try {
      await exportServedPdf({
        filePath: path,
        servedUrl: buildServeUrl(frameBase, path),
        // The print fallback opens this page again, never the served URL,
        // which carries the grant.
        openUrl: shareLinkHref(code),
        printHint: t('filePanel.pdfPrintHint'),
        generatingHint: t('filePanel.pdfGenerating'),
      });
    } finally {
      setPdfBusy(false);
    }
  };

  return (
    <div className="share-page">
      {/* Outside the app's shell, so nothing else hears the agent's replies here. */}
      <ThreadLifecycleFeed />
      <ShareTopBar
        name={name}
        actions={(
          <>
            <CommentTools tabId={VIEW_ID} workspaceId={workspaceId} path={path} ui={ui} variant="bar" modeDisabled={versionView.version !== null} />
            <ArtifactVersionPicker view={versionView} variant="bar" />
            <ShareTopBarAction icon={RefreshCw} label={t('filePanel.reloadPage')} onClick={() => setReloads((n) => n + 1)} />
            <ShareTopBarAction icon={FileDown} label={t('filePanel.saveAsPdf')} onClick={() => void savePdf()} disabled={pdfBusy} />
            <ShareTopBarAction icon={Link2} label={t('artifact.publish')} onClick={() => setPublishOpen(true)} />
          </>
        )}
      />
      <PublishDialog open={publishOpen} workspaceId={workspaceId} path={path} onClose={() => setPublishOpen(false)} />
      <div className="share-content">
        <ArtifactFrames
          // The agent's edits reach the page on a reload: no panel here reads
          // the file's bytes to notice them.
          revision={`r${reloads}`}
          fileName={name}
          workspaceId={workspaceId}
          filePath={path}
          prefix={frameBase}
          view={{ tabId: VIEW_ID, ui, active: true, versionView }}
        />
      </div>
    </div>
  );
}
