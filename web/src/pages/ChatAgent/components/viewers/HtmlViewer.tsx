import React, { useMemo, useState } from 'react';
import { useTheme } from '@/contexts/ThemeContext';
import SyntaxHighlighter, { oneDark, oneLight } from '../SyntaxHighlighter';
import { ComposerDockSlot } from '../chatView/composerDock';
import { ArtifactFrames, ServeFailed, type HtmlArtifactView } from './html/ArtifactFrames';
import { BridgedFrame } from './html/BridgedFrame';
import { useFragmentScroll, useLiveFrame } from './html/liveFrame';
import type { PageBridge } from './html/usePageBridge';
import './HtmlViewer.css';

export type { HtmlArtifactView } from './html/ArtifactFrames';

interface HtmlViewerProps {
  /** Full source (read unlimited so Source isn't truncated). */
  content: string;
  fileName: string;
  workspaceId: string;
  /** Path within the workspace, e.g. "results/report.html". */
  filePath: string;
  /** The markup rather than the rendered page; the switch is in the panel header. */
  source?: boolean;
  /** The serve prefix a share's page was handed. Omitted, the owner's grant
   *  serves the file. */
  servePrefix?: string;
  /** Element id a reference pointed at; the iframe scrolls to it. */
  anchor?: string | null;
  /** Bumped by the panel on every reference open, so the same anchor asked for
   *  twice is two requests rather than one unchanged prop. Read only alongside
   *  `anchor`. */
  anchorSeq?: number | null;
  /** The owner's comments and versions. */
  artifact?: HtmlArtifactView | null;
}

/**
 * A short fingerprint of the file's bytes as the panel last read them. The
 * live frame is keyed on it, so the agent's edit, once read, loads the file
 * again (over the old one, scroll carried) while a remount of the same bytes
 * keeps the browser's copy.
 */
function contentHash(text: string): string {
  let h = 0x811c9dc5;
  for (let i = 0; i < text.length; i += 1) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 0x01000193);
  }
  return (h >>> 0).toString(36);
}

export default function HtmlViewer({
  content,
  fileName,
  workspaceId,
  filePath,
  source = false,
  servePrefix,
  anchor = null,
  anchorSeq = null,
  artifact = null,
}: HtmlViewerProps) {
  const { theme } = useTheme();
  const isLight = theme === 'light';
  // Handed only to the owner's panel, which decides that once (`PanelAccess`).
  const owner = !!artifact;
  const revision = useMemo(() => contentHash(content), [content]);

  return (
    <div className="html-viewer">
      {source && (
        <div className="html-viewer-source">
          <SyntaxHighlighter
            language="markup"
            style={isLight ? oneLight : oneDark}
            customStyle={{ margin: 0, padding: 0, backgroundColor: 'transparent', fontSize: '0.75rem', lineHeight: '1.6' }}
            codeTagProps={{ style: { backgroundColor: 'transparent' } }}
            wrapLongLines
          >
            {content}
          </SyntaxHighlighter>
        </div>
      )}
      {/* The owner's frames stay up under Source, so a draft and the place
          read are where they were on the way back. */}
      {owner ? (
        <div className="html-viewer-stage" data-shown={!source}>
          <ArtifactFrames
            revision={revision}
            fileName={fileName}
            workspaceId={workspaceId}
            filePath={filePath}
            view={artifact}
            shown={!source}
            anchor={anchor}
            anchorSeq={anchorSeq}
          />
        </div>
      ) : !source && (
        <div className="html-viewer-stage">
          <ViewFrame fileName={fileName} workspaceId={workspaceId} filePath={filePath} servePrefix={servePrefix} anchor={anchor} anchorSeq={anchorSeq} />
        </div>
      )}
      {owner && <ComposerDockSlot workspaceId={workspaceId} path={filePath} tabId={artifact.tabId} ui={artifact.ui} active={artifact.active} />}
    </div>
  );
}

/** The file as served, read only: a share's view, and a panel that takes no comments. */
function ViewFrame({ fileName, workspaceId, filePath, servePrefix, anchor, anchorSeq }: {
  fileName: string;
  workspaceId: string;
  filePath: string;
  servePrefix?: string;
  anchor: string | null;
  anchorSeq: number | null;
}): React.ReactElement {
  const live = useLiveFrame({ workspaceId, filePath, prefix: servePrefix, inject: 'view', anchor });
  const [bridge, setBridge] = useState<PageBridge | null>(null);
  useFragmentScroll(bridge, anchor, anchorSeq);
  if (live.failed) return <ServeFailed onRetry={live.retry} />;
  return <BridgedFrame source={live.source} title={fileName || 'HTML Preview'} groundKey={filePath} onBridge={setBridge} />;
}
