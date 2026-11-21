import React, { useState, useCallback, useRef, useEffect } from 'react';
import { RefreshCw, ExternalLink, X, LayoutDashboard, AlertCircle } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useShareLink } from '@/hooks/useShareLink';
import type { BridgeOptions } from '@/lib/appBridge/AppBridge';
import { useAppFrame } from '@/lib/appBridge/useAppFrame';
import { shareLinkHref } from '../../utils/api/shareLinks';
import { HostedFrame } from './HostedFrame';
import { StatusCard } from './StatusCard';
import './PreviewViewer.css';
import type { PreviewData } from '../../hooks/utils/types';
import type { PreviewFailure } from '../../utils/api/sandbox';

/** What the card says for each failure; the hint takes the port. */
const FAILURE_COPY: Record<PreviewFailure, { title: string; hint: string }> = {
  offline: { title: 'filePanel.serverOffline', hint: 'filePanel.serverOfflineHint' },
  portInUse: { title: 'filePanel.portInUse', hint: 'filePanel.portInUseHint' },
};

interface PreviewViewerProps extends Pick<PreviewData, 'url' | 'port' | 'title' | 'path' | 'loading' | 'error' | 'workspaceId'> {
  /** Omitted inside the file panel, where the tab's own X is the close. */
  onClose?: () => void;
  onRefresh?: () => void;
  /** False drops the toolbar: the surface around the viewer already names the
   *  app and owns its actions, and two headers would stack. */
  chrome?: boolean;
  /** When true, a frosted overlay covers the iframe for smooth resizing. */
  isDragging?: boolean;
  /** Monotonic counter — when it changes, force iframe reload even if URL is the same. */
  reloadToken?: number;
  /** Given, the frame may read the owner's data through the kit, under its `/a/` code's grants. */
  bridge?: Pick<BridgeOptions, 'grantKey' | 'active'>;
}

export default function PreviewViewer({ url, port, title, path, workspaceId, loading: externalLoading, error: failure, onClose, onRefresh, isDragging, reloadToken, chrome = true, bridge }: PreviewViewerProps) {
  const { t } = useTranslation();
  // The toolbar opens the app's private /a/ link, minted with the toolbar so
  // the tab opens inside the click.
  const { data: link } = useShareLink(workspaceId, port ? { kind: 'app', port } : null, { enabled: chrome });
  const [loading, setLoading] = useState(true);
  const [iframeKey, setIframeKey] = useState(0);
  const app = useAppFrame(url, bridge ?? null);
  // Keep overlay visible briefly after drag ends so iframe repaints at new size
  const [overlayLinger, setOverlayLinger] = useState(false);

  useEffect(() => {
    if (isDragging) {
      setOverlayLinger(true);
    } else if (overlayLinger) {
      const t = setTimeout(() => setOverlayLinger(false), 80);
      return () => clearTimeout(t);
    }
  }, [isDragging, overlayLinger]);

  // Show overlay immediately when isDragging is true (first render), plus 80ms cooldown
  const showDragOverlay = isDragging || overlayLinger;

  // Reload iframe when url or reloadToken changes (token forces reload even with same URL)
  const prevUrlRef = useRef(url);
  const prevTokenRef = useRef(reloadToken);
  useEffect(() => {
    const urlChanged = url && prevUrlRef.current && url !== prevUrlRef.current;
    const tokenChanged = reloadToken !== undefined && reloadToken !== prevTokenRef.current;
    if (urlChanged || (tokenChanged && url)) {
      setIframeKey(k => k + 1);
      setLoading(true);
    }
    prevUrlRef.current = url;
    prevTokenRef.current = reloadToken;
  }, [url, reloadToken]);

  const handleIframeLoad = useCallback(() => {
    setLoading(false);
  }, []);

  const handleRefresh = useCallback(() => {
    setLoading(true);
    if (onRefresh) {
      onRefresh();
    } else {
      setIframeKey((k) => k + 1);
    }
  }, [onRefresh]);

  // Never the frame's URL: that one is a short-lived key the app opens for
  // whoever holds it, and a tab on it dies when it lapses.
  const handleOpenExternal = useCallback(() => {
    if (link) window.open(shareLinkHref(link.code, path ?? '/'), '_blank', 'noopener,noreferrer');
  }, [link, path]);

  const displayTitle = title || t('filePanel.previewTab');

  return (
    <div className={`preview-viewer${chrome ? '' : ' is-bare'}`} style={{ position: 'relative' }}>
      {chrome && (
        <div className="preview-viewer-toolbar rule-b">
          <div className="preview-viewer-title">
            <span>{displayTitle}</span>
            <span className="preview-viewer-port-badge">:{port}</span>
          </div>
          <div className="preview-viewer-actions">
            <button className="preview-viewer-btn" onClick={handleRefresh} title={t('filePanel.reloadApp')} aria-label={t('filePanel.reloadApp')}>
              <RefreshCw size={18} />
            </button>
            <button className="preview-viewer-btn" onClick={handleOpenExternal} disabled={!link} title={t('filePanel.openInBrowser')} aria-label={t('filePanel.openInBrowser')}>
              <ExternalLink size={18} />
            </button>
            {onClose && (
              <button className="preview-viewer-btn" onClick={onClose} title={t('filePanel.closePreview')} aria-label={t('filePanel.closePreview')}>
                <X size={18} />
              </button>
            )}
          </div>
        </div>
      )}
      {failure ? (
        /* Nothing answered on the port. Checked first: a failed resolve
           leaves no URL, so the loading test below would answer for it. */
        <StatusCard
          icon={AlertCircle}
          title={t(FAILURE_COPY[failure].title)}
          detail={`${displayTitle} :${port}`}
          body={t(FAILURE_COPY[failure].hint, { port })}
        />
      ) : externalLoading || !url ? (
        /* Server is starting, or its URL is still being minted. */
        <StatusCard icon="working" title={t('filePanel.startingServer')} detail={`${displayTitle} :${port}`} />
      ) : (
        /* Normal iframe view */
        <>
          {app.bridge}
          {loading && !showDragOverlay && (
            <div className="preview-viewer-loading">
              <Loader size={24} className="text-(--color-text-tertiary)" />
            </div>
          )}
          {showDragOverlay && (
            <div className="preview-viewer-resize-overlay">
              <div className="preview-viewer-resize-card">
                <LayoutDashboard size={28} style={{ color: 'var(--color-accent-primary)' }} />
                <div className="preview-viewer-resize-info">
                  <span className="preview-viewer-resize-title">{displayTitle}</span>
                  <span className="preview-viewer-resize-url">:{port}</span>
                </div>
              </div>
            </div>
          )}
          <HostedFrame
            {...app.frame}
            key={iframeKey}
            className="preview-viewer-frame"
            title={t('filePanel.previewFrameTitle', { port })}
            onLoad={handleIframeLoad}
            style={isDragging ? { pointerEvents: 'none' } : undefined}
          />
        </>
      )}
    </div>
  );
}
