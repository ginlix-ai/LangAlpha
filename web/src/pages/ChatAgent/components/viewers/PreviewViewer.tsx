import React, { useState, useCallback, useRef, useEffect } from 'react';
import { RefreshCw, ExternalLink, X, LayoutDashboard, AlertCircle } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { useShareLink } from '@/hooks/useShareLink';
import { shareLinkHref } from '../../utils/api/shareLinks';
import { APP_PREVIEW_SANDBOX } from './html/sandbox';
import './PreviewViewer.css';
import type { PreviewData } from '../../hooks/utils/types';

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
}

export default function PreviewViewer({ url, port, title, path, workspaceId, loading: externalLoading, error: externalError, onClose, onRefresh, isDragging, reloadToken, chrome = true }: PreviewViewerProps) {
  const { t } = useTranslation();
  // The toolbar opens the app's private /a/ link, minted with the toolbar so
  // the tab opens inside the click.
  const { data: link } = useShareLink(workspaceId, port ? { kind: 'app', port } : null, { enabled: chrome });
  const [loading, setLoading] = useState(true);
  const [iframeKey, setIframeKey] = useState(0);
  const iframeRef = useRef<HTMLIFrameElement>(null);
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
        <div className="preview-viewer-toolbar">
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
      {externalError ? (
        /* Nothing answered on the port. Checked first: a failed resolve
           leaves no URL, so the loading test below would answer for it. */
        <div className="preview-viewer-resize-overlay" style={{ cursor: 'default' }}>
          <div className="preview-viewer-resize-card" style={{ flexDirection: 'column', alignItems: 'center', gap: 16, padding: '28px 36px' }}>
            <AlertCircle size={28} style={{ color: 'var(--color-text-tertiary)' }} />
            <div className="preview-viewer-resize-info" style={{ alignItems: 'center' }}>
              <span className="preview-viewer-resize-title">{t('filePanel.serverOffline')}</span>
              <span className="preview-viewer-resize-url">{displayTitle} :{port}</span>
              <span style={{ fontSize: '0.6875rem', color: 'var(--color-text-tertiary)', marginTop: 4 }}>
                {t('filePanel.serverOfflineHint')}
              </span>
            </div>
          </div>
        </div>
      ) : externalLoading || !url ? (
        /* Server is starting, or its URL is still being minted. */
        <div className="preview-viewer-resize-overlay" style={{ cursor: 'default' }}>
          <div className="preview-viewer-resize-card" style={{ flexDirection: 'column', alignItems: 'center', gap: 16, padding: '28px 36px' }}>
            <Loader size={20} label={t('filePanel.startingServer')} style={{ color: 'var(--color-accent-primary)' }} />
            <div className="preview-viewer-resize-info" style={{ alignItems: 'center' }}>
              <span className="preview-viewer-resize-title">{t('filePanel.startingServer')}</span>
              <span className="preview-viewer-resize-url">{displayTitle} :{port}</span>
            </div>
          </div>
        </div>
      ) : (
        /* Normal iframe view */
        <>
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
          <iframe
            ref={iframeRef}
            key={iframeKey}
            src={url}
            className="preview-viewer-frame"
            title={t('filePanel.previewFrameTitle', { port })}
            sandbox={APP_PREVIEW_SANDBOX}
            onLoad={handleIframeLoad}
            style={isDragging ? { pointerEvents: 'none' } : undefined}
          />
        </>
      )}
    </div>
  );
}
