import React, { useState, useCallback, useRef, useEffect } from 'react';
import { RefreshCw, ExternalLink, X, Loader2, Globe, AlertCircle } from 'lucide-react';
import './PreviewViewer.css';
import type { PreviewData } from '../../hooks/utils/types';

interface PreviewViewerProps extends Pick<PreviewData, 'url' | 'port' | 'title' | 'loading' | 'error'> {
  onClose: () => void;
  onRefresh?: () => void;
  /** When true, a frosted overlay covers the iframe for smooth resizing. */
  isDragging?: boolean;
}

export default function PreviewViewer({ url, port, title, loading: externalLoading, error: externalError, onClose, onRefresh, isDragging }: PreviewViewerProps) {
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

  // Reload iframe when url changes (e.g. after async refresh resolves)
  const prevUrlRef = useRef(url);
  useEffect(() => {
    if (url && prevUrlRef.current && url !== prevUrlRef.current) {
      setIframeKey(k => k + 1);
      setLoading(true);
    }
    prevUrlRef.current = url;
  }, [url]);

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

  const handleOpenExternal = useCallback(() => {
    window.open(url, '_blank', 'noopener,noreferrer');
  }, [url]);

  const displayTitle = title || 'Preview';
  const hostname = (() => {
    try { return new URL(url).hostname; } catch { return ''; }
  })();

  return (
    <div className="preview-viewer" style={{ position: 'relative' }}>
      <div className="preview-viewer-toolbar">
        <div className="preview-viewer-title">
          <span>{displayTitle}</span>
          <span className="preview-viewer-port-badge">:{port}</span>
        </div>
        <div className="preview-viewer-actions">
          <button className="preview-viewer-btn" onClick={handleRefresh} title="Refresh preview">
            <RefreshCw size={18} />
          </button>
          <button className="preview-viewer-btn" onClick={handleOpenExternal} title="Open in new tab">
            <ExternalLink size={18} />
          </button>
          <button className="preview-viewer-btn" onClick={onClose} title="Close preview">
            <X size={18} />
          </button>
        </div>
      </div>
      {externalLoading || !url ? (
        /* Server is starting — show frosted glass overlay with spinner */
        <div className="preview-viewer-resize-overlay" style={{ cursor: 'default' }}>
          <div className="preview-viewer-resize-card" style={{ flexDirection: 'column', alignItems: 'center', gap: 16, padding: '28px 36px' }}>
            <div className="preview-viewer-spinner" />
            <div className="preview-viewer-resize-info" style={{ alignItems: 'center' }}>
              <span className="preview-viewer-resize-title">Starting server...</span>
              <span className="preview-viewer-resize-url">{displayTitle} :{port}</span>
            </div>
          </div>
        </div>
      ) : externalError ? (
        /* Error state */
        <div className="preview-viewer-resize-overlay" style={{ cursor: 'default' }}>
          <div className="preview-viewer-resize-card" style={{ flexDirection: 'column', alignItems: 'center', gap: 16, padding: '28px 36px' }}>
            <AlertCircle size={28} style={{ color: 'var(--color-text-tertiary)' }} />
            <div className="preview-viewer-resize-info" style={{ alignItems: 'center' }}>
              <span className="preview-viewer-resize-title">Server offline</span>
              <span className="preview-viewer-resize-url">{displayTitle} :{port}</span>
              <span style={{ fontSize: 11, color: 'var(--color-text-tertiary)', marginTop: 4 }}>
                Click Refresh to restart
              </span>
            </div>
          </div>
        </div>
      ) : (
        /* Normal iframe view */
        <>
          {loading && !showDragOverlay && (
            <div className="preview-viewer-loading">
              <Loader2 size={24} className="animate-spin" style={{ color: 'var(--color-text-tertiary)' }} />
            </div>
          )}
          {showDragOverlay && (
            <div className="preview-viewer-resize-overlay">
              <div className="preview-viewer-resize-card">
                <Globe size={28} style={{ color: 'var(--color-accent-primary)' }} />
                <div className="preview-viewer-resize-info">
                  <span className="preview-viewer-resize-title">{displayTitle}</span>
                  {hostname && <span className="preview-viewer-resize-url">{hostname}:{port}</span>}
                </div>
              </div>
            </div>
          )}
          <iframe
            ref={iframeRef}
            key={iframeKey}
            src={url}
            className="preview-viewer-frame"
            title={`Preview - port ${port}`}
            sandbox="allow-scripts allow-same-origin allow-forms allow-popups allow-modals"
            onLoad={handleIframeLoad}
            style={isDragging ? { pointerEvents: 'none' } : undefined}
          />
        </>
      )}
    </div>
  );
}
