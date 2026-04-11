import React, { useState, useEffect } from 'react';
import { useWorkspaceId, useWorkspaceDownloadFile } from '../contexts/WorkspaceContext';
import { downloadWorkspaceFile } from '../utils/api';
import { parseWsPath } from './FileCard';
import ImageLightbox from './ImageLightbox';

// Module-level cache: key:path → blobUrl
const blobCache = new Map<string, string>();

function isExternalUrl(src: string): boolean {
  return /^(https?:\/\/|data:|blob:)/i.test(src);
}

type LoadState = 'idle' | 'loading' | 'loaded' | 'error';

interface WorkspaceImageProps extends React.ImgHTMLAttributes<HTMLImageElement> {
  src?: string;
  alt?: string;
}

function WorkspaceImage({ src, alt, ...props }: WorkspaceImageProps) {
  const [lightboxOpen, setLightboxOpen] = useState(false);
  const contextWorkspaceId = useWorkspaceId();
  const downloadFileFn = useWorkspaceDownloadFile();

  // Support __wsref__/{workspaceId}/path for cross-workspace file references
  const wsRef = src ? parseWsPath(src) : null;
  const workspaceId = wsRef?.workspaceId || contextWorkspaceId;
  // For __wsref__ paths, don't use the context downloadFileFn (it's bound to the wrong workspace)
  const effectiveDownloadFn = wsRef ? null : downloadFileFn;

  const canFetch = !!(src && !isExternalUrl(src) && (workspaceId || effectiveDownloadFn));
  const normalizedPath = canFetch ? (wsRef ? wsRef.path : src!) : '';
  const cacheKey = canFetch ? `${workspaceId || 'shared'}:${normalizedPath}` : '';

  const [state, setState] = useState<LoadState>(() =>
    cacheKey && blobCache.has(cacheKey) ? 'loaded' : 'idle'
  );
  const [blobUrl, setBlobUrl] = useState<string | null>(() =>
    cacheKey ? blobCache.get(cacheKey) || null : null
  );

  useEffect(() => {
    if (!canFetch) return;

    const cached = blobCache.get(cacheKey);
    if (cached) {
      setBlobUrl(cached);
      setState('loaded');
      return;
    }

    let cancelled = false;
    setState('loading');

    const fetcher = effectiveDownloadFn
      ? effectiveDownloadFn(normalizedPath)
      : downloadWorkspaceFile(workspaceId!, normalizedPath);

    (fetcher as Promise<string>)
      .then((url) => {
        if (cancelled) return;
        blobCache.set(cacheKey, url);
        setBlobUrl(url);
        setState('loaded');
      })
      .catch(() => {
        if (cancelled) return;
        setState('error');
      });

    return () => { cancelled = true; };
  }, [canFetch, cacheKey, workspaceId, normalizedPath, effectiveDownloadFn]);

  // Pass through: no context, no src, or external URL
  if (!canFetch) {
    // Don't render an empty src — browsers re-fetch the page
    if (!src) return null;
    return (
      <>
        <img
          className="rounded-lg my-2 cursor-pointer"
          style={{ maxWidth: '100%', height: 'auto' }}
          src={src}
          alt={alt}
          onClick={() => src && setLightboxOpen(true)}
          {...props}
        />
        {src && <ImageLightbox src={src} alt={alt} open={lightboxOpen} onClose={() => setLightboxOpen(false)} />}
      </>
    );
  }

  if (state === 'loading' || state === 'idle') {
    return (
      <span
        className="rounded-lg my-2 animate-pulse"
        style={{
          display: 'block',
          width: '100%',
          maxWidth: 480,
          height: 200,
          backgroundColor: 'var(--color-border-muted)',
        }}
      />
    );
  }

  if (state === 'error') {
    const filename = normalizedPath.split('/').pop();
    return (
      <span className="text-xs my-2 inline-block" style={{ color: 'var(--color-text-tertiary)' }}>
        [image: {filename}]
      </span>
    );
  }

  return (
    <>
      <img
        className="rounded-lg my-2 cursor-pointer"
        style={{ maxWidth: '100%', height: 'auto' }}
        src={blobUrl!}
        alt={alt}
        onClick={() => setLightboxOpen(true)}
        {...props}
      />
      <ImageLightbox src={blobUrl!} alt={alt} open={lightboxOpen} onClose={() => setLightboxOpen(false)} />
    </>
  );
}

export default WorkspaceImage;
