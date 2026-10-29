import React, { useState, useEffect } from 'react';
import { useTranslation } from 'react-i18next';
import { authSessionCheck, registerAuthReset } from '@/lib/authResets';
import { useWorkspaceId, useWorkspaceDownloadFile, useWorkspaceFolders } from '../contexts/WorkspaceContext';
import { downloadWorkspaceFile } from '../utils/api';
import { onOtherComputer, parseAgentHref } from '../utils/agentPaths';
import { normalizeFilePath, parseFolderHref, parseWsPath } from '../utils/filePaths';
import ImageLightbox from './ImageLightbox';

// Module-level cache: key:path → blobUrl
const blobCache = new Map<string, string>();
// Its bytes are the account's files, so a sign-out or account switch revokes
// and forgets them.
registerAuthReset(() => {
  blobCache.forEach((url) => URL.revokeObjectURL(url));
  blobCache.clear();
});

function isExternalUrl(src: string): boolean {
  return /^(https?:\/\/|data:|blob:)/i.test(src);
}

type LoadState = 'idle' | 'loading' | 'loaded' | 'error';

interface WorkspaceImageProps extends React.ImgHTMLAttributes<HTMLImageElement> {
  src?: string;
  alt?: string;
}

function WorkspaceImage({ src, alt, ...props }: WorkspaceImageProps) {
  const { t } = useTranslation();
  const [lightboxOpen, setLightboxOpen] = useState(false);
  const contextWorkspaceId = useWorkspaceId();
  const downloadFileFn = useWorkspaceDownloadFile();
  const folders = useWorkspaceFolders();

  // Support __wsref__/{workspaceId}/path for cross-workspace file references,
  // and another workspace's folder (`../NVDA/chart.png`), which names its
  // workspace the same way.
  const wsRef = src ? parseWsPath(src) : null;
  const other = parseFolderHref(src, folders);
  // The rest of another computer is on no machine this view reads, so the
  // image is named, as its link would be, rather than fetched here, where a
  // namesake could answer.
  const elsewhere = !!src && !wsRef && !other && !isExternalUrl(src)
    && onOtherComputer(parseAgentHref(src, folders?.rootDir), folders);
  const workspaceId = wsRef?.workspaceId || other?.workspaceId || contextWorkspaceId;
  // For __wsref__ paths the context downloadFileFn is normally skipped (it's
  // bound to the active workspace, not the referenced one). But the public
  // shared view has no authed per-workspace route — its downloadFileFn IS the
  // share-token-scoped blob fetcher and the only way to read bytes. So only
  // bypass it when a real workspace context exists to fall back on; otherwise a
  // logged-out share viewer would hit the authed /workspaces/{id}/files/download
  // endpoint and get a 401.
  const effectiveDownloadFn = (wsRef || other) && contextWorkspaceId ? null : downloadFileFn;

  const canFetch = !!(src && !elsewhere && !isExternalUrl(src) && (workspaceId || effectiveDownloadFn));
  // The one reading of the destination, the same one a file link gets, so
  // `/home/workspace/charts/x.png` fetches `charts/x.png`, and `../<own
  // folder>/x.png` fetches `x.png`, rather than a path no workspace holds. The
  // decode is part of it and happens exactly once, so a name an LLM emitted
  // encoded (`.../%E5%9B%BE%E8%A1%A8.png`) reaches the API as raw Unicode;
  // callers hand over the raw destination for that reason.
  const normalizedPath = canFetch ? other?.path ?? normalizeFilePath(src!, folders) : '';
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

    const current = authSessionCheck();
    (fetcher as Promise<string>)
      .then((url) => {
        // Nothing will show it, or a sign-out landed first and the file is the
        // last account's.
        if (cancelled || !current()) {
          URL.revokeObjectURL(url);
          if (!cancelled) setState('error');
          return;
        }
        // Two images of one file fetch at once, and only the cached copy is
        // revoked at sign-out, so the second to land shows the first's.
        const landed = blobCache.get(cacheKey);
        if (landed) URL.revokeObjectURL(url);
        else blobCache.set(cacheKey, url);
        setBlobUrl(landed ?? url);
        setState('loaded');
      })
      .catch(() => {
        if (cancelled) return;
        setState('error');
      });

    return () => { cancelled = true; };
  }, [canFetch, cacheKey, workspaceId, normalizedPath, effectiveDownloadFn]);

  if (elsewhere) return <span>{alt || src!.split('/').pop()}</span>;

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
    return (
      <span className="text-xs my-2 inline-block" style={{ color: 'var(--color-text-tertiary)' }}>
        {t('chat.imageLoadFailed', { name: normalizedPath.split('/').pop() })}
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
