import React, { useState, useEffect, useSyncExternalStore } from 'react';
import { useTranslation } from 'react-i18next';
import { authSessionCheck, registerAuthReset } from '@/lib/authResets';
import { createEmitter } from '@/lib/emitter';
import { useWorkspaceId, useWorkspaceDownloadFile, useWorkspaceFolders, useWorkspaceImageScope } from '../contexts/WorkspaceContext';
import { downloadWorkspaceFile } from '../utils/api';
import { onOtherComputer, parseAgentHref } from '../utils/agentPaths';
import { normalizeFilePath, parseFolderHref, parseWsPath } from '../utils/filePaths';
import ImageLightbox from './ImageLightbox';
import { useCapturedImageUrl } from '../contexts/CapturedImagesContext';

// key:path → blob URL of the copy last read. A path is not a version: the agent
// rewrites a chart in place and shows it again, so the cache only paints at
// once; every image that mounts reads the file again, and the copy that lands
// replaces this one for every image of the path still on screen.
// One emitter for the whole cache: each image reads its own key back, so only
// the images of the key that changed redraw.
const blobCache = new Map<string, string>();
const inFlight = new Map<string, Promise<void>>();
const changes = createEmitter();

function publish(key: string, url: string): void {
  const previous = blobCache.get(key);
  if (previous === url) return;
  blobCache.set(key, url);
  changes.emit();
  if (previous) URL.revokeObjectURL(previous);
}

/** One read of the file per key at a time, however many images of it mount together. */
function readInto(key: string, fetcher: () => Promise<string>): Promise<void> {
  const running = inFlight.get(key);
  if (running) return running;
  // A sign-out landing first makes the bytes the last account's.
  const current = authSessionCheck();
  const read = fetcher()
    .then((url) => {
      if (!current()) {
        URL.revokeObjectURL(url);
        throw new Error('signed out');
      }
      publish(key, url);
    })
    .finally(() => {
      if (inFlight.get(key) === read) inFlight.delete(key);
    });
  inFlight.set(key, read);
  return read;
}

// Its bytes are the account's files, so a sign-out or account switch revokes
// and forgets them.
registerAuthReset(() => {
  blobCache.forEach((url) => URL.revokeObjectURL(url));
  blobCache.clear();
  inFlight.clear();
  changes.emit();
});

function isExternalUrl(src: string): boolean {
  return /^(https?:\/\/|data:|blob:)/i.test(src);
}

type LoadState = 'loading' | 'loaded' | 'error';

interface WorkspaceImageProps extends React.ImgHTMLAttributes<HTMLImageElement> {
  src?: string;
  alt?: string;
}

function ImageLoadFailed({ name }: { name: string | undefined }) {
  const { t } = useTranslation();
  return (
    <span className="text-xs my-2 inline-block" style={{ color: 'var(--color-text-tertiary)' }}>
      {t('chat.imageLoadFailed', { name })}
    </span>
  );
}

function WorkspaceImage({ src: written, alt, ...props }: WorkspaceImageProps) {
  const folders = useWorkspaceFolders();
  // The copy the reply showed, once its run says where it was captured; until
  // then the path, which reads the file as it is now.
  const src = useCapturedImageUrl(written, folders) ?? written;
  const [lightboxOpen, setLightboxOpen] = useState(false);
  // Safari draws a broken image as a small question mark and drops its alt text.
  const [brokenSrc, setBrokenSrc] = useState<string | null>(null);
  const contextWorkspaceId = useWorkspaceId();
  const downloadFileFn = useWorkspaceDownloadFile();
  const imageScope = useWorkspaceImageScope();

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
  const cacheKey = canFetch ? `${imageScope || workspaceId || 'shared'}:${normalizedPath}` : '';

  const readCached = () => (cacheKey ? blobCache.get(cacheKey) ?? null : null);
  const blobUrl = useSyncExternalStore(changes.subscribe, readCached, readCached);
  // What failed: a read that left nothing to show (no url), or a copy the
  // browser could not draw. A new copy of the key, which any later mount reads,
  // clears it, and so does another path.
  const [failed, setFailed] = useState<{ key: string; url: string | null } | null>(null);

  useEffect(() => {
    if (!canFetch) return;
    let cancelled = false;
    readInto(cacheKey, () => {
      const fetcher = effectiveDownloadFn
        ? effectiveDownloadFn(normalizedPath)
        : downloadWorkspaceFile(workspaceId!, normalizedPath);
      return fetcher as Promise<string>;
    })
      // A copy already on screen stays when the new read fails.
      .catch(() => { if (!cancelled && !blobCache.has(cacheKey)) setFailed({ key: cacheKey, url: null }); });
    return () => { cancelled = true; };
  }, [canFetch, cacheKey, workspaceId, normalizedPath, effectiveDownloadFn]);

  const state: LoadState = failed?.key === cacheKey && failed.url === blobUrl
    ? 'error'
    : blobUrl ? 'loaded' : 'loading';

  if (elsewhere) return <span>{alt || src!.split('/').pop()}</span>;

  // Pass through: no context, no src, or external URL
  if (!canFetch) {
    // Don't render an empty src — browsers re-fetch the page
    if (!src) return null;
    if (src === brokenSrc) {
      return <ImageLoadFailed name={alt || src.split('/').pop()} />;
    }
    return (
      <>
        <img
          className="rounded-lg my-2 cursor-pointer"
          style={{ maxWidth: '100%', height: 'auto' }}
          src={src}
          alt={alt}
          onClick={() => src && setLightboxOpen(true)}
          onError={() => setBrokenSrc(src)}
          {...props}
        />
        {src && <ImageLightbox src={src} alt={alt} open={lightboxOpen} onClose={() => setLightboxOpen(false)} />}
      </>
    );
  }

  if (state === 'loading') {
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
    return <ImageLoadFailed name={normalizedPath.split('/').pop()} />;
  }

  return (
    <>
      <img
        className="rounded-lg my-2 cursor-pointer"
        style={{ maxWidth: '100%', height: 'auto' }}
        src={blobUrl!}
        alt={alt}
        onClick={() => setLightboxOpen(true)}
        // The copy stays cached for the other images of the file, each of
        // which fails on its own; the next mount reads the file again.
        onError={() => setFailed({ key: cacheKey, url: blobUrl })}
        {...props}
      />
      <ImageLightbox src={blobUrl!} alt={alt} open={lightboxOpen} onClose={() => setLightboxOpen(false)} />
    </>
  );
}

export default WorkspaceImage;
