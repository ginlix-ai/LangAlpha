import { useRef, useState } from 'react';
import { useTranslation } from 'react-i18next';
import { AlertCircle, Check, RotateCcw, X } from 'lucide-react';
import { HeaderButton } from '@/components/mcp/McpPrimitives';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import { useLocale } from '@/hooks/useLocale';
import { canKeepHandles, pickWithHandles } from '@/lib/fileHandles';
import { formatBytes } from '@/lib/format';
import type { DriveUsage } from '@/types/drive';
import { continuable } from '../driveUpload';
import { driveRefusal, refusalLine } from '../driveRefusal';
import { ACTIVE, canRetry, type ResumeResult, type UploadItem } from '../driveUploadStore';
import { ICON_BUTTON } from './PaneFrame';

/**
 * Asks for the file a paused upload was sent from: its handle first (one
 * click, where the browser kept one), else the browser's picker.
 */
function useResumePicker(onResume: (id: number, given?: { file: File; handle?: FileSystemFileHandle | null }) => Promise<ResumeResult>) {
  const input = useRef<HTMLInputElement>(null);
  const [pending, setPending] = useState<number | null>(null);
  const resume = async (id: number) => {
    if ((await onResume(id)) !== 'pick') return;
    if (canKeepHandles()) {
      const picked = await pickWithHandles(false);
      if (picked?.[0]) await onResume(id, picked[0]);
      return;
    }
    setPending(id);
    input.current?.click();
  };
  const picker = (
    <input
      ref={input}
      type="file"
      hidden
      onChange={(e) => {
        const file = e.target.files?.[0];
        e.target.value = '';
        if (file && pending !== null) void onResume(pending, { file });
        setPending(null);
      }}
    />
  );
  return { resume: (id: number) => void resume(id), picker };
}

/**
 * Uploads in flight and how the last ones ended, pinned under the list. A
 * file going up shows its bytes, then the server checking it; a failed one
 * says why in its own row and offers to go again where that could help; one
 * an earlier page left open shows paused, to continue or discard. The tray
 * stays until it is dismissed, so a refusal is never missed.
 */
export function UploadTray({ items, usage, onCancel, onRetry, onResume, onDiscard, onDismiss }: {
  items: UploadItem[];
  /** The space now, for a refusal that came without the room it was judged against. */
  usage: DriveUsage | null;
  onCancel: (id: number) => void;
  onRetry: (id: number) => void;
  onResume: (id: number, given?: { file: File; handle?: FileSystemFileHandle | null }) => Promise<ResumeResult>;
  onDiscard: (id: number) => void;
  onDismiss: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { resume, picker } = useResumePicker(onResume);
  if (items.length === 0) return null;
  const running = items.filter((i) => ACTIVE.has(i.status)).length;
  const failed = items.filter((i) => i.status === 'failed').length;
  const paused = items.filter((i) => i.status === 'paused').length;
  const done = items.filter((i) => i.status === 'done').length;
  const elsewhere = items.filter((i) => i.status === 'elsewhere').length;
  const summary = running
    ? t('drive:upload.sending', { count: running })
    : failed
      ? t('drive:upload.someFailed', { count: failed })
      : paused
        ? t('drive:upload.pausedCount', { count: paused })
        : elsewhere
          ? t('drive:upload.elsewhereCount', { count: elsewhere })
          : done
            ? t('drive:upload.done', { count: done })
            : t('drive:upload.allCanceled', { count: items.length });
  return (
    <section className="drive-uploads" aria-live="polite" aria-label={t('drive:upload.title')}>
      {picker}
      <div className="drive-uploads-head">
        {running ? <Loader size={12} className="text-(--color-accent-primary)" /> : null}
        <span className="drive-uploads-summary">{summary}</span>
        {/* Nothing to clear while only uploads another tab owns are left: they stay until it lets go. */}
        {!running && !paused && elsewhere < items.length && (
          <Button type="button" variant="ghost" size="icon" className={ICON_BUTTON} onClick={onDismiss} aria-label={t('drive:upload.dismiss')}>
            <X className="h-3.5 w-3.5" aria-hidden="true" />
          </Button>
        )}
      </div>
      <ul className="drive-uploads-list">
        {items.map((item) => {
          const loaded = item.status === 'sending' || item.status === 'paused' ? item.loaded : 0;
          // Continuing, the parts already sent are checked against the file first: the bar shows that stretch.
          const checked = item.status === 'sending' ? item.checked : null;
          const pct = checked
            ? Math.round((checked.done / checked.of) * 100)
            : item.total > 0 ? Math.round((loaded / item.total) * 100) : 0;
          const sizes = { loaded: formatBytes(loaded, locale), total: formatBytes(item.total, locale) };
          return (
            <li key={item.id} className="drive-upload" data-status={item.status}>
              <span className="drive-upload-glyph" aria-hidden="true">
                {item.status === 'done' ? (
                  <Check className="h-3.5 w-3.5" />
                ) : item.status === 'failed' ? (
                  <AlertCircle className="h-3.5 w-3.5" />
                ) : null}
              </span>
              <span className="drive-upload-text">
                <span className="drive-upload-name" title={item.path}>{item.name}</span>
                {item.replacesUnkept && ACTIVE.has(item.status) && (
                  <span className="drive-upload-detail">{t('drive:upload.replacesUnkept', { name: item.name })}</span>
                )}
                {item.status === 'sending' && (
                  <span className="drive-upload-detail">
                    {item.busy
                      ? t('drive:retrying')
                      : checked
                        ? t('drive:upload.checkingSent', { done: formatBytes(checked.done, locale), of: formatBytes(checked.of, locale) })
                        : t('drive:upload.progress', sizes)}
                  </span>
                )}
                {item.status === 'verifying' && <span className="drive-upload-detail">{t('drive:upload.verifying')}</span>}
                {item.status === 'elsewhere' && <span className="drive-upload-detail">{t('drive:upload.elsewhere')}</span>}
                {item.status === 'paused' && <span className="drive-upload-detail">{t('drive:upload.pausedSent', sizes)}</span>}
                {item.status === 'paused' && item.wrongFile && (
                  <span className="drive-upload-error">
                    {t('drive:upload.notSameFile', { chosen: item.wrongFile, name: item.name, size: formatBytes(item.total, locale) })}
                  </span>
                )}
                {item.status === 'canceled' && <span className="drive-upload-error">{t('drive:upload.canceled')}</span>}
                {item.status === 'failed' && (
                  <span className="drive-upload-error">
                    {/* The connection, or the server's passing trouble: the upload is still open and a retry goes on from it. */}
                    {continuable(item.failure.error)
                      ? t('drive:upload.interrupted')
                      : refusalLine(driveRefusal(item.failure.error, t, {
                        subject: item.name,
                        failed: t('drive:upload.failed'),
                        locale,
                        usage: item.failure.usage ?? usage,
                        size: item.total,
                      }))}
                  </span>
                )}
              </span>
              {item.status === 'sending' && !item.busy && <span className="drive-upload-pct">{`${pct}%`}</span>}
              {item.status === 'queued' && <span className="drive-upload-pct">{t('drive:upload.waiting')}</span>}
              {(item.status === 'queued' || item.status === 'sending') && (
                <Button type="button" variant="ghost" size="icon" className={ICON_BUTTON} onClick={() => onCancel(item.id)} aria-label={t('drive:upload.cancel', { name: item.name })}>
                  <X className="h-3.5 w-3.5" aria-hidden="true" />
                </Button>
              )}
              {item.status === 'paused' && (
                <span className="drive-upload-actions">
                  <HeaderButton variant="ghost" onClick={() => onDiscard(item.id)} aria-label={t('drive:upload.discardOne', { name: item.name })}>
                    {t('drive:upload.discard')}
                  </HeaderButton>
                  <HeaderButton onClick={() => resume(item.id)} aria-label={t('drive:upload.resumeOne', { name: item.name })}>
                    {t('drive:upload.resume')}
                  </HeaderButton>
                </span>
              )}
              {canRetry(item) && (
                <Button type="button" variant="ghost" size="icon" className={ICON_BUTTON} onClick={() => onRetry(item.id)} aria-label={t('drive:upload.retry', { name: item.name })}>
                  <RotateCcw className="h-3.5 w-3.5" aria-hidden="true" />
                </Button>
              )}
              {(item.status === 'sending' || item.status === 'verifying') && (
                <span className="drive-upload-bar" aria-hidden="true" data-indeterminate={item.status === 'verifying' || undefined}>
                  <span style={{ width: item.status === 'verifying' ? '100%' : `${pct}%` }} />
                </span>
              )}
            </li>
          );
        })}
      </ul>
    </section>
  );
}
