import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { Link } from 'react-router';
import { MessagesSquare, X } from 'lucide-react';
import { ConfirmStrip, HeaderButton, ListError, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { Button } from '@/components/ui/button';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { formatBytes, formatTimestamp, relativeTime, shortMonthDay } from '@/lib/format';
import { cn } from '@/lib/utils';
import type { DriveVersion } from '@/types/drive';
import { threadHref, whoLabel } from '../who';
import { ICON_BUTTON } from './PaneFrame';

/**
 * A file's versions, newest first: when, who (the agent's thread linked where
 * it worked in one), why, whether it restored an older one, and when an old
 * one goes. The newest is the file as it is; picking any other shows that
 * version in the viewer. Below them, what the earlier versions take and the
 * way to clear them; a file too large to keep earlier copies says so instead.
 */
export function DriveHistory({ name, versions, historyBytes, tooLarge, onClear, clearing, loading, failed, onRetry, viewing, onView, onClose }: {
  name: string;
  versions: DriveVersion[] | undefined;
  /** Bytes only the earlier versions hold: what clearing frees. */
  historyBytes: number;
  /** Saving over the file replaces it: no earlier copy is kept. */
  tooLarge: boolean;
  /** Drops the earlier versions; answers whether they went. */
  onClear?: () => Promise<boolean>;
  clearing: boolean;
  loading: boolean;
  failed: boolean;
  onRetry: () => void;
  /** The version in the viewer; null is the current one. */
  viewing: number | null;
  onView: (seq: number | null) => void;
  onClose: () => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const [confirming, setConfirming] = useState(false);
  const head = versions?.[0]?.seq ?? null;
  const older = Math.max(0, (versions?.length ?? 0) - 1);
  const clear = async () => {
    if (await onClear?.()) setConfirming(false);
  };
  return (
    <aside className="drive-history" aria-label={t('drive:history.title')}>
      <div className="drive-history-head">
        <h2 className="drive-history-title">{t('drive:history.title')}</h2>
        <Button type="button" variant="ghost" size="icon" className={ICON_BUTTON} onClick={onClose} aria-label={t('drive:history.close')}>
          <X className="h-4 w-4" aria-hidden="true" />
        </Button>
      </div>
      <div className="drive-history-scroll">
        {failed ? (
          <div className="p-3"><ListError onRetry={onRetry}>{t('drive:errors.loadFailed')}</ListError></div>
        ) : loading ? (
          <div className="p-3" aria-busy="true">
            <ListSkeleton rows={4} />
          </div>
        ) : tooLarge && older === 0 ? (
          <p className="drive-history-note">{t('drive:history.tooLarge', { name })}</p>
        ) : (
          <ol className="drive-versions">
            {(versions ?? []).map((v) => {
              const current = v.seq === head;
              const selected = current ? viewing === null || viewing === v.seq : viewing === v.seq;
              const who = whoLabel(t, v.actor);
              return (
                <li key={v.seq}>
                  <button
                    type="button"
                    className={cn('drive-version', selected && 'is-selected')}
                    onClick={() => onView(current ? null : v.seq)}
                    aria-current={selected ? 'true' : undefined}
                  >
                    <span className="drive-version-top">
                      <span className="drive-version-seq">{t('drive:file.version', { seq: v.seq })}</span>
                      {current && <span className="drive-tag">{t('drive:history.current')}</span>}
                      <span className="drive-version-when" title={formatTimestamp(v.created_at, locale)}>
                        {relativeTime(v.created_at, locale, now)}
                      </span>
                    </span>
                    <span className="drive-version-who">
                      {who}
                      <span className="drive-version-size"> · {formatBytes(v.size, locale)}</span>
                    </span>
                    {v.restored_from != null && (
                      <span className="drive-version-restored">{t('drive:history.restoredFrom', { seq: v.restored_from })}</span>
                    )}
                    {v.note && <span className="drive-version-note">{v.note}</span>}
                    {!current && (
                      <span className="drive-version-expires" title={v.expires_at ? formatTimestamp(v.expires_at, locale) : undefined}>
                        {v.expires_at ? t('drive:history.goes', { date: shortMonthDay(new Date(v.expires_at), locale) }) : t('drive:history.kept')}
                      </span>
                    )}
                  </button>
                  {v.actor.thread_id && (
                    <Link to={threadHref(v.actor.thread_id)} className="drive-version-thread">
                      <MessagesSquare className="h-3 w-3" aria-hidden="true" />
                      {t('drive:history.openThread')}
                    </Link>
                  )}
                </li>
              );
            })}
          </ol>
        )}
      </div>
      {!failed && !loading && older > 0 && (
        <div className="drive-history-foot">
          {tooLarge && <p className="drive-history-note">{t('drive:history.tooLarge', { name })}</p>}
          {confirming ? (
            <ConfirmStrip
              message={t('drive:history.clearConfirm', { name, count: older, size: formatBytes(historyBytes, locale) })}
              confirmLabel={t('drive:history.clear')}
              cancelLabel={t('common.cancel')}
              pending={clearing}
              onConfirm={() => void clear()}
              onCancel={() => setConfirming(false)}
            />
          ) : (
            <div className="drive-history-sum">
              <span>{t('drive:history.takes', { size: formatBytes(historyBytes, locale) })}</span>
              {onClear && (
                <HeaderButton variant="ghost" onClick={() => setConfirming(true)} disabled={clearing}>
                  {t('drive:history.clear')}
                </HeaderButton>
              )}
            </div>
          )}
        </div>
      )}
    </aside>
  );
}
