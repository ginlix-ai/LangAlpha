import { useState } from 'react';
import { useTranslation } from 'react-i18next';
import { RotateCcw, Trash2 } from 'lucide-react';
import { ConfirmStrip, HeaderButton, ListError, ListSkeleton } from '@/components/mcp/McpPrimitives';
import { useDriveTrash } from '@/hooks/useDrive';
import { useLocale } from '@/hooks/useLocale';
import { useNow } from '@/hooks/useNow';
import { formatBytes, formatTimestamp, relativeTime } from '@/lib/format';
import { EmptyState } from '@/pages/Plugins/components/EmptyState';
import type { DriveTrashItem } from '@/types/drive';
import { useDriveContext } from '../driveContext';
import { parentOf } from '../drivePaths';
import { EntryGlyph } from './EntryGlyph';
import { PaneFrame } from './PaneFrame';

const DAY_MS = 86_400_000;
/** Trash items read at a time; the API answers at most 1000 and pages by limit alone. */
export const TRASH_PAGE = 100;
export const TRASH_MAX = 1000;

/**
 * What an item holds beyond its top entry. A folder counts what went with
 * it; a file with company is a burst of deletes by one actor, filed as one
 * item, whose other entries the API does not name.
 */
function groupNote(item: DriveTrashItem, t: (key: string, opts?: Record<string, unknown>) => string): string | null {
  if (item.entries <= 1) return null;
  return item.entry.kind === 'folder'
    ? t('drive:trash.items', { count: item.entries })
    : t('drive:trash.andMore', { count: item.entries - 1 });
}

function TrashRow({ item, trashDays, onRestored }: {
  item: DriveTrashItem;
  trashDays: number;
  onRestored: (path: string, kind: 'file' | 'folder') => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const now = useNow();
  const { actions } = useDriveContext();
  const [confirming, setConfirming] = useState(false);
  const busy = actions.pending.restore === item.trash_id || actions.pending.purge === item.trash_id;
  const { entry } = item;
  const daysLeft = Math.max(0, Math.ceil((Date.parse(item.deleted_at) + trashDays * DAY_MS - now) / DAY_MS));
  const from = parentOf(entry.path);

  const restore = async () => {
    const top = await actions.restoreFromTrash(item);
    if (top) onRestored(top.path, top.kind);
  };

  const purge = async () => {
    if (!(await actions.purge(item))) setConfirming(false);
  };

  return (
    <li className="drive-trash-item">
      <div className="drive-trash-row">
        <EntryGlyph name={entry.name} folder={entry.kind === 'folder'} className="drive-row-icon" />
        <div className="drive-trash-text">
          <span className="drive-row-name">{entry.name}</span>
          <span className="drive-row-sub">
            {[
              from ? t('drive:trash.from', { folder: from }) : t('drive:trash.fromRoot'),
              groupNote(item, t),
              <span key="when" title={formatTimestamp(item.deleted_at, locale)}>
                {t('drive:trash.deleted', { when: relativeTime(item.deleted_at, locale, now) })}
              </span>,
              daysLeft <= 1 ? t('drive:trash.removedSoon') : t('drive:trash.removedIn', { count: daysLeft }),
            ].filter(Boolean).map((part, i) => (
              <span key={i}>{i > 0 && ' · '}{part}</span>
            ))}
          </span>
        </div>
        <div className="drive-trash-actions">
          <HeaderButton icon={RotateCcw} onClick={() => void restore()} disabled={busy}>
            {t('drive:actions.restore')}
          </HeaderButton>
          <HeaderButton variant="ghost" onClick={() => setConfirming(true)} disabled={busy} aria-label={t('drive:trash.purgeOne', { name: entry.name })}>
            {t('drive:trash.purge')}
          </HeaderButton>
        </div>
      </div>
      {confirming && (
        <ConfirmStrip
          message={t('drive:trash.purgeConfirm', { name: entry.name })}
          confirmLabel={t('drive:trash.purge')}
          cancelLabel={t('common.cancel')}
          pending={busy}
          onConfirm={() => void purge()}
          onCancel={() => setConfirming(false)}
        />
      )}
    </li>
  );
}

/** The trash: what was deleted, when it goes for good, and the way back. */
export function DriveTrash({ trashDays, trashBytes, onRestored }: {
  /** The space's retention, from `/spaces`. */
  trashDays: number;
  /** Bytes only the trash holds, which the allowance does not count; null where the server does not say. */
  trashBytes: number | null;
  onRestored: (path: string, kind: 'file' | 'folder') => void;
}) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { space, actions, go } = useDriveContext();
  const [limit, setLimit] = useState(TRASH_PAGE);
  const { data, isLoading, isFetching, isPlaceholderData, error, refetch } = useDriveTrash(space, limit);
  const items = data?.items;
  const [confirmEmpty, setConfirmEmpty] = useState(false);

  const empty = async () => {
    if (!(await actions.purge())) return;
    setConfirmEmpty(false);
    setLimit(TRASH_PAGE);
  };

  let body: React.ReactNode;
  if (error) body = <ListError onRetry={() => void refetch()}>{t('drive:errors.loadFailed')}</ListError>;
  else if (isLoading) body = <ListSkeleton rows={4} />;
  else if (!items?.length) body = <EmptyState message={t('drive:trash.none', { count: trashDays })} />;
  else {
    body = (
      <>
        <ul className="drive-trash-list">
          {items.map((item) => (
            <TrashRow key={item.trash_id} item={item} trashDays={trashDays} onRestored={onRestored} />
          ))}
        </ul>
        {data?.more && (
          <div className="drive-trash-more">
            {limit < TRASH_MAX ? (
              <HeaderButton
                variant="ghost"
                disabled={isFetching && isPlaceholderData}
                onClick={() => setLimit((n) => Math.min(TRASH_MAX, n + TRASH_PAGE))}
              >
                {t('drive:trash.loadMore')}
              </HeaderButton>
            ) : (
              <span className="drive-row-sub">{t('drive:trash.newestOnly', { count: TRASH_MAX })}</span>
            )}
          </div>
        )}
      </>
    );
  }

  return (
    <PaneFrame
      className="drive-trash"
      title={t('drive:trash.title')}
      back={{ label: t('drive:backToDrive'), onClick: () => go({}) }}
      actions={!!items?.length && (
        <HeaderButton icon={Trash2} onClick={() => setConfirmEmpty(true)}>{t('drive:trash.empty')}</HeaderButton>
      )}
      below={(
        <>
          <p className="drive-subtitle">
            {trashBytes !== null && !!items?.length
              ? t('drive:trash.descriptionHolding', { count: trashDays, size: formatBytes(trashBytes, locale) })
              : t('drive:trash.description', { count: trashDays })}
          </p>
          {confirmEmpty && (
            <ConfirmStrip
              message={data?.more ? t('drive:trash.emptyConfirmAll') : t('drive:trash.emptyConfirm', { count: items?.length ?? 0 })}
              confirmLabel={t('drive:trash.empty')}
              cancelLabel={t('common.cancel')}
              pending={actions.pending.purge === 'all'}
              onConfirm={() => void empty()}
              onCancel={() => setConfirmEmpty(false)}
            />
          )}
        </>
      )}
    >
      <div className="drive-pane-pad">{body}</div>
    </PaneFrame>
  );
}
