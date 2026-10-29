import { Check, Copy, ExternalLink, Link2, Lock } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogTitle,
} from '@/components/ui/dialog';
import { Loader } from '@/components/ui/loader';
import { ToggleSwitch } from '@/components/ui/switch';
import { toast } from '@/components/ui/use-toast';
import { cn } from '@/lib/utils';
import { formatBytes } from '@/lib/format';
import { useCopyShareLink } from '@/hooks/useCopyShareLink';
import { useLocale } from '@/hooks/useLocale';
import { useShareLink, useShareLinkFiles, useShareLinkMutations } from '@/hooks/useShareLink';
import type { ShareFileEntry } from '@/types/api';
import { basename, dirname } from '../utils/fileRefResolver';
import { shareCapIn, shareConflictIn, shareLinkHref, type ShareLinkPatch } from '../utils/api/shareLinks';

interface ShareLinkDialogProps {
  open: boolean;
  workspaceId: string;
  /** The file the dialog is about; any spelling the panel holds. */
  filePath: string;
  onClose: () => void;
}

function FileRow({ entry, notShared, t }: {
  entry: ShareFileEntry;
  /** Listed now but not in what visitors were given: the drift an update would add. */
  notShared: boolean;
  t: (key: string) => string;
}) {
  const locale = useLocale();
  const dir = dirname(entry.path);
  return (
    <li className="flex items-center gap-3 px-3 py-1.5 text-xs" data-selectable>
      <span className="flex-1 min-w-0 truncate" title={entry.path}>
        {dir && <span style={{ color: 'var(--color-text-tertiary)' }}>{dir}/</span>}
        <span style={{ color: 'var(--color-text-primary)' }}>{basename(entry.path)}</span>
      </span>
      <span className="shrink-0 tabular-nums" style={{ color: 'var(--color-text-tertiary)' }}>
        {formatBytes(entry.size, locale)}
      </span>
      <span
        className="shrink-0 w-20 text-right"
        style={{ color: notShared ? 'var(--color-warning)' : 'var(--color-text-quaternary)' }}
      >
        {notShared ? t('shareLink.notSharedYet') : t(`shareLink.reason.${entry.reason}`)}
      </span>
    </li>
  );
}

/**
 * One file's stable link, with the switch that makes it public.
 *
 * The list is the review step: sharing sends exactly the paths shown, and the
 * server refuses a list that no longer matches (409), so what the owner
 * confirmed is what visitors get. The chat itself is never part of it.
 */
function ShareLinkDialog({ open, workspaceId, filePath, onClose }: ShareLinkDialogProps) {
  const { t } = useTranslation();
  const locale = useLocale();
  const linkQuery = useShareLink(workspaceId, { kind: 'file', path: filePath }, { enabled: open });
  const link = linkQuery.data;
  const filesQuery = useShareLinkFiles(workspaceId, link?.code, { enabled: open });
  const { patch } = useShareLinkMutations(workspaceId);
  const { copy, copiedCode } = useCopyShareLink();

  const busy = patch.isPending;
  // The last attempt found a different list on the server. The hook re-reads
  // it, and the notice stands until the owner tries again with that list.
  const filesChanged = shareConflictIn(patch.error) === 'files_changed';
  const cap = shareCapIn(filesQuery.error);
  const files = filesQuery.data?.files ?? [];
  const drift = link?.shared ? filesQuery.data?.drift : null;
  const hasDrift = !!drift && (drift.added.length > 0 || drift.removed.length > 0);
  const addedPaths = new Set(hasDrift ? drift.added : []);
  // Sharing sends the list on screen, so it waits for one that is current;
  // stopping sends no list and never waits on it.
  const listReady = !!filesQuery.data && !filesQuery.isFetching && !filesQuery.isError;
  const href = link ? shareLinkHref(link.code) : '';
  const copied = !!link && copiedCode === link.code;

  const send = (body: ShareLinkPatch) => {
    if (!link || busy) return;
    patch.mutate({ code: link.code, patch: body }, {
      onError: (err) => {
        const conflict = shareConflictIn(err);
        if (conflict === 'files_changed') return;
        if (conflict === 'link_changed') {
          toast({ description: t('shareLink.linkChanged') });
          return;
        }
        console.error('[ShareLinkDialog] Share failed:', err);
        toast({ description: t('shareLink.shareFailed'), variant: 'destructive' });
      },
    });
  };
  const shareListed = () => send({ shared: true, files: files.map((f) => f.path) });

  // The dialog stays mounted while closed, so the last attempt's error would
  // otherwise greet the next open.
  const close = () => {
    if (busy) return;
    patch.reset();
    onClose();
  };

  const switchDisabled = !link || busy || (!link.shared && !listReady);

  return (
    <Dialog open={open} onOpenChange={(o) => { if (!o) close(); }}>
      <DialogContent
        className="sm:max-w-lg"
        style={{ backgroundColor: 'var(--color-bg-page)', borderColor: 'var(--color-border-muted)' }}
      >
        <DialogTitle className="text-base font-semibold truncate pr-6" style={{ color: 'var(--color-text-primary)' }}>
          {t('shareLink.title', { file: basename(filePath) })}
        </DialogTitle>
        <DialogDescription className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
          {t('shareLink.chatStaysPrivate')}
        </DialogDescription>

        <div className="flex flex-col gap-4 pt-1">
          {/* The link */}
          <div className="flex items-center gap-2">
            <div
              className="flex-1 flex items-center px-3 py-2 rounded-md text-xs min-w-0"
              style={{ backgroundColor: 'var(--color-bg-input)', color: 'var(--color-text-secondary)' }}
            >
              <span className="truncate font-mono" data-selectable>
                {href || (linkQuery.isError ? t('shareLink.linkFailed') : '…')}
              </span>
            </div>
            {linkQuery.isError ? (
              <Button
                variant="outline"
                size="sm"
                className="text-xs"
                onClick={() => void linkQuery.refetch()}
                disabled={linkQuery.isFetching}
              >
                {t('common.retry')}
              </Button>
            ) : (
              <Button
                variant="outline"
                size="sm"
                className="gap-1.5 text-xs"
                onClick={() => { if (link) void copy(link.code); }}
                disabled={!link}
              >
                {copied
                  ? <Check className="h-3.5 w-3.5" style={{ color: 'var(--color-success)' }} />
                  : <Copy className="h-3.5 w-3.5" />}
                {copied ? t('shareLink.copied') : t('shareLink.copy')}
              </Button>
            )}
          </div>

          {/* The switch */}
          <div
            className="flex items-center justify-between gap-3 rounded-lg border px-3 py-2.5"
            style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-bg-card)' }}
          >
            <div className="flex items-center gap-2 min-w-0">
              {link?.shared
                ? <Link2 className="h-4 w-4 shrink-0" style={{ color: 'var(--color-accent-primary)' }} />
                : <Lock className="h-4 w-4 shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />}
              <div className="flex flex-col min-w-0">
                <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                  {t('shareLink.anyoneWithLink')}
                </span>
                <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                  {link?.shared ? t('shareLink.sharedHint') : t('shareLink.privateHint')}
                </span>
              </div>
            </div>
            {busy
              ? <Loader size={16} className="text-(--color-text-tertiary)" />
              : (
                <ToggleSwitch
                  checked={!!link?.shared}
                  onChange={() => (link?.shared ? send({ shared: false }) : shareListed())}
                  disabled={switchDisabled}
                  ariaLabel={t('shareLink.anyoneWithLink')}
                />
              )}
          </div>

          {/* Notices */}
          {filesChanged && (
            <p className="text-xs" role="status" style={{ color: 'var(--color-warning)' }}>
              {t('shareLink.filesChanged')}
            </p>
          )}
          {cap !== null && (
            <p className="text-xs" role="status" style={{ color: 'var(--color-warning)' }}>
              {cap.code === 'too_many_bytes'
                ? t('shareLink.tooManyBytes', { size: formatBytes(cap.limit, locale) })
                : t('shareLink.tooManyFiles', { limit: cap.limit })}
            </p>
          )}
          {hasDrift && (
            <div
              className="flex items-start justify-between gap-3 rounded-lg border px-3 py-2.5 text-xs"
              style={{ borderColor: 'var(--color-border-muted)', backgroundColor: 'var(--color-warning-soft)', color: 'var(--color-text-primary)' }}
            >
              <span>{addedPaths.size > 0 ? t('shareLink.drift') : t('shareLink.driftRemoved')}</span>
              <Button
                size="sm"
                className="h-7 shrink-0 px-2.5 text-xs"
                onClick={shareListed}
                disabled={busy || !listReady}
              >
                {t('shareLink.updateSharing')}
              </Button>
            </div>
          )}

          {/* The list; a link that failed to load has none to show. */}
          {cap === null && !linkQuery.isError && (
            <div className="flex flex-col gap-1.5">
              <span className="text-xs font-medium" style={{ color: 'var(--color-text-secondary)' }}>
                {!filesQuery.data
                  ? t('shareLink.listingFiles')
                  : hasDrift
                    ? t('shareLink.visitorsCanOpenAfterUpdate', { count: files.length })
                    : t('shareLink.linkCovers', { count: files.length })}
              </span>
              <div
                className={cn('rounded-lg border overflow-hidden', filesQuery.isLoading && 'py-4')}
                style={{ borderColor: 'var(--color-border-muted)' }}
              >
                {filesQuery.isLoading || !link ? (
                  <div className="flex justify-center">
                    <Loader size={16} className="text-(--color-text-tertiary)" />
                  </div>
                ) : filesQuery.isError ? (
                  <p className="px-3 py-2 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                    {t('shareLink.listFailed')}
                  </p>
                ) : (
                  <ul className="max-h-56 overflow-y-auto divide-y" style={{ borderColor: 'var(--color-border-subtle)' }}>
                    {files.map((entry) => (
                      <FileRow key={entry.path} entry={entry} notShared={addedPaths.has(entry.path)} t={t} />
                    ))}
                  </ul>
                )}
              </div>
            </div>
          )}

          {/* Footer */}
          <div className="flex items-center justify-between gap-3 pt-1">
            {link?.shared ? (
              <a
                href={`${href}?as=visitor`}
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-1.5 text-xs hover:underline"
                style={{ color: 'var(--color-text-secondary)' }}
              >
                <ExternalLink className="h-3.5 w-3.5" />
                {t('shareLink.viewAsVisitor')}
              </a>
            ) : <span />}
            <Button variant="outline" size="sm" onClick={close} disabled={busy}>
              {t('common.done')}
            </Button>
          </div>
        </div>
      </DialogContent>
    </Dialog>
  );
}

export default ShareLinkDialog;
