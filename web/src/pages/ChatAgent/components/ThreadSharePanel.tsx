import { useState, useCallback, type ElementType } from 'react';
import { Copy, Check, Link2, Lock, FileText } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { copyText } from '@/lib/clipboard';
import { shortMonthDay } from '@/lib/format';
import { useCopyShareLink } from '@/hooks/useCopyShareLink';
import { useLocale } from '@/hooks/useLocale';
import { useSharedLinks, useShareLinkMutations } from '@/hooks/useShareLink';
import { useQueryClient } from '@tanstack/react-query';
import { saveThreadSharing, useThreadShareStatus } from '../hooks/useThreadShareStatus';
import { shareConflictIn } from '../utils/api/shareLinks';
import type { ShareLink, ThreadShareStatus, ThreadSharePermissions } from '../../../types/api';

function sharedLinkName(link: ShareLink): string {
  return link.title || link.path?.split('/').pop() || link.code;
}

/** Every file link the workspace has made public, each with Copy and Stop. */
function SharedInWorkspace({ workspaceId }: { workspaceId: string }) {
  const { t } = useTranslation();
  const locale = useLocale();
  const { data: links } = useSharedLinks(workspaceId);
  const { patch } = useShareLinkMutations(workspaceId);
  const { copy, copiedCode } = useCopyShareLink();

  if (!links?.length) return null;

  const stopSharing = (link: ShareLink) => {
    patch.mutate({ code: link.code, patch: { shared: false } }, {
      onError: (err) => {
        if (shareConflictIn(err) === 'link_changed') {
          toast({ description: t('shareLink.linkChanged') });
          return;
        }
        console.error('[ShareButton] Stop sharing failed:', err);
        toast({ description: t('shareLink.shareFailed'), variant: 'destructive' });
      },
    });
  };

  return (
    <div className="pt-3 mt-3 border-t space-y-2" style={{ borderColor: 'var(--color-border-muted)' }}>
      <div className="flex items-center justify-between">
        <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
          {t('share.sharedInWorkspace')}
        </p>
        <span className="text-xs tabular-nums" style={{ color: 'var(--color-text-quaternary)' }}>{links.length}</span>
      </div>
      <ul className="max-h-48 overflow-y-auto space-y-1">
        {links.map((link) => (
          <li key={link.code} className="flex items-center gap-2 min-w-0">
            <FileText className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
            <div className="flex-1 min-w-0">
              <p className="text-xs truncate" style={{ color: 'var(--color-text-primary)' }} title={link.path ?? undefined}>
                {sharedLinkName(link)}
              </p>
              {link.shared_at && (
                <p className="text-[0.6875rem]" style={{ color: 'var(--color-text-tertiary)' }}>
                  {t('share.sharedOn', { date: shortMonthDay(new Date(link.shared_at), locale) })}
                </p>
              )}
            </div>
            <button
              type="button"
              onClick={() => void copy(link.code)}
              className="p-1 rounded-md shrink-0"
              style={{ color: copiedCode === link.code ? 'var(--color-success)' : 'var(--color-text-secondary)' }}
              title={t('share.copyLink')}
              aria-label={t('share.copyLink')}
            >
              {copiedCode === link.code ? <Check className="h-3.5 w-3.5" /> : <Copy className="h-3.5 w-3.5" />}
            </button>
            <button
              type="button"
              onClick={() => stopSharing(link)}
              disabled={patch.isPending}
              className="text-xs px-1.5 py-0.5 rounded-md shrink-0 disabled:opacity-50 hover:underline"
              style={{ color: 'var(--color-text-secondary)' }}
            >
              {t('share.stop')}
            </button>
          </li>
        ))}
      </ul>
    </div>
  );
}

interface ThreadSharePanelProps {
  threadId: string;
  /** The workspace whose shared files the panel lists; null hides the list. */
  workspaceId?: string | null;
  /** What the heading renders as: a dialog passes its title so the dialog is named by it. */
  Heading?: ElementType;
}

/**
 * A thread's public link: the share toggle, the URL to copy, and what visitors
 * may open. The status is the thread's one cached entry, read again as the
 * panel opens, so a host mounts it only while it is shown (the chat header's
 * popover, the sidebar's share dialog), keyed by the thread.
 */
export function ThreadSharePanel({
  threadId,
  workspaceId = null,
  Heading = 'span',
}: ThreadSharePanelProps) {
  const { t } = useTranslation();
  const [copied, setCopied] = useState(false);
  const [updating, setUpdating] = useState(false);

  const queryClient = useQueryClient();
  const { data: shareState, isPending, isError, refetch } = useThreadShareStatus(threadId, { refetchOnMount: 'always' });

  const save = useCallback(async (body: Pick<ThreadShareStatus, 'is_shared' | 'permissions'>) => {
    setUpdating(true);
    try {
      await saveThreadSharing(queryClient, threadId, body);
    } catch (e) {
      console.error('Failed to update sharing:', e);
    }
    setUpdating(false);
  }, [queryClient, threadId]);

  const handleToggleShare = (enabled: boolean) => save({
    is_shared: enabled,
    permissions: shareState?.permissions || { allow_files: false, allow_download: false },
  });

  const handlePermissionChange = (key: keyof ThreadSharePermissions, value: boolean) => {
    const newPerms = { ...shareState?.permissions, [key]: value };
    // If disabling files, also disable download
    if (key === 'allow_files' && !value) {
      newPerms.allow_download = false;
    }
    return save({ is_shared: true, permissions: newPerms });
  };

  const handleCopy = useCallback(() => {
    if (!shareState?.share_url) return;
    const fullUrl = `${window.location.origin}${shareState.share_url}`;
    copyText(fullUrl).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    }, (e) => {
      console.error('Failed to copy share link:', e);
      toast({ description: t('filePanel.shareLinkFailed'), variant: 'destructive' });
    });
  }, [shareState, t]);

  const isShared = shareState?.is_shared === true;
  const permissions = shareState?.permissions || {};

  if (isPending) {
    return (
      <div className="px-4 py-6 text-center">
        <Heading className="sr-only">{t('share.shareConversation')}</Heading>
        <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.loading')}</span>
      </div>
    );
  }

  // Without the status the toggle would show a guess, and turning it on would
  // send default permissions over the thread's own.
  if (isError && !shareState) {
    return (
      <div className="px-4 py-6 text-center space-y-2">
        <Heading className="sr-only">{t('share.shareConversation')}</Heading>
        <p className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{t('share.statusLoadFailed')}</p>
        <button
          type="button"
          onClick={() => void refetch()}
          className="text-xs hover:underline"
          style={{ color: 'var(--color-text-secondary)' }}
        >
          {t('common.retry')}
        </button>
      </div>
    );
  }

  return (
    <div className="p-4 space-y-3">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div className="flex items-center gap-2">
          {isShared ? (
            <Link2 className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
          ) : (
            <Lock className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
          )}
          <Heading className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
            {isShared ? t('share.publicLinkEnabled') : t('share.shareConversation')}
          </Heading>
        </div>
        {/* Toggle */}
        <button
          type="button"
          role="switch"
          aria-checked={isShared}
          aria-label={t('share.shareConversation')}
          onClick={() => handleToggleShare(!isShared)}
          disabled={updating}
          className="relative inline-flex h-5 w-9 shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors"
          style={{
            backgroundColor: isShared ? 'var(--color-accent-primary)' : 'var(--color-border-muted)',
            opacity: updating ? 0.6 : 1,
          }}
        >
          <span
            className="pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow-sm ring-0 transition"
            style={{ transform: isShared ? 'translateX(16px)' : 'translateX(0)' }}
          />
        </button>
      </div>

      {isShared && shareState && (
        <>
          {/* Share URL */}
          <div className="flex items-center gap-2">
            <div
              className="flex-1 flex items-center px-3 py-1.5 rounded-md text-xs truncate"
              style={{
                backgroundColor: 'var(--color-bg-input)',
                color: 'var(--color-text-secondary)',
              }}
            >
              <span className="truncate">
                {window.location.origin}{shareState.share_url}
              </span>
            </div>
            <button
              onClick={handleCopy}
              className="p-1.5 rounded-md transition-colors shrink-0"
              style={{ color: copied ? 'var(--color-success)' : 'var(--color-text-secondary)' }}
              onMouseEnter={(e) => { e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
              onMouseLeave={(e) => { e.currentTarget.style.backgroundColor = ''; }}
              title={t('share.copyLink')}
            >
              {copied ? <Check className="h-4 w-4" /> : <Copy className="h-4 w-4" />}
            </button>
          </div>

          {/* Permissions */}
          <div className="space-y-2 pt-1">
            <p className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
              {t('share.permissions')}
            </p>
            <label className="flex items-center gap-2 cursor-pointer">
              <input
                type="checkbox"
                checked={permissions.allow_files || false}
                onChange={(e) => handlePermissionChange('allow_files', e.target.checked)}
                disabled={updating}
                className="rounded border-gray-300"
              />
              <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                {t('share.allowFileBrowsing')}
              </span>
            </label>
            {permissions.allow_files && (
              <label className="flex items-center gap-2 cursor-pointer ml-4">
                <input
                  type="checkbox"
                  checked={permissions.allow_download || false}
                  onChange={(e) => handlePermissionChange('allow_download', e.target.checked)}
                  disabled={updating}
                  className="rounded border-gray-300"
                />
                <span className="text-xs" style={{ color: 'var(--color-text-secondary)' }}>
                  {t('share.allowFileDownload')}
                </span>
              </label>
            )}
          </div>

          {/* Info */}
          <p className="text-xs pt-1" style={{ color: 'var(--color-text-tertiary)' }}>
            {t('share.anyoneWithLink')}
          </p>
        </>
      )}
      {workspaceId && <SharedInWorkspace workspaceId={workspaceId} />}
    </div>
  );
}
