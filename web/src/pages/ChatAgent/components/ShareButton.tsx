import { useState, useEffect, useRef, useCallback } from 'react';
import { Share2, Copy, Check, Link2, Globe, Lock } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { getThreadShareStatus, updateThreadSharing } from '../utils/api';
import type { ThreadShareStatus, ThreadSharePermissions } from '../../../types/api';

interface ShareButtonProps {
  threadId: string;
  initialIsShared?: boolean;
}

/**
 * ShareButton -- Toggle public sharing for a thread, with permission controls.
 * Shows a popover with share toggle, URL copy, and permission checkboxes.
 */
export default function ShareButton({ threadId, initialIsShared = false }: ShareButtonProps) {
  const { t } = useTranslation();
  const [open, setOpen] = useState(false);
  const [loading, setLoading] = useState(false);
  const [shareState, setShareState] = useState<ThreadShareStatus | null>(null);
  const [copied, setCopied] = useState(false);
  const [updating, setUpdating] = useState(false);
  const popoverRef = useRef<HTMLDivElement>(null);

  // Fetch full share status when popover opens (ensures fresh data including URL + permissions)
  useEffect(() => {
    if (!open || !threadId) return;
    let cancelled = false;
    setLoading(true);
    (async () => {
      try {
        const status = await getThreadShareStatus(threadId);
        if (!cancelled) setShareState(status);
      } catch (e) {
        console.error('Failed to fetch share status:', e);
      }
      if (!cancelled) setLoading(false);
    })();
    return () => { cancelled = true; };
  }, [open, threadId]);

  // Close popover on outside click
  useEffect(() => {
    if (!open) return;
    const handler = (e: MouseEvent) => {
      if (popoverRef.current && !popoverRef.current.contains(e.target as Node)) {
        setOpen(false);
      }
    };
    document.addEventListener('mousedown', handler);
    return () => document.removeEventListener('mousedown', handler);
  }, [open]);

  const handleToggleShare = useCallback(async (enabled: boolean) => {
    setUpdating(true);
    try {
      const result = await updateThreadSharing(threadId, {
        is_shared: enabled,
        permissions: shareState?.permissions || { allow_files: false, allow_download: false },
      });
      setShareState(result);
    } catch (e) {
      console.error('Failed to update sharing:', e);
    }
    setUpdating(false);
  }, [threadId, shareState]);

  const handlePermissionChange = useCallback(async (key: keyof ThreadSharePermissions, value: boolean) => {
    const currentPerms = shareState?.permissions || {};
    const newPerms = { ...currentPerms, [key]: value };

    // If disabling files, also disable download
    if (key === 'allow_files' && !value) {
      newPerms.allow_download = false;
    }

    setUpdating(true);
    try {
      const result = await updateThreadSharing(threadId, {
        is_shared: true,
        permissions: newPerms,
      });
      setShareState(result);
    } catch (e) {
      console.error('Failed to update permissions:', e);
    }
    setUpdating(false);
  }, [threadId, shareState]);

  const handleCopy = useCallback(() => {
    if (!shareState?.share_url) return;
    const fullUrl = `${window.location.origin}${shareState.share_url}`;
    navigator.clipboard.writeText(fullUrl).then(() => {
      setCopied(true);
      setTimeout(() => setCopied(false), 2000);
    });
  }, [shareState]);

  const isShared = shareState ? shareState.is_shared === true : initialIsShared;
  const permissions = shareState?.permissions || {};

  return (
    <div className="relative" ref={popoverRef}>
      <button
        onClick={() => setOpen((p) => !p)}
        className="p-2 rounded-md transition-colors"
        style={{
          color: isShared ? 'var(--color-accent-primary)' : 'var(--color-text-primary)',
          backgroundColor: open ? 'var(--color-border-muted)' : undefined,
        }}
        title={t('share.shareConversation')}
        onMouseEnter={(e) => { if (!open) e.currentTarget.style.backgroundColor = 'var(--color-border-muted)'; }}
        onMouseLeave={(e) => { if (!open) e.currentTarget.style.backgroundColor = ''; }}
      >
        <Share2 className="h-5 w-5" />
      </button>

      {open && (
        <div
          className="absolute right-0 top-full mt-2 w-80 rounded-lg border shadow-lg z-50"
          style={{
            backgroundColor: 'var(--color-bg-elevated, var(--color-bg-card))',
            borderColor: 'var(--color-border-muted)',
          }}
        >
          {loading ? (
            <div className="px-4 py-6 text-center">
              <span className="text-sm" style={{ color: 'var(--color-text-tertiary)' }}>{t('common.loading')}</span>
            </div>
          ) : (
            <div className="p-4 space-y-3">
              {/* Header */}
              <div className="flex items-center justify-between">
                <div className="flex items-center gap-2">
                  {isShared ? (
                    <Globe className="h-4 w-4" style={{ color: 'var(--color-accent-primary)' }} />
                  ) : (
                    <Lock className="h-4 w-4" style={{ color: 'var(--color-text-tertiary)' }} />
                  )}
                  <span className="text-sm font-medium" style={{ color: 'var(--color-text-primary)' }}>
                    {isShared ? t('share.publicLinkEnabled') : t('share.shareConversation')}
                  </span>
                </div>
                {/* Toggle */}
                <button
                  onClick={() => handleToggleShare(!isShared)}
                  disabled={updating}
                  className="relative inline-flex h-5 w-9 flex-shrink-0 cursor-pointer rounded-full border-2 border-transparent transition-colors duration-200"
                  style={{
                    backgroundColor: isShared ? 'var(--color-accent-primary)' : 'var(--color-border-muted)',
                    opacity: updating ? 0.6 : 1,
                  }}
                >
                  <span
                    className="pointer-events-none inline-block h-4 w-4 transform rounded-full bg-white shadow ring-0 transition duration-200"
                    style={{ transform: isShared ? 'translateX(16px)' : 'translateX(0)' }}
                  />
                </button>
              </div>

              {isShared && shareState && (
                <>
                  {/* Share URL */}
                  <div className="flex items-center gap-2">
                    <div
                      className="flex-1 flex items-center gap-2 px-3 py-1.5 rounded-md text-xs truncate"
                      style={{
                        backgroundColor: 'var(--color-bg-tertiary)',
                        color: 'var(--color-text-secondary)',
                      }}
                    >
                      <Link2 className="h-3.5 w-3.5 flex-shrink-0" style={{ color: 'var(--color-text-tertiary)' }} />
                      <span className="truncate">
                        {window.location.origin}{shareState.share_url}
                      </span>
                    </div>
                    <button
                      onClick={handleCopy}
                      className="p-1.5 rounded-md transition-colors flex-shrink-0"
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
            </div>
          )}
        </div>
      )}
    </div>
  );
}
