import React from 'react';
import { Check, ExternalLink, LayoutDashboard, Link2, RefreshCw } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Loader } from '@/components/ui/loader';
import { toast } from '@/components/ui/use-toast';
import { useCopyShareLink } from '@/hooks/useCopyShareLink';
import { useShareLink } from '@/hooks/useShareLink';
import { shareLinkHref } from '../../utils/api/shareLinks';
import type { PreviewEntry } from './usePreviews';

interface PreviewCrumbsProps {
  entry: PreviewEntry;
  onRefresh: () => void;
  /** The workspace the app runs in, whose `/a/` link opens it in a tab. */
  workspaceId?: string | null;
  /** A read-only panel opens the app's link but offers no copy of it. */
  readOnly?: boolean;
}

/**
 * The crumb row a running app gets instead of a path: what it is, the port it
 * answers on, and the things that can be done with it. The viewer below
 * carries no chrome of its own — the tab's X is its close, and this row is
 * where everything else lives, so the panel keeps one header shape.
 */
export function PreviewCrumbs({ entry, onRefresh, workspaceId = null, readOnly = false }: PreviewCrumbsProps): React.ReactElement {
  const { t } = useTranslation();
  const name = entry.title || t('filePanel.previewTab');

  // An app's link is private, so minting it on open costs nothing and
  // lets the copy, or the tab it opens, land inside the click.
  const { data: link } = useShareLink(workspaceId, { kind: 'app', port: entry.port });
  const { copy, copiedCode } = useCopyShareLink();
  const copied = !!link && copiedCode === link.code;

  // The check confirms the copy; the toast says who the link opens for.
  const copyLink = async () => {
    if (link && await copy(link.code, entry.path ?? '/')) toast({ description: t('shareLink.copiedPrivate') });
  };

  return (
    <div className="file-panel-crumbs">
      <LayoutDashboard className="h-3.5 w-3.5 shrink-0" style={{ color: 'var(--color-icon-muted)' }} />
      <span className="file-panel-crumb is-file">{name}</span>
      <span className="file-panel-port-chip">:{entry.port}</span>
      <span className="file-panel-crumb-spacer" />
      {/* The path gives way before the buttons do: it shrinks to an
          ellipsis, and they keep their size at any length of it. */}
      {entry.path && (
        <span className="file-panel-crumb-meta file-panel-crumb-path font-mono" title={entry.path}>
          {entry.path}
        </span>
      )}
      {workspaceId && !readOnly && (
        <button
          type="button"
          onClick={() => void copyLink()}
          className="file-panel-icon-btn shrink-0"
          title={t('filePanel.copyPrivateLink')}
          aria-label={t('filePanel.copyPrivateLink')}
          disabled={!link}
        >
          {copied
            ? <Check className="h-3.5 w-3.5" style={{ color: 'var(--color-success)' }} />
            : <Link2 className="h-3.5 w-3.5" />}
        </button>
      )}
      <button
        type="button"
        onClick={onRefresh}
        className="file-panel-icon-btn shrink-0"
        title={t('filePanel.reloadApp')}
        aria-label={t('filePanel.reloadApp')}
        disabled={entry.loading}
      >
        {entry.loading ? <Loader size={14} className="text-current" /> : <RefreshCw className="h-3.5 w-3.5" />}
      </button>
      {/* The link, never the frame's URL: that one is a short-lived key the
          app opens for whoever holds it, and a tab on it dies when it lapses. */}
      <button
        type="button"
        onClick={() => link && window.open(shareLinkHref(link.code, entry.path ?? '/'), '_blank', 'noopener,noreferrer')}
        className="file-panel-icon-btn shrink-0"
        title={t('filePanel.openInBrowser')}
        aria-label={t('filePanel.openInBrowser')}
        disabled={!link}
      >
        <ExternalLink className="h-3.5 w-3.5" />
      </button>
    </div>
  );
}
