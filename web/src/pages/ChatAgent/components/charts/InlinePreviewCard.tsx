import React, { useState, useEffect, useRef, useCallback } from 'react';
import { useTranslation } from 'react-i18next';
import { ChevronDown, ExternalLink, LayoutDashboard, Link2, Lock, PanelRight } from 'lucide-react';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { Loader } from '@/components/ui/loader';
import { toast } from '@/components/ui/use-toast';
import { useCopyShareLink } from '@/hooks/useCopyShareLink';
import { useShareLink } from '@/hooks/useShareLink';
import { useWorkspaceId } from '../../contexts/WorkspaceContext';
import { checkPreviewHealth } from '../../utils/api';
import { shareLinkHref } from '../../utils/api/shareLinks';
import { finite, text, type InlineCardProps } from './cardReaders';
import '../messageList/TurnFileCards.css';
import './turnFileTerm.css';
import './termCardStyles.css';

// Module-level deduplication: share a single in-flight health check per (workspaceId, port)
const inflightChecks = new Map<string, Promise<{ reachable: boolean; checked_at: number }>>();

function deduplicatedHealthCheck(workspaceId: string, port: number) {
  const key = `${workspaceId}:${port}`;
  const existing = inflightChecks.get(key);
  if (existing) return existing;
  const promise = checkPreviewHealth(workspaceId, port).finally(() => {
    inflightChecks.delete(key);
  });
  inflightChecks.set(key, promise);
  return promise;
}

type Health = 'checking' | 'live' | 'stopped' | 'offline';

function StatusGlyph({ health }: { health: Health }): React.ReactElement {
  if (health === 'checking') {
    return <Loader size={12} style={{ color: 'var(--color-accent-primary)' }} />;
  }
  if (health === 'live') {
    return <span className="h-1.5 w-1.5 shrink-0 rounded-full" style={{ background: 'var(--color-success)' }} />;
  }
  return <span className="h-1.5 w-1.5 shrink-0 rounded-full" style={{ border: '1px solid currentColor' }} />;
}

/**
 * A running app the agent published, drawn as the same card as a turn's files
 * so everything a turn produced reads as one kind of object. Without a
 * workspace in context (a shared chat) the app cannot be reached, so the card
 * says it is private instead of checking forever.
 */
export function InlinePreviewCard({ artifact, onClick }: InlineCardProps): React.ReactElement | null {
  const { t } = useTranslation();
  const workspaceId = useWorkspaceId();
  const [health, setHealth] = useState<Health>('checking');
  const [menuOpen, setMenuOpen] = useState(false);
  const checkingRef = useRef(false);
  const port = finite(artifact.port);
  // The page this card announced, which the link's own entry may have moved past.
  const path = text(artifact.path);

  // The private /a/ link is minted when the menu opens, not per card on mount:
  // a long chat can hold many of these, and only the menu uses the link.
  const { data: link } = useShareLink(
    workspaceId,
    port ? { kind: 'app', port } : null,
    { enabled: menuOpen },
  );
  const { copy } = useCopyShareLink();

  const doHealthCheck = useCallback(async () => {
    if (!workspaceId || !port || checkingRef.current) return;
    checkingRef.current = true;
    try {
      const result = await deduplicatedHealthCheck(workspaceId, port);
      setHealth(result.reachable ? 'live' : 'offline');
    } catch (err: unknown) {
      const status = (err as { response?: { status?: number } })?.response?.status;
      setHealth(status === 503 ? 'stopped' : 'offline');
    } finally {
      checkingRef.current = false;
    }
  }, [workspaceId, port]);

  // Checked on mount and every 2 minutes, but only while the page is visible:
  // a live answer counts as use of the computer, so a tab left open in the
  // background must not keep it from stopping when idle. Coming back checks
  // at once if the last answer is older than one interval.
  useEffect(() => {
    const intervalMs = 2 * 60 * 1000;
    let lastCheck = 0;
    const check = () => {
      if (document.visibilityState !== 'visible') return;
      lastCheck = Date.now();
      doHealthCheck();
    };
    const onVisible = () => {
      if (Date.now() - lastCheck >= intervalMs) check();
    };
    check();
    const interval = setInterval(check, intervalMs);
    document.addEventListener('visibilitychange', onVisible);
    return () => {
      clearInterval(interval);
      document.removeEventListener('visibilitychange', onVisible);
    };
  }, [doHealthCheck]);

  const title = text(artifact.title) || (port ? t('filePanel.previewFrameTitle', { port }) : t('filePanel.previewTab'));
  const thumb = (
    <span className="turn-file-thumb" aria-hidden="true">
      <span className="turn-file-sheet" />
      <span className="turn-file-page">
        <LayoutDashboard className="turn-file-glyph" />
        {port && <span className="turn-file-ext">:{port}</span>}
      </span>
    </span>
  );

  if (!workspaceId) {
    return (
      <div className="turn-file-card is-inline is-static is-term">
        {thumb}
        <span className="flex min-w-0 flex-1 flex-col gap-0.5">
          <span className="turn-file-name">{title}</span>
          <span className="turn-file-meta flex items-center gap-1.5">
            <Lock className="h-3 w-3 shrink-0" />
            <span className="truncate">{t('chat.previewCard.private')}</span>
          </span>
        </span>
      </div>
    );
  }

  // The menu closes on select, so the copy is confirmed by a toast rather
  // than by the item's own state.
  const copyLink = async () => {
    if (link && await copy(link.code, path ?? '/')) toast({ description: t('shareLink.copiedPrivate') });
  };

  return (
    <div className="turn-file-card is-inline is-term">
      {thumb}
      <button
        type="button"
        className="turn-file-hit"
        aria-label={t('chat.previewCard.openTitle', { name: title })}
        onClick={onClick}
      >
        <span className="turn-file-name">{title}</span>
        <span className="turn-file-meta flex items-center gap-1.5">
          <StatusGlyph health={health} />
          <span className="truncate">{t(`chat.previewCard.${health}`)}</span>
        </span>
      </button>
      <span className="turn-file-actions">
        <span className="turn-file-open" aria-hidden="true">
          {t('chat.turnFiles.open')}
        </span>
        <DropdownMenu modal={false} onOpenChange={setMenuOpen}>
          <DropdownMenuTrigger asChild>
            <button type="button" className="turn-file-more" aria-label={t('chat.turnFiles.moreActions')}>
              <ChevronDown className="h-3 w-3" />
            </button>
          </DropdownMenuTrigger>
          <DropdownMenuContent align="end" sideOffset={4}>
            <DropdownMenuItem onSelect={() => onClick?.()}>
              <PanelRight className="h-3.5 w-3.5" />
              {t('chat.turnFiles.open')}
            </DropdownMenuItem>
            <DropdownMenuItem
              disabled={!link}
              onSelect={() => link && window.open(shareLinkHref(link.code, path ?? '/'), '_blank', 'noopener,noreferrer')}
            >
              <ExternalLink className="h-3.5 w-3.5" />
              {t('filePanel.openInBrowser')}
            </DropdownMenuItem>
            <DropdownMenuItem disabled={!link} onSelect={() => void copyLink()}>
              <Link2 className="h-3.5 w-3.5" />
              {t('filePanel.copyPrivateLink')}
            </DropdownMenuItem>
          </DropdownMenuContent>
        </DropdownMenu>
      </span>
    </div>
  );
}
