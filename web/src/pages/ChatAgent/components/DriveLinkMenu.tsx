import React from 'react';
import { Copy, ExternalLink, HardDrive } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { useInRouterContext, useNavigate } from 'react-router';
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from '@/components/ui/context-menu';
import { useIsMobile } from '@/hooks/useIsMobile';
import { copyWithToast } from '@/lib/clipboard';
import { useLinkActions } from '../contexts/LinkActionsContext';
import { useRouteLeaveGuard } from '../contexts/RouteLeaveGuardContext';

interface DriveLinkMenuProps {
  /** The Drive page address the link names, `/drive?…`. */
  href: string;
  children: React.ReactElement;
}

/**
 * A Drive link's right-click menu, where the chat opens the link beside it.
 * The menu offers the places that click does not reach: the Drive page
 * itself, a new tab, and the link to share. Anywhere else (a share, the
 * Drive page) the link goes to the page, and the browser's own menu is
 * right. A phone keeps its own long press, as it does on a web link.
 */
export function DriveLinkMenu({ href, children }: DriveLinkMenuProps): React.ReactElement {
  const mobile = useIsMobile();
  const inRouter = useInRouterContext();
  const { openAppRoute } = useLinkActions();
  if (mobile || !inRouter || !openAppRoute) return children;
  return <DriveLinkMenuBody href={href}>{children}</DriveLinkMenuBody>;
}

function DriveLinkMenuBody({ href, children }: DriveLinkMenuProps): React.ReactElement {
  const { t } = useTranslation();
  const navigate = useNavigate();
  const guardLeave = useRouteLeaveGuard();
  return (
    <ContextMenu>
      <ContextMenuTrigger asChild>{children}</ContextMenuTrigger>
      <ContextMenuContent>
        <ContextMenuItem onSelect={() => guardLeave(() => navigate(href))}>
          <HardDrive className="h-4 w-4" />
          {t('linkMenu.openInDrive')}
        </ContextMenuItem>
        <ContextMenuItem onSelect={() => window.open(href, '_blank', 'noopener,noreferrer')}>
          <ExternalLink className="h-4 w-4" />
          {t('linkMenu.openInNewTab')}
        </ContextMenuItem>
        <ContextMenuSeparator />
        <ContextMenuItem
          onSelect={() => copyWithToast(new URL(href, window.location.origin).href, t('nav.linkCopied'), t('nav.copyLinkFailed'))}
        >
          <Copy className="h-4 w-4" />
          {t('linkMenu.copyLink')}
        </ContextMenuItem>
      </ContextMenuContent>
    </ContextMenu>
  );
}
