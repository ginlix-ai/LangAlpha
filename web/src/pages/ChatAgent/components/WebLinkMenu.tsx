import React from 'react';
import { Copy, ExternalLink, Globe } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  ContextMenu,
  ContextMenuContent,
  ContextMenuItem,
  ContextMenuSeparator,
  ContextMenuTrigger,
} from '@/components/ui/context-menu';
import { copyWithToast } from '@/lib/clipboard';

/** Only a page on the web goes to the computer's browser, which opens nothing else. */
export function isWebAddress(href: string | undefined): href is string {
  return !!href && /^https?:\/\/[^\s]/i.test(href);
}

/**
 * A web link's right-click menu, which adds the computer's browser to where it can
 * open. It stands in for the browser's own menu on the link, so it keeps the two
 * entries people reach for there.
 */
export function WebLinkMenu({
  href,
  openInBrowser,
  children,
}: {
  href: string;
  openInBrowser: (url: string) => void;
  children: React.ReactElement;
}): React.ReactElement {
  const { t } = useTranslation();
  return (
    <ContextMenu>
      <ContextMenuTrigger asChild>{children}</ContextMenuTrigger>
      <ContextMenuContent>
        <ContextMenuItem onSelect={() => openInBrowser(href)}>
          <Globe className="h-4 w-4" />
          {t('linkMenu.openInBrowser')}
        </ContextMenuItem>
        <ContextMenuItem onSelect={() => window.open(href, '_blank', 'noopener,noreferrer')}>
          <ExternalLink className="h-4 w-4" />
          {t('linkMenu.openInNewTab')}
        </ContextMenuItem>
        <ContextMenuSeparator />
        <ContextMenuItem onSelect={() => copyWithToast(href, t('nav.linkCopied'), t('nav.copyLinkFailed'))}>
          <Copy className="h-4 w-4" />
          {t('linkMenu.copyLink')}
        </ContextMenuItem>
      </ContextMenuContent>
    </ContextMenu>
  );
}
