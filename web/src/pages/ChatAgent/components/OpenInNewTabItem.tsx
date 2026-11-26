import type React from 'react';
import { useTranslation } from 'react-i18next';
import { ExternalLink } from 'lucide-react';
import { useOpenInNewTab } from '@/components/PageTabs/useOpenInNewTab';

/**
 * "Open in new tab" at the head of a row's menu, with its separator. The
 * menu's own parts come in, since Radix ties an item to its own menu's
 * context; nothing renders where no tab can open.
 */
export function OpenInNewTabItem({ path, Item, Separator }: {
  path: string;
  Item: React.ComponentType<{ onSelect?: (event: Event) => void; children?: React.ReactNode }>;
  Separator: React.ComponentType;
}) {
  const { t } = useTranslation();
  const openInNewTab = useOpenInNewTab();
  if (!openInNewTab) return null;
  return (
    <>
      <Item onSelect={() => openInNewTab(path)}>
        <ExternalLink className="h-4 w-4" />
        {t('nav.openInNewTab')}
      </Item>
      <Separator />
    </>
  );
}
