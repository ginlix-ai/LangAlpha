import React from 'react';
import { Check, ChevronRight, Copy, MoreHorizontal, RefreshCw, SlidersHorizontal } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuRadioGroup,
  DropdownMenuRadioItem,
  DropdownMenuSeparator,
  DropdownMenuSub,
  DropdownMenuSubContent,
  DropdownMenuSubTrigger,
  DropdownMenuTrigger,
} from '@/components/ui/dropdown-menu';
import { copyWithToast } from '@/lib/clipboard';
import type { Computer } from '@/types/api';
import { hasBrowser } from '../../utils/computerStatus';
import { QUALITY_CHOICES, isQualityChoice, useBrowserQuality } from '../browserQuality';

/**
 * What the tab does less often than going somewhere: copy the page's address,
 * reconnect the view, and the computer's live view quality, the same setting as
 * its details, so a choice reaches the next browser the computer opens.
 */
export function BrowserMoreMenu({
  computer,
  pageUrl,
  onReconnect,
  reconnecting,
}: {
  computer: Computer | null;
  /** The page's address, or null on a blank page. */
  pageUrl: string | null;
  onReconnect: () => void;
  reconnecting: boolean;
}): React.ReactElement {
  const { t } = useTranslation();
  const { value, label, retinaHeld, autoIs, set } = useBrowserQuality(computer);
  const title = t('filePanel.browser.more', 'More');
  const copy = () => {
    if (!pageUrl) return;
    copyWithToast(pageUrl, t('nav.linkCopied'), t('nav.copyLinkFailed'));
  };
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <button type="button" className="file-panel-icon-btn shrink-0" title={title} aria-label={title}>
          <MoreHorizontal className="h-3.5 w-3.5" />
        </button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-60">
        <DropdownMenuItem disabled={!pageUrl} onSelect={copy}>
          <Copy className="h-4 w-4" />
          {t('filePanel.browser.copyAddress', 'Copy address')}
        </DropdownMenuItem>
        <DropdownMenuItem disabled={reconnecting} onSelect={onReconnect}>
          <RefreshCw className="h-4 w-4" />
          {t('filePanel.browser.reconnect')}
        </DropdownMenuItem>
        {computer && hasBrowser(computer) && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuSub>
              <DropdownMenuSubTrigger>
                <SlidersHorizontal className="h-4 w-4" />
                <span className="flex-1 whitespace-nowrap">{t('computer.browserQuality.label', 'Live view quality')}</span>
                <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{label(value)}</span>
                <ChevronRight className="h-3.5 w-3.5" style={{ color: 'var(--color-text-tertiary)' }} />
              </DropdownMenuSubTrigger>
              <DropdownMenuSubContent className="w-56">
                <DropdownMenuRadioGroup
                  value={value}
                  onValueChange={(choice) => {
                    if (isQualityChoice(choice)) set(choice);
                  }}
                >
                  {QUALITY_CHOICES.map((choice) => (
                    <DropdownMenuRadioItem key={choice} value={choice} disabled={choice === 'retina' && retinaHeld}>
                      <Check className="h-3.5 w-3.5 shrink-0" style={{ opacity: value === choice ? 1 : 0 }} aria-hidden="true" />
                      <span className="flex-1">{label(choice)}</span>
                      {choice === 'auto' && autoIs && (
                        <span className="text-xs" style={{ color: 'var(--color-text-tertiary)' }}>{label(autoIs)}</span>
                      )}
                    </DropdownMenuRadioItem>
                  ))}
                </DropdownMenuRadioGroup>
                <DropdownMenuSeparator />
                <p className="px-2.5 py-1.5 text-xs" style={{ color: 'var(--color-text-tertiary)' }}>
                  {value === 'retina' && retinaHeld
                    ? t('computer.browserQuality.retinaFallback', 'Retina needs 2 or more CPUs, so this computer uses Sharp.') + ' '
                    : ''}
                  {t('filePanel.browser.qualityNote', 'Applies the next time the browser opens.')}
                </p>
              </DropdownMenuSubContent>
            </DropdownMenuSub>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
