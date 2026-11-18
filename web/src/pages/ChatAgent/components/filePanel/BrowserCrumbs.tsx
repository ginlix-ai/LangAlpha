import React, { useState } from 'react';
import { ArrowLeft, ArrowRight, ExternalLink, Hand, RotateCw, type LucideIcon } from 'lucide-react';
import { useTranslation } from 'react-i18next';
import { Button } from '@/components/ui/button';
import { Loader } from '@/components/ui/loader';
import type { Computer } from '@/types/api';
import type { BrowserNavigation } from '../../utils/api/sandbox';
import { BrowserAddressField } from './BrowserAddressField';
import { showAddress } from './browserAddress';
import { BrowserMoreMenu } from './BrowserMoreMenu';
import { isBlankPage, isTransient } from './browserPhase';
import { toastBrowserRefusal } from './browserRefusal';
import type { BrowserViewSession } from './browserStream';
import { useBrowserViewState } from './useBrowserView';

type Step = Exclude<BrowserNavigation['action'], 'open'>;

const STEPS: readonly { action: Step; labelKey: string; label: string; Icon: LucideIcon }[] = [
  { action: 'back', labelKey: 'filePanel.browser.back', label: 'Back', Icon: ArrowLeft },
  { action: 'forward', labelKey: 'filePanel.browser.forward', label: 'Forward', Icon: ArrowRight },
  { action: 'reload', labelKey: 'filePanel.browser.reload', label: 'Reload', Icon: RotateCw },
];

/**
 * The browser's toolbar: back, forward, reload and the address, which are the
 * person's only while they hold the page, then the hold itself, the page in their
 * own browser, and a menu for the rest.
 */
export function BrowserCrumbs({
  view,
  computer,
}: {
  view: BrowserViewSession;
  /** The computer whose live view quality the menu sets; null where the reader
   *  may not change it, as a member of a shared workspace may not. */
  computer: Computer | null;
}): React.ReactElement {
  const { t } = useTranslation();
  const { phase, pageUrl, control } = useBrowserViewState(view);
  const working = isTransient(phase);
  const holding = control === 'held';
  const [stepping, setStepping] = useState(false);
  const steering = holding && phase === 'live';

  // Resolves to whether the page went; a refusal is said here, for the buttons and the field alike.
  const go = async (navigation: BrowserNavigation): Promise<boolean> => {
    const refusal = await view.navigate(navigation);
    if (refusal !== null) toastBrowserRefusal(t, refusal);
    return refusal === null;
  };
  const step = async (action: Step) => {
    setStepping(true);
    await go({ action });
    setStepping(false);
  };

  const shown = pageUrl && !isBlankPage(pageUrl) ? pageUrl : '';
  // A page on the computer itself is somewhere else in the person's own browser.
  const address = shown ? showAddress(shown) : null;
  const outside = !!address && !address.local;
  const openOutside = t('filePanel.browser.openOutside', 'Open in your browser');
  return (
    <div className="file-panel-crumbs">
      {STEPS.map(({ action, labelKey, label, Icon }) => (
        <button
          key={action}
          type="button"
          className="file-panel-icon-btn shrink-0"
          title={t(labelKey, label)}
          aria-label={t(labelKey, label)}
          disabled={!steering || stepping}
          onClick={() => void step(action)}
        >
          {action === 'reload' && (working || stepping)
            ? <Loader size={14} className="text-current" />
            : <Icon className="h-3.5 w-3.5" />}
        </button>
      ))}
      <BrowserAddressField
        pageUrl={shown}
        holding={holding}
        steering={steering && !stepping}
        onOpen={(url) => go({ action: 'open', url })}
      />
      {(phase === 'live' || holding) && (
        <Button
          type="button"
          variant={holding ? 'default' : 'outline'}
          size="sm"
          // The row's height, as the field and icons beside it are.
          className="h-6 shrink-0 gap-1 rounded px-2 text-xs"
          disabled={control === 'taking'}
          onClick={() => (holding ? view.releaseControl() : view.takeControl())}
        >
          {!holding && <Hand className="h-3 w-3" aria-hidden="true" />}
          {t(holding ? 'filePanel.browser.releaseControl' : 'filePanel.browser.takeControl')}
        </Button>
      )}
      {outside && (
        <a
          href={shown}
          target="_blank"
          rel="noopener noreferrer"
          className="file-panel-icon-btn shrink-0"
          title={openOutside}
          aria-label={openOutside}
        >
          <ExternalLink className="h-3.5 w-3.5" />
        </a>
      )}
      <BrowserMoreMenu computer={computer} pageUrl={shown || null} onReconnect={() => view.retry()} reconnecting={working} />
    </div>
  );
}
