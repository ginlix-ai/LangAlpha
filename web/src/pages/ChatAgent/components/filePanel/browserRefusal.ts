import type { TFunction } from 'i18next';
import { toast } from '@/components/ui/use-toast';
import type { BrowserCloseRefusal } from '../../utils/api/sandbox';
import type { BrowserViewSession, BrowserVisitRefusal } from './browserStream';

const REFUSAL_KEYS: Record<BrowserVisitRefusal, string> = {
  bad_address: 'filePanel:browser.badAddress',
  not_held: 'filePanel:browser.lostControl',
  not_running: 'filePanel:browser.idleTitle',
  failed: 'filePanel:browser.unreachable',
  no_answer: 'filePanel:browser.noAnswer',
};

const CLOSE_REFUSAL_KEYS: Record<BrowserCloseRefusal, string> = {
  held: 'filePanel:browser.heldByOther',
  failed: 'filePanel:browser.noAnswer',
};

/** Why the browser the person asked to close is still open. */
export function toastBrowserCloseRefusal(t: TFunction, refusal: BrowserCloseRefusal): void {
  toast({
    variant: 'destructive',
    title: t('filePanel:browser.closeFailed', "Couldn't close the browser"),
    description: t(CLOSE_REFUSAL_KEYS[refusal]),
  });
}

/** Take *view*'s browser to what the person typed or clicked; resolves to
 *  whether it went. A browser that could not be opened is said by the tab,
 *  which shows why; anything else that stopped it is said here, as is one that
 *  closed under a visit while the tab still shows it live. */
export async function visitFor(t: TFunction, view: BrowserViewSession, typed: string): Promise<boolean> {
  const refusal = await view.visit(typed);
  const shown = refusal === 'not_running' && view.getSnapshot().phase !== 'live';
  if (refusal !== null && !shown) toastBrowserRefusal(t, refusal);
  return refusal === null;
}

/** Why a page the person asked for did not open. */
export function toastBrowserRefusal(t: TFunction, refusal: BrowserVisitRefusal): void {
  toast({
    variant: 'destructive',
    title: t('filePanel:browser.navigateFailed', "Couldn't open that page"),
    description: t(REFUSAL_KEYS[refusal]),
  });
}
