import type { TFunction } from 'i18next';
import { toast } from '@/components/ui/use-toast';
import type { BrowserVisitRefusal } from './browserStream';

const REFUSAL_KEYS: Record<BrowserVisitRefusal, string> = {
  bad_address: 'filePanel.browser.badAddress',
  not_held: 'filePanel.browser.lostControl',
  not_running: 'filePanel.browser.idleTitle',
  failed: 'filePanel.browser.unreachable',
  no_answer: 'filePanel.browser.noAnswer',
};

/** Why a page the person asked for did not open, from the toolbar or a link. */
export function toastBrowserRefusal(t: TFunction, refusal: BrowserVisitRefusal): void {
  toast({
    variant: 'destructive',
    title: t('filePanel.browser.navigateFailed', "Couldn't open that page"),
    description: t(REFUSAL_KEYS[refusal]),
  });
}
