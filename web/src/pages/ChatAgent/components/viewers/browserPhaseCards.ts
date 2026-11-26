import { AlertCircle, Globe } from 'lucide-react';
import type { BrowserPhase } from '../filePanel/browserPhase';
import type { StatusIcon } from './StatusCard';

export interface PhaseCard {
  icon: StatusIcon;
  titleKey: string;
  bodyKey?: string;
  /** Open starts what is not up; retry only connects again; chrome downloads
   *  Chrome for Testing to a machine of the user's own that has no browser. */
  action?: { run: 'open' | 'retry' | 'chrome'; labelKey: string };
}

/** The phases with no card. Live shows the page; with no browser open or no
 *  computer running the start page stands there instead, and going somewhere
 *  from it opens both (`BrowserStartPage`). */
type Uncarded = 'live' | 'idle' | 'stopped';

/** The card each other phase puts over the view. */
const PHASE_CARDS: Record<Exclude<BrowserPhase, Uncarded>, PhaseCard> = {
  checking: { icon: 'working', titleKey: 'filePanel:browser.checking' },
  starting: { icon: 'working', titleKey: 'filePanel:browser.starting' },
  connecting: { icon: 'working', titleKey: 'filePanel:browser.connecting' },
  reconnecting: { icon: 'working', titleKey: 'filePanel:browser.reconnecting' },
  low_memory: {
    icon: AlertCircle,
    titleKey: 'filePanel:browser.lowMemoryTitle',
    bodyKey: 'filePanel:browser.lowMemoryBody',
    action: { run: 'open', labelKey: 'filePanel:browser.retry' },
  },
  outdated: { icon: Globe, titleKey: 'filePanel:browser.outdatedTitle', bodyKey: 'filePanel:browser.outdatedBody' },
  no_chrome: {
    icon: Globe,
    titleKey: 'filePanel:browser.noChromeTitle',
    bodyKey: 'filePanel:browser.noChromeBody',
    action: { run: 'chrome', labelKey: 'filePanel:browser.getChrome' },
  },
  installing: {
    icon: 'working',
    titleKey: 'filePanel:browser.installingTitle',
    bodyKey: 'filePanel:browser.installingBody',
  },
  unavailable: { icon: Globe, titleKey: 'filePanel:browser.unavailableTitle', bodyKey: 'filePanel:browser.unavailableBody' },
  failed: {
    icon: AlertCircle,
    titleKey: 'filePanel:browser.failedTitle',
    bodyKey: 'filePanel:browser.failedBody',
    action: { run: 'retry', labelKey: 'filePanel:browser.retry' },
  },
};

export function cardOf(phase: BrowserPhase): PhaseCard | null {
  return phase === 'live' || phase === 'idle' || phase === 'stopped' ? null : PHASE_CARDS[phase];
}
