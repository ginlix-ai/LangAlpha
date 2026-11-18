import { AlertCircle, Globe } from 'lucide-react';
import type { BrowserPhase } from '../filePanel/browserPhase';
import type { StatusIcon } from './StatusCard';

export interface PhaseCard {
  icon: StatusIcon;
  titleKey: string;
  bodyKey?: string;
  /** Open starts what is not up; retry only connects again. */
  action?: { run: 'open' | 'retry'; labelKey: string };
}

/** The card each phase puts over the view. Live has none: the page is the view. */
export const PHASE_CARDS: Record<BrowserPhase, PhaseCard | null> = {
  checking: { icon: 'working', titleKey: 'filePanel.browser.checking' },
  starting: { icon: 'working', titleKey: 'filePanel.browser.starting' },
  connecting: { icon: 'working', titleKey: 'filePanel.browser.connecting' },
  reconnecting: { icon: 'working', titleKey: 'filePanel.browser.reconnecting' },
  live: null,
  idle: {
    icon: Globe,
    titleKey: 'filePanel.browser.idleTitle',
    bodyKey: 'filePanel.browser.idleBody',
    action: { run: 'open', labelKey: 'filePanel.browser.open' },
  },
  low_memory: {
    icon: AlertCircle,
    titleKey: 'filePanel.browser.lowMemoryTitle',
    bodyKey: 'filePanel.browser.lowMemoryBody',
    action: { run: 'open', labelKey: 'filePanel.browser.retry' },
  },
  stopped: {
    icon: Globe,
    titleKey: 'filePanel.browser.stoppedTitle',
    bodyKey: 'filePanel.browser.stoppedBody',
    action: { run: 'open', labelKey: 'filePanel.browser.startAndOpen' },
  },
  outdated: { icon: Globe, titleKey: 'filePanel.browser.outdatedTitle', bodyKey: 'filePanel.browser.outdatedBody' },
  unavailable: { icon: Globe, titleKey: 'filePanel.browser.unavailableTitle', bodyKey: 'filePanel.browser.unavailableBody' },
  failed: {
    icon: AlertCircle,
    titleKey: 'filePanel.browser.failedTitle',
    bodyKey: 'filePanel.browser.failedBody',
    action: { run: 'retry', labelKey: 'filePanel.browser.retry' },
  },
};
