import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { useSetBrowserChoices } from '../hooks/useSetBrowserChoices';
import { useComputerProviders } from '../hooks/useComputers';
import type { BrowserQuality } from '../utils/api';
import { effectiveTier } from './specChangeUi';

export type QualityChoice = BrowserQuality | 'auto';
export const QUALITY_CHOICES: readonly QualityChoice[] = ['auto', 'light', 'standard', 'sharp', 'retina'];

/** Where a string from a menu becomes a choice. */
export function isQualityChoice(value: unknown): value is QualityChoice {
  return QUALITY_CHOICES.some((choice) => choice === value);
}

/** As the computer's browser runtime decides (`browser_runtime/quality.py`):
 *  retina's frames take a whole CPU, so it needs two, and four get it by default
 *  except on a machine of the user's own. */
const RETINA_MIN_CPUS = 2;
const RETINA_BY_DEFAULT = 4;

export interface BrowserQualityState {
  /** What is saved for the computer, Auto when nothing is. */
  value: QualityChoice;
  label: (choice: QualityChoice) => string;
  /** Retina is not on offer: the computer's size has too few CPUs for it. */
  retinaHeld: boolean;
  /** What Auto picks on this computer, when its size is known. */
  autoIs: BrowserQuality | null;
  /** A change for this computer is in flight. */
  busy: boolean;
  /** Auto clears what is saved. */
  set: (choice: QualityChoice) => void;
}

/** The computer's live view quality and how to change it, shared by the Browser
 *  tab's menu and the computer's details so both read and write it one way. */
export function useBrowserQuality(computer: Computer | null | undefined): BrowserQualityState {
  const { t } = useTranslation();
  const { data } = useComputerProviders({ enabled: !!computer });
  const mutation = useSetBrowserChoices();
  // A machine of the user's own has no sizes to read: it reports its cores, and
  // Auto picks Standard there whatever their count.
  const own = computer?.ssh != null;
  const cpus = !computer
    ? undefined
    : computer.ssh
      ? computer.ssh.cores ?? undefined
      : data?.providers
        .find((p) => p.kind === computer.kind)
        ?.tiers?.find((s) => s.tier === effectiveTier(computer))?.cpu;
  const label = (choice: QualityChoice) =>
    ({
      auto: t('computer.browserQuality.auto', 'Auto'),
      light: t('computer.browserQuality.light', 'Light'),
      standard: t('computer.browserQuality.standard', 'Standard'),
      sharp: t('computer.browserQuality.sharp', 'Sharp'),
      retina: t('computer.browserQuality.retina', 'Retina'),
    })[choice];
  return {
    value: computer?.browser?.quality ?? 'auto',
    label,
    retinaHeld: cpus !== undefined && cpus < RETINA_MIN_CPUS,
    autoIs: own ? 'standard' : cpus === undefined ? null : cpus >= RETINA_BY_DEFAULT ? 'retina' : 'standard',
    busy: !!computer && mutation.busyFor(computer.computer_id),
    set: (choice) => {
      if (!computer) return;
      mutation.mutate({ computerId: computer.computer_id, patch: { quality: choice === 'auto' ? null : choice } });
    },
  };
}
