import { useTranslation } from 'react-i18next';

import type { Computer } from '@/types/api';

import { computerSourceLabel, rowsShowProvider } from '../components/providerUi';
import { useComputerProviders } from './useComputers';
import { useThisMac } from './useRemoteControl';

/**
 * What a list of computers says each runs on (this Mac, remote control, or its
 * provider), by the rule the Computers dialog follows: null on every row
 * unless there is something to tell apart.
 */
export function useComputerSource(
  computers: readonly Computer[],
  options: { enabled: boolean },
): (computer: Computer) => string | null {
  const { t } = useTranslation();
  const { data } = useComputerProviders(options);
  const thisMac = useThisMac(options);
  const shown = rowsShowProvider(data?.providers, computers);
  return (computer) => (shown ? computerSourceLabel(t, computer, thisMac.of(computer) !== null) : null);
}
