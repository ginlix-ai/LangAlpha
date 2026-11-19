import { useTranslation } from 'react-i18next';
import { useRule } from '@/hooks/useAccount';
import { RULE } from '@/lib/accountRules';
import type { WorkspaceQuota } from '@/types/api';
import { computerCapReached, type ProviderGate } from '../components/providerUi';

/** What locks providers for a new computer, read once for every surface
 *  that offers or holds one. */
export function useProviderGate(quota: WorkspaceQuota | null | undefined): ProviderGate {
  const { t } = useTranslation();
  const ownMachines = useRule(RULE.ownMachines);
  return {
    atCap: computerCapReached(quota),
    ownMachinesRule: ownMachines.allowed ? null : t('organization.machines.cardBody', { who: ownMachines.who.start }),
  };
}
