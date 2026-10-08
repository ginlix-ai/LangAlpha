import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useToggleBrokerage } from '@/hooks/useMcpServers';
import { formatApiErrorDetail, type CatalogServer } from '@/pages/ChatAgent/utils/api';
import type { Brokerage } from '../brokerages';
import { useMcpOauthActions } from './useMcpOauthActions';

/**
 * Connect a shipped brokerage, from whichever surface offers one.
 *
 * The connect lifecycle and its questions are `useMcpOauthActions`'. What this
 * adds is the pair of steps a brokerage alone needs: it has no catalog row
 * until someone connects it, and an inert row's grant would be revoked the
 * moment it landed, so bringing the row to life is part of connecting. It runs
 * after the consent question, not before, and comes back off if the flow never
 * reached the vendor. Shared so the Brokerages tab and onboarding cannot
 * connect the same broker two different ways.
 */
export function useBrokerageConnect({ returnTo }: { returnTo: string }) {
  const { t } = useTranslation();
  const oauth = useMcpOauthActions({ returnTo });
  const toggleMutation = useToggleBrokerage();

  /** Create-and-enable, the step a connect implies when there is no row yet. */
  async function ensureLive(name: string): Promise<boolean> {
    try {
      await toggleMutation.mutateAsync({ name, enabled: true });
      return true;
    } catch (err) {
      toast({
        variant: 'destructive',
        title: t('plugins.brokerages.toggleFailed'),
        description: formatApiErrorDetail(err),
      });
      return false;
    }
  }

  /**
   * Stand down a row `ensureLive` brought up for a connect that never happened.
   *
   * Switched off rather than deleted, which is the same outcome for both shapes
   * of `wasInert` and the safe one for either: a disabled row is an inert
   * template, in no workspace's effective set and carrying nothing. Deleting
   * would also be right for a row this click created, and destructive for one
   * the user had already made and merely switched off, and by the time this
   * runs the two are no longer distinguishable.
   */
  async function revertLive(name: string) {
    try {
      await toggleMutation.mutateAsync({ name, enabled: false });
    } catch {
      // Silent by choice: the connect failure is already on screen and is the
      // one the user can act on. A second toast about the tidying would bury
      // it, and the row it leaves behind is visible and switchable on the row
      // itself.
    }
  }

  function requestConnect(
    brokerage: Brokerage,
    row: CatalogServer | null,
    vendor: Brokerage | null,
  ) {
    const wasInert = !row?.enabled;
    oauth.connect({
      name: brokerage.name,
      vendor,
      // The row's own address once it has one, and otherwise the address the
      // row `prepare` is about to create will carry -- which is the registry's,
      // the one thing here the user does not choose.
      url: row?.url ?? brokerage.url,
      // What the row already grants, so the dialog opens on the answer the user
      // gave last time rather than on the vendor's default.
      granted: row?.remembered_capabilities ?? null,
      prepare: wasInert ? () => ensureLive(brokerage.name) : undefined,
      rollback: wasInert ? () => revertLive(brokerage.name) : undefined,
    });
  }

  // `turnOn` is for a row whose connection outlived its switch: the tokens are
  // still there, so switching it back on is the whole connect.
  return { oauth, requestConnect, turnOn: ensureLive };
}
