import { useTranslation } from 'react-i18next';
import { toast } from '@/components/ui/use-toast';
import { useToggleBrokerage } from '@/hooks/useMcpServers';
import { formatApiErrorDetail, type CatalogServer } from '@/pages/ChatAgent/utils/api';
import type { Brokerage } from '../brokerages';
import { connectRequestFor, useMcpOauthActions, type ConnectRequest } from './useMcpOauthActions';

/**
 * Connect a shipped brokerage, from whichever surface offers one.
 *
 * The connect lifecycle and its questions are `useMcpOauthActions`'. What this
 * adds is the request a brokerage's connect makes: a broker with no row yet
 * starts the sign-in that creates it, so nothing is saved before the vendor
 * answers. Shared so the Brokerages tab and onboarding cannot connect the same
 * broker two different ways. A surface that holds the user while the sign-in
 * starts passes `onHandedOff` to let go when the desktop app hands the vendor's
 * page to the system browser.
 */
export function useBrokerageConnect({
  returnTo,
  onHandedOff,
}: {
  returnTo: string;
  onHandedOff?: () => void;
}) {
  const { t } = useTranslation();
  const oauth = useMcpOauthActions({ returnTo });
  const toggleMutation = useToggleBrokerage();

  /**
   * The connect for this broker, as the lifecycle takes it.
   *
   * A broker with a row reconnects through it, and a reconnect that lands
   * switches the row on. One with no row yet starts the sign-in that creates
   * it, carrying the capability answer the consent dialog collects.
   */
  function connectRequest(brokerage: Brokerage, row: CatalogServer | null): ConnectRequest {
    return {
      ...connectRequestFor(row ?? brokerage),
      name: brokerage.name,
      // The row's own address once it has one, and otherwise the registry's,
      // the one thing here the user does not choose.
      url: row?.url ?? brokerage.url,
      pending: row ? undefined : { kind: 'brokerage', name: brokerage.name },
      onHandedOff,
    };
  }

  function requestConnect(brokerage: Brokerage, row: CatalogServer | null) {
    oauth.connect(connectRequest(brokerage, row));
  }

  /**
   * Switch a broker's row on or off. On a row whose connection outlived its
   * switch the tokens are still there, so switching it back on is the whole
   * connect; a switch cannot sign anyone in, so a row refused for want of a
   * connection is offered the sign-in instead.
   */
  async function toggle(
    brokerage: Brokerage,
    row: CatalogServer | null,
    enabled: boolean,
  ): Promise<boolean> {
    try {
      await toggleMutation.mutateAsync({ name: brokerage.name, enabled });
      return true;
    } catch (err) {
      if (!oauth.offerSignInIfRefused(err, connectRequest(brokerage, row))) {
        toast({
          variant: 'destructive',
          title: t('plugins.brokerages.toggleFailed'),
          description: formatApiErrorDetail(err),
        });
      }
      return false;
    }
  }

  // The broker whose switch is on its way, by name, so a click on one broker
  // never greys out another's controls.
  const togglingName = toggleMutation.isPending ? toggleMutation.variables.name : null;

  return { oauth, requestConnect, toggle, togglingName };
}
