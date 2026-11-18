import type { Brokerage } from '../../brokerages';

/** Whether a rule against the user's own connectors keeps this one back.
 *  A brokerage's is the trading rule's, told apart by its address as the
 *  server tells it; `vendor` is undefined while the registry loads, which
 *  holds nothing back yet. */
export function connectorHeldBack(ownConnectorsAllowed: boolean, vendor: Brokerage | null | undefined): boolean {
  return !ownConnectorsAllowed && vendor === null;
}
