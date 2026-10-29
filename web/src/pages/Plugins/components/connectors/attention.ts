import type { CatalogServer } from '@/pages/ChatAgent/utils/api';
import { connectorState, type ConnectorState } from '../../utils/connectorState';

/**
 * Whether a connector is something the user has to act on: a check refused it,
 * it is missing a key, or it needs a sign-in that something has asked for (a
 * check that answered with one, or a connection that lapsed).
 *
 * Narrower than `connectorState`'s Connect on purpose. An http row nothing has
 * checked yet still offers Connect, since nobody knows it wants no sign-in, but
 * nothing has gone wrong with it either: counted here, a handful of connectors
 * added and left off filled "Needs attention" with rows that need nothing.
 */
export function connectorNeedsUser(
  server: CatalogServer,
  state: ConnectorState = connectorState(server),
): boolean {
  if (state.kind === 'failing') return true;
  if (state.kind !== 'connect') return false;
  return state.status !== null || !!server.probe;
}
