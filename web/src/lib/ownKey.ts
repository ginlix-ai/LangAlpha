import type { ConfiguredProvider } from '@/hooks/useConfiguredProviders';

/** How the server masks a key that is the deployment's own, from its environment. */
export const SERVER_KEY = '(server)';

/** A key saved by the user, not the deployment's own, which has no row of
 *  theirs to change. */
export const isOwnKey = (p: ConfiguredProvider | null | undefined): p is ConfiguredProvider =>
  !!p && !!p.masked_key && p.masked_key !== SERVER_KEY;
