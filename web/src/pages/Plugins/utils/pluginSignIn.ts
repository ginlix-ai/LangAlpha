import type { McpSignInNeeded } from '@/pages/ChatAgent/utils/api';
import type { ConnectRequest } from '../hooks/useMcpOauthActions';
import type { PluginSource } from '../components/PluginSourceStep';

/** An install the server answered with "sign in first", as it was sent. */
export interface PluginInstallAttempt {
  source: PluginSource;
  subdir?: string;
  secrets?: Record<string, string>;
  /** What the user knows the plugin's vendor as, when the caller knows. */
  label?: string;
  /** How to put the user back if the sign-in does not finish, when not
   *  the wizard on this source: a Discover entry reopens its own page. */
  retry?: ConnectRequest['retry'];
  /** Where the sign-in comes back to, when not the surface's own page. */
  returnTo?: string;
}

/**
 * The sign-in that installs a plugin, after its install answered 409
 * `mcp_sign_in_required`. The install is carried whole, keys included, and
 * the callback installs it once the user has signed in and every server
 * passes; nothing is installed before then.
 *
 * Named for the first server listed: an install carries at most one sign-in,
 * and that server's name is what the callback reports.
 */
export function pluginSignInRequest(
  servers: McpSignInNeeded[],
  attempt: PluginInstallAttempt,
  outcomes: Pick<ConnectRequest, 'onNotStarted' | 'onRefused' | 'onHandedOff'>,
): ConnectRequest | null {
  const first = servers[0];
  if (!first) return null;
  const { source, subdir, secrets, label, retry, returnTo } = attempt;
  return {
    ...outcomes,
    name: first.server,
    label,
    retry,
    returnTo,
    url: first.url || null,
    granted: null,
    pending:
      'file' in source
        ? { kind: 'plugin_upload', file: source.file, subdir, secrets }
        : { kind: 'plugin', sourceUrl: source.url, subdir, secrets },
  };
}
