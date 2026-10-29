import { useBrokerages, useBuiltinMcpServers, useMcpCatalog } from '@/hooks/useMcpServers';
import { usePlugins } from '@/hooks/usePlugins';
import { useSkills } from '@/hooks/useSkills';
import { useSubagents } from '@/hooks/useSubagents';
import { useUserVaultSecrets } from '@/hooks/useUserVault';
import { needsOauthConnect } from '@/pages/ChatAgent/components/mcp/mcpState';
import type { CatalogServer, PluginInfo, SubagentsResponse } from '@/pages/ChatAgent/utils/api';
import { connectorState, failureBucket, type PluginProblem } from '../../utils/connectorState';
import { hasLifecycle } from '../../utils/pluginSurface';
import { isEffectivelyEnabled } from '../../utils/provenance';
import { connectorNeedsUser } from '../connectors/attention';
import { pluginStatus } from '../pluginsPage/pluginStatus';

/**
 * What the rail and the page header say about every section at once: counts,
 * problems, and the installed plugins the rail lists.
 *
 * The same queries the sections render from (same keys, same arguments), so
 * React Query answers both from one fetch and the rail cannot count a list
 * the section does not show. A section whose query has not answered reads
 * null, and the surfaces say nothing for it rather than a zero.
 */
export interface ConnectorTally {
  /** Every connector the section lists, built in included. */
  total: number;
  /** Of those, the ones the app ships; null while their list is unanswered. */
  builtIn: number | null;
  signIn: number;
  key: number;
  rejected: number;
  unreachable: number;
  off: number;
}

export interface ShellData {
  plugins: {
    installed: PluginInfo[];
    builtIn: PluginInfo[];
    problems: ReadonlyMap<string, PluginProblem>;
    /** Plugins whose pending Connect is held up by a missing key, which is
     *  the thing to tell rather than the sign-in behind it. */
    keyMissing: ReadonlySet<string>;
    off: number;
  } | null;
  connectors: ConnectorTally | null;
  skills: { total: number; off: number } | null;
  subagents: {
    total: number;
    /** Rows with a problem, which the Needs attention pill counts too. */
    attention: number;
    /** What the agent itself runs on, which the summary names. */
    main: SubagentsResponse['main'];
  } | null;
  brokerages: { connected: number; signIn: number } | null;
  secrets: number | null;
}

/**
 * A connector offering Connect that never got past the vault: its check named
 * the values it is missing. The Connect stays, but what it needs first is a
 * key, which is how the plugin cells word it too.
 */
function wantsKey(server: CatalogServer): boolean {
  return (server.probe?.missing_secrets?.length ?? 0) > 0;
}

export function useShellData(): ShellData {
  const { data: pluginList } = usePlugins();
  const { data: catalog } = useMcpCatalog();
  const { data: builtinData } = useBuiltinMcpServers();
  // SkillsList's exact arguments: one cache entry for the list and the count.
  const { data: skills } = useSkills({ includeDisabled: true, allScopes: true });
  const { data: shipped } = useBrokerages();
  const { data: vault } = useUserVaultSecrets();
  const { data: subagentData } = useSubagents();

  const servers = catalog?.servers;

  let plugins: ShellData['plugins'] = null;
  if (pluginList) {
    const problems = new Map<string, PluginProblem>();
    const keyMissing = new Set<string>();
    for (const plugin of pluginList.plugins) {
      if (!servers) break;
      // The cell's rule: a plugin that is off, or whose connectors only wait
      // on a first check, has nothing for the user to fix.
      const { problem, needsUser, missingKeys } = pluginStatus(plugin, servers);
      if (!problem || !needsUser) continue;
      problems.set(plugin.name, problem);
      if (problem.kind === 'connect' && missingKeys.length > 0) keyMissing.add(plugin.name);
    }
    plugins = {
      installed: pluginList.plugins.filter(hasLifecycle),
      builtIn: pluginList.plugins.filter((p) => !hasLifecycle(p)),
      problems,
      keyMissing,
      off: pluginList.plugins.filter((p) => !p.enabled).length,
    };
  }

  // Every connector the section lists: what the user added, what their
  // plugins installed, and what ships with the app, which the section shows
  // under Built in and so counts as Plugins and Skills count theirs.
  let connectors: ConnectorTally | null = null;
  if (servers) {
    const builtins = builtinData?.servers;
    const tally: ConnectorTally = {
      total: servers.length + (builtins?.length ?? 0),
      builtIn: builtins ? builtins.length : null,
      signIn: 0,
      key: 0,
      rejected: 0,
      unreachable: 0,
      off: 0,
    };
    for (const server of servers) {
      const state = connectorState(server);
      if (connectorNeedsUser(server, state)) {
        if (state.kind === 'connect') tally[wantsKey(server) ? 'key' : 'signIn'] += 1;
        else if (state.kind === 'failing') tally[failureBucket(state.noteKey)] += 1;
      } else if (!isEffectivelyEnabled(server)) {
        tally.off += 1;
      }
    }
    for (const server of builtins ?? []) {
      if (!isEffectivelyEnabled(server)) tally.off += 1;
    }
    connectors = tally;
  }

  let brokerages: ShellData['brokerages'] = null;
  if (shipped && servers) {
    const byName = new Map(servers.map((s) => [s.name, s]));
    let connected = 0;
    let signIn = 0;
    for (const b of shipped) {
      const row = byName.get(b.name);
      if (!row) continue;
      // The card's rule: only an http row can sign in, and the user may have
      // edited theirs to something else.
      if (row.transport === 'http' && needsOauthConnect(row.oauth_status ?? null)) signIn += 1;
      else connected += 1;
    }
    brokerages = { connected, signIn };
  }

  return {
    plugins,
    connectors,
    skills: skills
      ? { total: skills.length, off: skills.filter((s) => !isEffectivelyEnabled(s)).length }
      : null,
    subagents: subagentData
      ? {
          total: subagentData.subagents.length,
          attention: subagentData.subagents.filter((s) => s.problems.length > 0).length,
          main: subagentData.main,
        }
      : null,
    brokerages,
    secrets: vault ? vault.secrets.length : null,
  };
}

export function connectorAttention(tally: ConnectorTally): number {
  return tally.signIn + tally.key + tally.rejected + tally.unreachable;
}
