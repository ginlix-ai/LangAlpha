import type { TFunction } from 'i18next';
import { serverKeyRefs } from '@/hooks/useSecretUsage';
import type { CatalogServer, MarketplaceEntry, PluginInfo, SubagentView } from '@/pages/ChatAgent/utils/api';
import {
  connectorState,
  pluginConnectors,
  pluginProblem,
  type ConnectorState,
  type PluginProblem,
} from '../../utils/connectorState';
import { isBundled } from '../../utils/pluginSurface';
import { connectorNeedsUser } from '../connectors/attention';

const KEY_REJECTED = 'mcp.probe.rowCredentialRejected';

/** What fixes a plugin's problem, which its cell and its page both offer. */
export type PluginFix =
  /** A key a check found missing: Secrets' add form, with its name. */
  | { kind: 'addKey'; key: string }
  /** A key a server refused: that key's own form, or Secrets when the
   *  refusing connectors read more than one, since which is wrong is not
   *  known. `by` are those connectors. */
  | { kind: 'replaceKey'; key: string | null; by: CatalogServer[] }
  /** The sign-in `server` waits on. */
  | { kind: 'signIn' }
  /** Anything else, which `server`'s own page says how to fix. */
  | { kind: 'open' };

/**
 * A plugin read the way every surface says it: the problem `pluginProblem`
 * settles on, the connector it is about, and what fixes it. A switched-off
 * plugin has no problem: nothing reaches the agent from it until it is back
 * on, so there is nothing to fix yet.
 */
export interface PluginStatus {
  problem: PluginProblem | null;
  /** Whether the problem is the user's to act on, the rule every attention
   *  count reads. Narrower than `problem`: a connector nothing has checked
   *  offers Connect without anything having gone wrong with it. */
  needsUser: boolean;
  /** The connector the problem reports: the one whose reason the line names. */
  server: CatalogServer | null;
  fix: PluginFix | null;
  /** Vault names a problem connector asks for and the vault does not hold. */
  missingKeys: string[];
  /** Every vault name the plugin's connectors read, for the Keys block. */
  keys: string[];
}

export function pluginStatus(plugin: PluginInfo, servers: CatalogServer[]): PluginStatus {
  const own = pluginConnectors(plugin, servers).map((server) => ({ server, state: connectorState(server) }));
  const keys = unique(own.flatMap(({ server }) => serverKeyRefs(server)));
  const problem = plugin.enabled ? pluginProblem(plugin, servers) : null;
  if (!problem) return { problem, needsUser: false, server: null, fix: null, missingKeys: [], keys };

  const inProblem = own.filter(({ state }) => state.kind === problem.kind);
  const reported = (state: ConnectorState) =>
    problem.kind === 'connect' || (state.kind === 'failing' && state.noteKey === problem.noteKey);
  const server = inProblem.find(({ state }) => reported(state))?.server ?? null;
  const missingKeys = unique(inProblem.flatMap(({ server }) => server.probe?.missing_secrets ?? []));
  const needsUser = own.some(({ server, state }) => connectorNeedsUser(server, state));

  return { problem, needsUser, server, fix: fixFor(problem, inProblem, missingKeys), missingKeys, keys };
}

function fixFor(
  problem: PluginProblem,
  inProblem: { server: CatalogServer; state: ConnectorState }[],
  missingKeys: string[],
): PluginFix {
  if (missingKeys.length > 0) return { kind: 'addKey', key: missingKeys[0] };
  if (problem.kind === 'connect') return { kind: 'signIn' };
  if (problem.noteKey !== KEY_REJECTED) return { kind: 'open' };
  const by = inProblem.flatMap(({ server, state }) =>
    state.kind === 'failing' && state.noteKey === KEY_REJECTED ? [server] : [],
  );
  const refs = unique(by.flatMap(serverKeyRefs));
  // A refusal that names no stored key has no key to replace.
  if (refs.length === 0) return { kind: 'open' };
  return { kind: 'replaceKey', key: refs.length === 1 ? refs[0] : null, by };
}

function unique(values: string[]): string[] {
  return [...new Set(values)].sort();
}

/** The short reason for a probe note, as a plugin cell words it. Literal keys
 *  so the locale check can see each one. */
const REASON_KEY: Record<string, string> = {
  'mcp.probe.rowCredentialRejected': 'plugins.pluginsPage.reason.rejected',
  'mcp.probe.rowMissingSecrets': 'plugins.pluginsPage.reason.missing',
  'mcp.probe.rowCredential': 'plugins.pluginsPage.reason.credential',
  'mcp.probe.rowUnreachable': 'plugins.pluginsPage.reason.unreachable',
  'mcp.probe.rowOauth': 'plugins.pluginsPage.reason.signIn',
};

export function reasonLabel(t: TFunction, noteKey: string): string {
  return t(REASON_KEY[noteKey] ?? noteKey);
}

/** The one line a plugin cell shows, and whether it is the problem. A newer
 *  version in the catalog takes the description's place, never a problem's. */
export function statusLine(
  t: TFunction,
  plugin: PluginInfo,
  status: PluginStatus,
  updateAvailable = false,
): { text: string; warn: boolean } {
  const { problem, server, missingKeys } = status;
  if (problem?.kind === 'failing') {
    const reason = reasonLabel(t, problem.noteKey);
    return {
      text:
        problem.count === 1 && server
          ? t('plugins.pluginsPage.problemOn', { reason, server: server.name })
          : t('plugins.pluginsPage.problemOnCount', { reason, count: problem.count }),
      warn: true,
    };
  }
  if (problem?.kind === 'connect') {
    // A connector that never got past the vault still offers Connect, but the
    // missing value is the thing to tell.
    if (missingKeys.length > 0) {
      return { text: t('plugins.pluginsPage.keyMissing', { key: missingKeys[0] }), warn: true };
    }
    return {
      text:
        problem.count === 1 && server
          ? t('plugins.pluginsPage.signInTo', { server: server.name })
          : t('plugins.pluginsPage.signInCount', { count: problem.count }),
      warn: false,
    };
  }
  if (updateAvailable) return { text: t('plugins.pluginsPage.updateAvailable'), warn: false };
  return { text: plugin.description, warn: false };
}

/**
 * What an uninstall removes and what outlives it, one sentence each, worded
 * alike before (the page's note) and after (the toast). Only what is there
 * is said: an empty count is left out, the user's changes to its subagents
 * (which go with them) only when there are some, and the keys sentence names
 * stored keys the plugin's connectors read, which uninstall leaves in
 * Secrets. A connector or skill the user customized survives, but it no
 * longer says which plugin it came from, so neither side can name it and
 * neither claims any. `plugins` is how many go at once (Select mode), which
 * only the subagent sentence's "its" or "their" depends on.
 */
export function uninstallLines(
  t: TFunction,
  removed: { connectors: number; skills: number; subagents: number; changedSubagents: number; plugins?: number },
  keptKeys: readonly string[],
  when: 'before' | 'after',
): string[] {
  const lines: string[] = [];
  const parts = [
    removed.connectors > 0 ? t('plugins.pluginsPage.connectorCount', { count: removed.connectors }) : null,
    removed.skills > 0 ? t('plugins.card.skillCount', { count: removed.skills }) : null,
    removed.subagents > 0 ? t('plugins.card.subagentCount', { count: removed.subagents }) : null,
  ].filter((p): p is string => p !== null);
  if (parts.length > 0) {
    // "2 connectors, 1 skill and 3 subagents": a list, then "and" before the last.
    const last = parts[parts.length - 1];
    const what =
      parts.length === 1
        ? last
        : t('plugins.card.partsAnd', {
            connectors: parts.slice(0, -1).reduce((a, b) => t('plugins.card.componentCount', { servers: a, skills: b })),
            skills: last,
          });
    lines.push(
      when === 'before'
        ? t('plugins.pluginsPage.uninstallNote', { what })
        : t('plugins.card.uninstalledRemoved', { what }),
    );
  }
  if (removed.subagents > 0 && removed.changedSubagents > 0) {
    lines.push(t(subagentChangesKey(when, (removed.plugins ?? 1) > 1)));
  }
  if (keptKeys.length === 1) lines.push(t('plugins.pluginsPage.keyStays', { key: keptKeys[0] }));
  else if (keptKeys.length > 1) lines.push(t('plugins.pluginsPage.keysStay', { n: keptKeys.length }));
  return lines;
}

/**
 * The plugin's subagents the user has changed. While it is on, as the
 * Subagents list reads them, the fresher read since the agent edits them
 * mid-thread; none when the list has not been read, so no sentence claims
 * any. A switched-off plugin's subagents leave that list, as does one a
 * deployment subagent of the same name shadows, but uninstall still deletes
 * the user's changes to them, so its components say which.
 */
export function changedSubagents(plugin: PluginInfo, views: readonly SubagentView[] | undefined): number {
  const owned = plugin.components.filter((c) => c.kind === 'subagent');
  if (!plugin.enabled) return owned.filter((c) => c.edited).length;
  if (!views) return 0;
  const listed = views.filter((v) => v.source === 'plugin' && v.plugin.name === plugin.name);
  const unlisted = owned.filter((c) => c.edited && !listed.some((v) => v.name === c.name));
  return listed.filter((v) => v.edited).length + unlisted.length;
}

/**
 * Select mode's uninstall confirm. The bar has room for a question, not the
 * page's counts, so it names the kinds that go, subagents only when one of
 * the plugins carries any, and the user's changes to them only when there
 * are some, as the single uninstall says it.
 */
export function bulkUninstallConfirm(
  t: TFunction,
  plugins: readonly PluginInfo[],
  views: readonly SubagentView[] | undefined,
): string {
  const count = plugins.length;
  const carries = plugins.some((p) => p.components.some((c) => c.kind === 'subagent'));
  if (!carries) return t('plugins.bulk.confirmUninstall', { count });
  const changed = plugins.reduce((sum, p) => sum + changedSubagents(p, views), 0);
  const ask = t('plugins.bulk.confirmUninstallSubagents', { count });
  if (changed === 0) return ask;
  return `${ask} ${t(subagentChangesKey('before', count > 1))}`;
}

/** That the user's changes to the subagents go with them, before or after
 *  the uninstall, of one plugin or several. */
function subagentChangesKey(when: 'before' | 'after', many: boolean): string {
  if (when === 'before') return many ? 'plugins.bulk.subagentChangesGo' : 'plugins.pluginsPage.subagentChangesGo';
  return many ? 'plugins.bulk.subagentChangesGone' : 'plugins.card.subagentChangesGone';
}

/** "7 connectors", "20 skills", "4 connectors, 2 skills", "1 skill, 2 subagents". */
export function partsLabel(t: TFunction, plugin: PluginInfo): string {
  const servers = plugin.components.filter((c) => c.kind === 'mcp').length;
  const skills = plugin.components.filter((c) => c.kind === 'skill').length;
  const subagents = plugin.components.filter((c) => c.kind === 'subagent').length;
  const parts = [
    servers > 0 ? t('plugins.pluginsPage.connectorCount', { count: servers }) : null,
    skills > 0 ? t('plugins.card.skillCount', { count: skills }) : null,
    subagents > 0 ? t('plugins.card.subagentCount', { count: subagents }) : null,
  ].filter((part): part is string => part !== null);
  if (parts.length === 0) return t('plugins.pluginsPage.empty');
  return parts.reduce((servers, skills) => t('plugins.card.componentCount', { servers, skills }));
}

export type PluginOrigin = 'builtIn' | 'catalog' | 'git' | 'uploaded';

/**
 * How the package got here. A catalog install and a pasted git URL are both
 * stored as a git source, so only the catalog naming the plugin as one of its
 * entries (`useCatalogPlugins`) tells them apart. Before the catalog answers,
 * a git source reads as the catalog's, as it did before it could be told.
 */
export function pluginOrigin(
  plugin: PluginInfo,
  catalog: ReadonlyMap<string, MarketplaceEntry> | null,
): PluginOrigin {
  if (isBundled(plugin)) return 'builtIn';
  if (plugin.source_type === 'zip' || !plugin.source_ref) return 'uploaded';
  return catalog === null || catalog.has(plugin.name) ? 'catalog' : 'git';
}

const ORIGIN_LABEL: Record<PluginOrigin, string> = {
  builtIn: 'plugins.pluginsPage.sourceBuiltIn',
  catalog: 'plugins.pluginsPage.sourceCatalog',
  git: 'plugins.pluginsPage.sourceGit',
  uploaded: 'plugins.pluginsPage.sourceUploaded',
};

/** How the package got here, in the words of its group. */
export function sourceLabel(
  t: TFunction,
  plugin: PluginInfo,
  catalog: ReadonlyMap<string, MarketplaceEntry> | null,
): string {
  return t(ORIGIN_LABEL[pluginOrigin(plugin, catalog)]);
}

/** The keys among `keys` the vault holds. A vault that has not been read
 *  holds none, so a sentence about kept keys is left out rather than guessed. */
export function storedKeys(keys: readonly string[], vaultNames: ReadonlySet<string> | null): string[] {
  return vaultNames ? keys.filter((key) => vaultNames.has(key)) : [];
}
